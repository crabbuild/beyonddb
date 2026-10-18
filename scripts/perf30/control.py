"""Local benchmark control for the pinned Cellar 0.1.0 wire protocol.

Queries use the authenticated private peer tunnel; management actions are
bound to the live node generation. No authority records or SQL files are edited.
This is a fixture tool, not an externally exposed administration API.
"""
import hashlib
import hmac
import http.client
import json
import secrets
import socket
import struct
import time
from pathlib import Path

import boto3
from blake3 import blake3
from botocore.config import Config

ACCOUNT = "123456789012"
APP = bytes([0x42]) * 16
ACCOUNT_NS = APP
DATA_NS = bytes([0x43]) * 16
NODE_NAMES = [f"perf30-node{i}" for i in range(1, 4)]
PUBLIC = [f"http://127.0.0.1:{18000+i}" for i in range(1, 4)]
PEER_PORTS = [19000+i for i in range(1, 4)]


def digest(*parts):
    return blake3(b"".join(parts)).digest()


def blob(value):
    return struct.pack(">I", len(value)) + value


def tenant():
    return digest(b"beyonddb.account.v1\0", ACCOUNT.encode())[:16]


def target_id(namespace, partition):
    return digest(b"crab.cell.v1\0", tenant(), APP, namespace,
                  struct.pack(">I", len(partition)), partition)


def data_partition(table_id, ordinal):
    scope = table_id.encode() + ordinal.to_bytes(16, "big")
    return b"\x01" + digest(b"crab.entity.partition.v1\0", DATA_NS,
                             struct.pack(">I", len(scope)), scope)


def identity(namespace, partition):
    name = "bdb-v1-" + target_id(namespace, partition).hex()
    kind = "beyonddb-data" if namespace == DATA_NS else "beyonddb-account"
    key = hashlib.sha256(f"cells:v1:8:beyonddb:{kind}".encode()).digest()
    first = hmac.digest(key, name.encode(), "sha256")[:16]
    native = first + hmac.digest(key, first, "sha256")[:16]
    return name, f"{kind}:{native.hex()}"


def headers(key, node, path, body):
    stamp, nonce = str(time.time_ns() // 1_000_000), secrets.token_hex(16)
    body_hash = hashlib.sha256(body).hexdigest()
    canonical = "\n".join(["cells-peer-request-v1", "1", "POST", path,
                            body_hash, "perf30-control", node, stamp, nonce])
    return {"x-cells-peer-version": "1", "x-cells-peer-source": "perf30-control",
            "x-cells-peer-target": node, "x-cells-peer-timestamp": stamp,
            "x-cells-peer-nonce": nonce, "x-cells-peer-body-sha256": body_hash,
            "x-cells-peer-signature": hmac.digest(key, canonical.encode(), "sha256").hex()}


class Control:
    def __init__(self, run_dir):
        self.root = Path(run_dir)
        self.key = (self.root / "peer.key").read_bytes()
        self.credentials = json.loads((self.root / "credentials.json").read_text())
        settings = json.loads((self.root / "node1.json").read_text())
        self.bucket = settings["fleet"]["bucket"]
        self.prefix = settings["fleet"]["prefix"]
        self.s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:19100",
                               region_name="us-east-1", aws_access_key_id="perf30store",
                               aws_secret_access_key=self.credentials["store_secret"],
                               config=Config(retries={"total_max_attempts": 1}))

    def sdk(self, index=0, pool=128):
        return boto3.client("dynamodb", endpoint_url=PUBLIC[index], region_name="us-east-1",
                            aws_access_key_id=self.credentials["access_key"],
                            aws_secret_access_key=self.credentials["secret"],
                            config=Config(max_pool_connections=pool, connect_timeout=2,
                                          read_timeout=45, retries={"total_max_attempts": 1}))

    def lease(self, index):
        value = json.loads(self.s3.get_object(Bucket=self.bucket,
                           Key=f"{self.prefix}/nodes/{NODE_NAMES[index]}.json")["Body"].read())
        if value["expires_ms"] <= time.time_ns() // 1_000_000:
            raise RuntimeError("node lease expired")
        return value

    def action(self, index, action):
        lease = self.lease(index)
        body = json.dumps({"generation": lease["generation"], "action": action}).encode()
        conn = http.client.HTTPConnection("127.0.0.1", PEER_PORTS[index], timeout=40)
        try:
            conn.request("POST", "/peer/management", body,
                         headers(self.key, NODE_NAMES[index], "/peer/management", body))
            response = conn.getresponse()
            result = json.loads(response.read())
            if response.status != 200 or response.getheader("x-cells-peer-version") != "1":
                raise RuntimeError(f"management {response.status}: {result}")
            return result["result"]
        finally:
            conn.close()

    def query(self, index, namespace, partition, operation, value):
        name, scope = identity(namespace, partition)
        encoded = blob(json.dumps(value, separators=(",", ":")).encode())
        frame = (blob(b"BDB1") + blob(tenant()) + blob(APP) + blob(namespace) + blob(partition)
                 + struct.pack(">II", operation, 1) + b"\0" + struct.pack(">Q", 0) + blob(encoded))
        sock = socket.create_connection(("127.0.0.1", PEER_PORTS[index]), timeout=45)
        stream = sock.makefile("rb")
        try:
            upgrade_headers = headers(self.key, NODE_NAMES[index], "/peer/tunnel", b"")
            upgrade_headers.update({"Host": "localhost", "Connection": "Upgrade",
                                    "Upgrade": "cellar-tunnel", "Content-Length": "0",
                                    "x-cells-tunnel-version": "1", "x-cells-tunnel-kind": "do"})
            sock.sendall(("POST /peer/tunnel HTTP/1.1\r\n" + "".join(
                f"{k}: {v}\r\n" for k, v in upgrade_headers.items()) + "\r\n").encode())
            status = stream.readline()
            reply_headers = {}
            while (line := stream.readline()) not in (b"\r\n", b""):
                k, v = line.decode().split(":", 1)
                reply_headers[k.lower()] = v.strip()
            if b" 101 " not in status or reply_headers.get("x-cells-peer-version") != "1":
                raise RuntimeError(f"peer tunnel refused: {status!r}")
            path = f"http://bdb.internal/_bdb/BDB_{namespace.hex()}/{name}"
            inner = {"Host": "bdb.internal", "Content-Type": "application/octet-stream",
                     "Content-Length": str(len(frame)), "Connection": "close",
                     "x-cells-scope": scope, "x-cells-do-name": name}
            sock.sendall((f"POST {path} HTTP/1.1\r\n" + "".join(
                f"{k}: {v}\r\n" for k, v in inner.items()) + "\r\n").encode() + frame)
            response = http.client.HTTPResponse(sock)
            response.begin()
            data = response.read()
            if response.status != 200:
                raise RuntimeError(f"native query {response.status}: {data[:500]!r}")
            size = struct.unpack(">I", data[:4])[0]
            if data[4:4+size] != b"BDR1":
                raise RuntimeError("unexpected Cellar application reply")
            offset = 4 + size
            sequence, rejected, outer_len = struct.unpack(">QBI", data[offset:offset+13])
            payload = data[offset+13:]
            json_len = struct.unpack(">I", payload[:4])[0]
            if rejected or outer_len != len(payload) or json_len != len(payload)-4:
                raise RuntimeError("invalid or rejected native query reply")
            return sequence, json.loads(payload[4:])
        finally:
            stream.close()
            sock.close()

    def ownership(self):
        result = []
        for index in range(3):
            items, after = [], None
            while True:
                page = self.action(index, {"cells": {"after": after, "limit": 100}})
                if not page["authoritative"]:
                    raise RuntimeError("node does not report authoritative cell inventory")
                items.extend(page["items"])
                after = page["next_cursor"]
                if after is None:
                    break
            result.append({"node": NODE_NAMES[index], "lease": self.lease(index), "cells": items})
        return result
