#!/usr/bin/env python3
"""Preserve native Cellar peer bytes over authenticated WSS; supervise one node.

Public HTTP terminates TLS at the Worker and reaches loopback through a raw TCP
forwarder. Private peer ports are exposed only by the Worker's secret-gated
WebSocket bridge. The same bridge supports the unmodified perf control protocol.
"""
import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import time

from websockets.asyncio.client import connect


async def copy_stream(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()


async def local_forward(reader, writer, port):
    try:
        remote_reader, remote_writer = await asyncio.open_connection('127.0.0.1', port)
        await asyncio.gather(copy_stream(reader, remote_writer), copy_stream(remote_reader, writer))
    except (OSError, ConnectionError):
        writer.close()


async def peer_forward(reader, writer, index, settings):
    try:
        url = settings['worker_url'].replace('https://', 'wss://') + f'/_peer/{index}'
        async with connect(url, additional_headers={'Authorization': 'Bearer ' + settings['tunnel_token']},
                           max_size=None, compression=None, open_timeout=30,
                           user_agent_header='BeyondDBPeerBridge/0.1',
                           ping_interval=20, ping_timeout=30) as ws:
            async def outbound():
                while data := await reader.read(65536):
                    await ws.send(data)
                await ws.close()
            async def inbound():
                async for data in ws:
                    if not isinstance(data, bytes):
                        raise ValueError('unexpected nonbinary peer frame')
                    writer.write(data)
                    await writer.drain()
            tasks = [asyncio.create_task(outbound()), asyncio.create_task(inbound())]
            try:
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as error:
        # Do not log URLs, auth headers or request bytes.
        print(f'peer bridge {index}: {type(error).__name__}', flush=True)
    finally:
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


def read_optional(path):
    try:
        return Path(path).read_text()
    except OSError:
        return None


def snapshot(process):
    return {
        'node': int(os.environ['NODE_INDEX']), 'observed_at': time.time(),
        'pid': process.pid, 'exit_code': process.poll(),
        'platform': {k: v for k, v in os.environ.items() if k.startswith('CLOUDFLARE_')},
        'cpu_count': os.cpu_count(), 'meminfo': read_optional('/proc/meminfo'),
        'process_status': read_optional(f'/proc/{process.pid}/status'),
        'process_limits': read_optional(f'/proc/{process.pid}/limits'),
        'agent_nofile_limit': resource.getrlimit(resource.RLIMIT_NOFILE),
        'process_open_fds': len(list(Path(f'/proc/{process.pid}/fd').iterdir())) if process.poll() is None else None,
        'cgroup': {p: read_optional('/sys/fs/cgroup/' + p) for p in
                   ('cpu.max', 'cpu.stat', 'memory.max', 'memory.current', 'memory.events', 'io.stat')},
        'disk': dict(zip(('total', 'used', 'free'), __import__('shutil').disk_usage('/data'))),
    }


async def status_handler(reader, writer, process):
    try:
        line = await reader.readline()
        path = line.split()[1].decode()
        await reader.readuntil(b'\r\n\r\n')
        if path == '/logs':
            body = Path('/data/server.log').read_bytes()[-48000:]
            kind = 'text/plain'
        else:
            body = json.dumps(snapshot(process)).encode()
            kind = 'application/json'
        writer.write(f'HTTP/1.1 200 OK\r\nContent-Type: {kind}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n'.encode() + body)
        await writer.drain()
    except (OSError, ValueError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()


def configure():
    index = int(os.environ['NODE_INDEX'])
    root = Path('/config'); root.mkdir(mode=0o700, exist_ok=True)
    Path('/data').mkdir(exist_ok=True)
    for name, env in [('peer.key', 'PEER_KEY_HEX'), ('encryption.key', 'ENCRYPTION_KEY_HEX')]:
        p = root/name; p.write_bytes(bytes.fromhex(os.environ[env])); p.chmod(0o600)
    policy = {'Version': '2012-10-17', 'Statement': [{'Effect': 'Allow', 'Action': 'dynamodb:*', 'Resource': '*'}]}
    (root/'policy.json').write_text(json.dumps(policy))
    config = {
        'node_name': f'perf30-node{index}', 'data_dir': '/data', 'region': 'us-east-1',
        'public_bind': '127.0.0.1:18000', 'public_endpoint': os.environ['WORKER_URL'],
        'peer_bind': '0.0.0.0:9000', 'peer_advertise': f'127.0.0.1:{19000+index}',
        'peer_key_file': '/config/peer.key', 'encryption_key_file': '/config/encryption.key',
        'accounts': ['123456789012'], 'initial_partitions': 30, 'max_active_cells': 2048,
        'split_threshold_bytes': 1073741824, 'auth_cache_enabled': False,
        'shutdown_mode': 'preserve',
        'fleet': {'bucket': os.environ['R2_BUCKET'], 'endpoint': os.environ['R2_ENDPOINT'],
                  'prefix': 'ordered-v1', 'follower_durability': False},
        'bootstrap': {'account_id': '123456789012', 'access_key_id': os.environ['DDB_ACCESS_KEY'],
                      'principal_name': 'perf', 'policy_name': 'benchmark', 'policy_file': '/config/policy.json'},
    }
    # The executable's loopback listener requires an HTTP advertised endpoint;
    # the public HTTPS proxy is recorded separately by the deployment manifest.
    config['public_endpoint'] = 'http://127.0.0.1:18000'
    (root/'node.json').write_text(json.dumps(config))
    return index


async def main(settings=None):
    if settings is not None:
        servers = [await asyncio.start_server(lambda r, w, i=i: peer_forward(r, w, i, settings),
                                               '127.0.0.1', 19000+i) for i in range(1, 4)]
        await asyncio.Event().wait()
        return
    index = configure()
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))
    except (ValueError, PermissionError) as error:
        raise RuntimeError('The benchmark requires an effective nofile limit of 65536') from error
    print('Effective nofile limit:', resource.getrlimit(resource.RLIMIT_NOFILE), flush=True)
    settings = {'worker_url': os.environ['WORKER_URL'], 'tunnel_token': os.environ['TUNNEL_TOKEN']}
    servers = []
    for target in range(1, 4):
        handler = (lambda r,w: local_forward(r,w,9000)) if target == index else (
            lambda r,w,i=target: peer_forward(r,w,i,settings))
        servers.append(await asyncio.start_server(handler, '127.0.0.1', 19000+target))
    env = dict(os.environ, AWS_VIRTUAL_HOSTED_STYLE_REQUEST='false', RUST_LOG='info')
    command = ['/opt/beyonddb', '/config/node.json']
    bootstrap = os.environ.get('BOOTSTRAP') == '1'
    if bootstrap:
        command.append('--bootstrap')
    log = open('/data/server.log', 'ab', buffering=0)
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    process.stdin.write(os.environ['DDB_SECRET_KEY'].encode() if bootstrap else b'')
    process.stdin.close()
    servers.append(await asyncio.start_server(lambda r,w: local_forward(r,w,18000), '0.0.0.0', 8080))
    servers.append(await asyncio.start_server(lambda r,w: status_handler(r,w,process), '0.0.0.0', 8090))
    async def capture_logs():
        sensitive = [os.environ[k] for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                     'PEER_KEY_HEX', 'ENCRYPTION_KEY_HEX', 'DDB_SECRET_KEY', 'TUNNEL_TOKEN')]
        while line := await asyncio.to_thread(process.stdout.readline):
            text = line.decode(errors='replace')
            for secret in sensitive:
                text = text.replace(secret, '[redacted]')
            log.write(text.encode())
            print(text, end='', flush=True)
    log_task = asyncio.create_task(capture_logs())
    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()
    def request_stop():
        stopping.set()
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_stop)
    while process.poll() is None:
        await asyncio.sleep(1)
    await log_task
    print('BeyondDB exited', process.returncode, flush=True)
    if not stopping.is_set():
        # Retain startup-failure diagnostics without pretending the DB is ready.
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stopping.wait(), 600)
    for server in servers:
        server.close()
    raise SystemExit(process.returncode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tunnels', type=Path)
    args = parser.parse_args()
    asyncio.run(main(json.loads(args.tunnels.read_text()) if args.tunnels else None))
