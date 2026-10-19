"""Offline topology, routing and artifact regression checks (no Docker or AWS)."""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import itertools
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import bench
from control import Control
import report
from topology import Topology

SCRIPTS = Path(__file__).resolve().parent


def encoded(value):
    return b"\x10" + value.encode().replace(b"\0", b"\0\xff") + b"\0\0"


def manifest_for(topology):
    return {"topology": topology.to_dict(), "data_cells": topology.cells, "ranges": [
        {"ordinal": i, "node": topology.owner(i), "scope": f"scope-{i}", "installed_spec": {
            "lower": list(encoded(chr(i * 256 // topology.cells))) if i else None,
            "upper": list(encoded(chr((i+1) * 256 // topology.cells))) if i+1 < topology.cells else None}}
        for i in range(topology.cells)]}


class FixtureControl:
    def __init__(self, topology, manifest):
        self.topology = topology
        self.nodes = [{"node": name, "cells": []} for name in topology.node_names]
        for row in manifest["ranges"]:
            self.nodes[row["node"]]["cells"].append({"scope": row["scope"], "phase": "resident"})

    def ownership(self):
        return self.nodes


class SDK:
    def __init__(self):
        self.calls = []

    def get_item(self, **args):
        self.calls.append(args)
        return {"Item": {"payload": {"S": "x" * 1024}}}

    def query(self, **args):
        self.calls.append(args)
        return {"Items": [{}, {}]}

    def put_item(self, **args):
        self.calls.append(args)

    def transact_get_items(self, **args):
        self.calls.append(args)
        return {"Responses": [self.get_item(), self.get_item()]}

    def transact_write_items(self, **args):
        self.calls.append(args)


class TopologyTests(unittest.TestCase):
    def test_invalid_topology(self):
        for nodes, cells in [(0, 10), (3, 0), (-1, 3), (1, 1), (257, 1), (3, 86), (True, 2)]:
            with self.subTest(nodes=nodes, cells=cells), self.assertRaises(ValueError):
                Topology(nodes, cells)

    def test_all_bootstrap_sizes_and_partners(self):
        for total in range(2, 257):
            for nodes in (1, total):
                topology = Topology(nodes, total // nodes)
                manifest = manifest_for(topology)
                control = FixtureControl(topology, manifest)
                self.assertEqual(sum(bench.verify(control, manifest)["data_cells_per_node"].values()), total)
                for i in range(total):
                    other = topology.partner(i)
                    self.assertNotEqual(i, other)
                    if nodes > 1:
                        self.assertNotEqual(topology.owner(i), topology.owner(other))
        ports = [Topology(256, 1).peer_port(i) for i in range(256)]
        self.assertEqual(len(set(ports)), 256)
        self.assertNotIn(19100, ports)

    def test_ownership_mismatch_and_missing_range_rejected(self):
        topology = Topology(4, 2)
        manifest = manifest_for(topology)
        control = FixtureControl(topology, manifest)
        self.assertEqual(set(bench.verify(control, manifest)["data_cells_per_node"].values()), {2})
        control.nodes[0]["cells"][0]["phase"] = "opening"
        with self.assertRaisesRegex(RuntimeError, "not resident"):
            bench.verify(control, manifest)
        control = FixtureControl(topology, manifest)
        control.nodes[1]["cells"].append(control.nodes[0]["cells"].pop())
        with self.assertRaisesRegex(RuntimeError, "placement differs"):
            bench.verify(control, manifest)
        manifest["ranges"].pop()
        with self.assertRaisesRegex(RuntimeError, "manifest differs"):
            bench.verify(control, manifest)

    def test_legacy_manifest(self):
        topology = Topology()
        manifest = manifest_for(topology)
        del manifest["topology"]
        self.assertEqual(bench.verify(FixtureControl(topology, manifest), manifest)["data_cells_per_node"],
                         dict.fromkeys(topology.node_names, 10))
        for i in range(30):
            self.assertEqual(topology.prefix(i), chr(i*256//30 + 1))

    def test_init_persists_topology_and_rejects_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            command = [sys.executable, str(SCRIPTS / "setup.py"), "init", "--run-dir", str(root),
                       "--nodes", "4", "--cells-per-node", "8"]
            subprocess.run(command, check=True, capture_output=True)
            topology = Topology.load(root)
            self.assertEqual(topology, Topology(4, 8))
            self.assertEqual(len(list(root.glob("node*.json"))), 4)
            for i in range(4):
                node = json.loads((root / f"node{i+1}.json").read_text())
                self.assertEqual(node["initial_partitions"], 32)
                self.assertEqual(node["public_endpoint"], topology.endpoints[i])
            with patch("control.boto3.client"):
                self.assertEqual(Control(root).topology, topology)
                node["initial_partitions"] = 30
                (root / "node4.json").write_text(json.dumps(node))
                with self.assertRaisesRegex(ValueError, "configuration differs"):
                    Control(root)
            original = (root / "peer.key").read_bytes()
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual((root / "peer.key").read_bytes(), original)
            invalid = Path(temporary) / "invalid"
            command[command.index(str(root))] = str(invalid)
            command[-1] = "100"
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertFalse(invalid.exists())

    def test_setup_stop_uses_saved_node_count(self):
        import setup
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "fixture.json").write_text(json.dumps(Topology(4, 2).to_dict()))
            states = [{"State": {"Running": False, "ExitCode": 0, "OOMKilled": False}}] * 5
            with patch.object(sys, "argv", ["setup", "stop", "--run-dir", str(root)]), \
                    patch.object(setup, "docker", side_effect=["nodes", "store", json.dumps(states)]) as docker, \
                    redirect_stdout(io.StringIO()):
                setup.main()
            self.assertIn("beyonddb-perf30-node4", docker.call_args_list[0].args)
            self.assertNotIn("beyonddb-perf30-node5", docker.call_args_list[0].args)

    def test_prepare_installs_and_seeds_every_planned_range(self):
        from control import DATA_NS, ACCOUNT_NS, data_partition, digest, identity, target_id, tenant
        for topology in (Topology(2, 3), Topology(1, 256)):
            manifest = manifest_for(topology)
            control = FixtureControl(topology, manifest)
            table_id = (tenant() + digest(b"beyonddb.table.v1\0", target_id(ACCOUNT_NS, bytes(4)),
                                        (8).to_bytes(8, "big"), b"Perf30")[:16]).hex()
            partitions = {data_partition(table_id, i): i for i in range(topology.cells)}
            warmed = []
            created = False
            clients = [SDK() for _ in topology.indices]

            def create_table(**kwargs):
                nonlocal created
                self.assertEqual(len(warmed), topology.cells)
                created = True

            clients[0].create_table = create_table

            def query(index, namespace, partition, operation, value):
                if namespace == ACCOUNT_NS:
                    return 7, None
                self.assertEqual(namespace, DATA_NS)
                ordinal = partitions[partition]
                self.assertEqual(index, ordinal // topology.cells_per_node)
                if not created:
                    warmed.append(ordinal)
                    return 0, None
                return 0, {"spec": {**manifest["ranges"][ordinal]["installed_spec"],
                                   "table": {"id": table_id}, "partition_id": list(ordinal.to_bytes(16, "big"))}}

            control.query = query
            control.action = lambda index, action: None
            control.sdk = lambda index=0: clients[index]
            for row in manifest["ranges"]:
                _, scope = identity(DATA_NS, data_partition(table_id, row["ordinal"]))
                control.nodes[row["node"]]["cells"][row["ordinal"] % topology.cells_per_node]["scope"] = scope
            with tempfile.TemporaryDirectory() as temporary, redirect_stdout(io.StringIO()):
                bench.prepare(control, Path(temporary))
                saved = json.loads((Path(temporary) / "table.json").read_text())
                self.assertEqual(saved["data_cells"], topology.cells)
                self.assertEqual(saved["seed_items"], topology.cells * 8)
                self.assertEqual([len(c.calls) for c in clients], [topology.cells_per_node * 8] * topology.nodes)

    def test_workloads_cover_configured_cells_and_cross_owners(self):
        for topology in (Topology(1, 2), Topology(2, 3), Topology(4, 2)):
            for operation in ("get", "query", "put", "transact_get", "transact_write"):
                with self.subTest(topology=topology, operation=operation):
                    clients = [SDK() for _ in topology.indices]
                    # Deterministic clock: exactly two full rounds, then stop.
                    times = [0] + [v for i in range(topology.cells*2) for v in (i, i, i)]
                    times += [topology.cells*2+1, topology.cells*2+1]
                    with patch.object(bench.time, "perf_counter", side_effect=times):
                        case = bench.run_case(clients, operation, 1, topology.cells*2+0.5,
                                              "run", "test", topology)
                    self.assertTrue(case["covers_all_cells"])
                    self.assertEqual(case["errors"], 0)
                    self.assertEqual(set(case["frontend_requests"]), set(topology.indices))
                    if operation == "transact_write":
                        self.assertEqual(len(case["write_witnesses"]), topology.cells)
                        seen = set()
                        prefix_owner = {topology.prefix(i): topology.owner(i) for i in range(topology.cells)}
                        for sdk in clients:
                            for call in sdk.calls:
                                seen.add(call["ClientRequestToken"])
                                items = [v["Put"]["Item"] for v in call["TransactItems"]]
                                self.assertNotEqual(items[0]["pk"], items[1]["pk"])
                                if topology.nodes > 1:
                                    self.assertNotEqual(*(prefix_owner[item["pk"]["S"][0]] for item in items))
                        self.assertEqual(len(seen), case["completed"])

    def test_errors_are_retained_and_coverage_fails(self):
        sdk = SDK()
        sdk.get_item = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("unavailable"))
        with patch.object(bench.time, "perf_counter", side_effect=itertools.count(0, 0.01)):
            case = bench.run_case([sdk], "get", 1, 1, "run", "failed", Topology(1, 2))
        self.assertEqual(case["errors"], 3)
        self.assertFalse(case["covers_all_cells"])
        self.assertEqual(len(case["failures"]), 3)

    def test_measure_saves_topology_and_routes_witnesses_to_owners(self):
        topology = Topology(4, 2)
        manifest = manifest_for(topology)
        control = FixtureControl(topology, manifest)
        clients = [SDK() for _ in topology.indices]
        control.sdk = lambda index=0, pool=128: clients[index]
        control.lease = lambda index: {"load": {}}
        case = {"operation": "put", "clients": 2, "completed": 8, "errors": 0,
                "elapsed_s": 1, "requests_per_s": 8, "p50_ms": 1, "p95_ms": 2, "p99_ms": 3,
                "covers_all_cells": True, "write_witnesses": {
                    str(i): bench.key(i, topology=topology) for i in range(8)}}
        args = SimpleNamespace(operations=["put"], clients=[2], repeats=1, warmup=0,
                               seconds=1, label="custom")
        with tempfile.TemporaryDirectory() as temporary, redirect_stdout(io.StringIO()):
            output = Path(temporary)
            (output / "table.json").write_text(json.dumps(manifest))
            with patch.object(bench, "run_case", return_value=case) as run, \
                    patch.object(bench, "snapshot_cgroups", return_value={}) as snapshots:
                bench.measure(control, output, args)
            self.assertEqual(run.call_args.args[-1], topology)
            self.assertEqual(snapshots.call_args.args, (topology,))
            saved = json.loads((output / "results-custom.json").read_text())
            self.assertEqual(saved["topology"], {"nodes": 4, "cells_per_node": 2})
            self.assertEqual(saved["planned_cases"], 1)
            self.assertEqual(saved["cases"][0]["write_witnesses_verified"], 8)
            self.assertEqual(len(saved["cases"][0]["lease_loads_after"]), 4)
            self.assertTrue((output / "ownership-after-custom.json").exists())
            for index, sdk in enumerate(clients):
                self.assertEqual([c["Key"] for c in sdk.calls],
                                 [bench.key(i, topology=topology) for i in range(index*2, index*2+2)])

    def test_report_uses_topology_and_preserves_failed_and_empty_cases(self):
        case = {"operation": "get", "clients": 2, "completed": 8, "errors": 1,
                "elapsed_s": 1, "requests_per_s": 8, "items_per_s": 8,
                "p50_ms": 1, "p95_ms": 2, "p99_ms": 3, "covers_all_cells": False,
                "tail_sample_warning": True, "write_witnesses_verified": 0}
        result = {"topology": Topology(4, 2).to_dict(), "cases": [case], "planned_cases": 1,
                  "warmup_errors": 2, "warmup_s": 0, "transaction_placement": "different cells and nodes"}
        with tempfile.TemporaryDirectory() as temporary:
            text, rows = report.render(result, Path(temporary), 2)
            self.assertIn("4 nodes", text)
            self.assertIn("All 8 cells", text)
            self.assertIn("1 contained request errors", text)
            self.assertNotIn("Incomplete matrix", text)
            self.assertNotIn("3,723", text)
            self.assertFalse(rows[0]["covers_all_cells"])
            result["cases"] = []
            self.assertIn("0 of 1 planned", report.render(result, Path(temporary))[0])
            result["cases"] = [deepcopy(case)]
            del result["topology"]
            result["cases"][0]["covers_all_30_cells"] = result["cases"][0].pop("covers_all_cells")
            text, rows = report.render(result, Path(temporary))
            self.assertIn("All 30 cells", text)
            self.assertFalse(rows[0]["covers_all_cells"])


if __name__ == "__main__":
    unittest.main()
