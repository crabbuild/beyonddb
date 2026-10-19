"""Offline byte-preservation check for the Cloudflare deployment peer bridge."""
import asyncio
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import cloudflare_report
from cloudflare import check_writes
from topology import Topology


class CloudflareRecovery(unittest.TestCase):
    def test_failed_readback_is_retained_without_resubmission(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'table.json').write_text('{"table":"Offline"}')
            source = root/'results.json'
            source.write_text(json.dumps({'cases': [{
                'phase': 'offline-put', 'write_witnesses': {'0': {
                    'pk': {'S': 'original-key'}, 'sk': {'N': '1'}}}}]}))
            class Client:
                def __init__(self): self.calls = []
                def get_item(self, **request):
                    self.calls.append(request)
                    raise TimeoutError('offline fault')
            client = Client()
            control = SimpleNamespace(topology=Topology(3, 1), sdk=lambda i: client)
            with self.assertRaises(SystemExit):
                check_writes(control, root, [source], 'offline')
            result = json.loads((root/'write-check-offline.json').read_text())
            self.assertEqual(result['verified'], 0)
            self.assertEqual(len(result['checks']), 4)
            self.assertEqual(len(client.calls), 4)
            self.assertTrue(all(r['ConsistentRead'] for r in client.calls))
            self.assertEqual(client.calls[0]['Key']['pk']['S'], 'original-key')
            self.assertTrue(all(c['error_type'] == 'TimeoutError' for c in result['checks']))


class CloudflareReports(unittest.TestCase):
    def test_interrupted_window_and_unsaved_warmup_errors_remain_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'deployment.json').write_text(json.dumps({
                'source_revision': 'offline-revision', 'binary_sha256': 'offline-digest'}))
            # The warmup completed, but the process died before saving its total.
            (root/'warmup-pressure-transact_get-60.json').write_text('{"errors": 2}')
            result = {'label': 'pressure', 'warmup_errors': 0, 'cases': [{
                'operation': 'transact_get', 'phase': 'pressure-transact_get-60', 'clients': 60, 'completed': 0,
                'errors': 3, 'elapsed_s': 45, 'requests_per_s': 0, 'items_per_s': 0,
                'p50_ms': None, 'p95_ms': None, 'p99_ms': None, 'covers_all_cells': False}]}
            source = root/'results-pressure.json'
            source.write_text(json.dumps(result))
            output = root/'report'
            with patch('sys.argv', ['cloudflare_report.py', '--input', str(source),
                                    '--output', str(output)]):
                cloudflare_report.main()
            combined = json.loads((output/'combined-results.json').read_text())
            self.assertEqual(combined['warmup_errors'], 2)
            self.assertEqual(combined['cases'][0]['errors'], 3)
            self.assertFalse(combined['runs'][0]['completed'])
            self.assertIn('0 successful calls, 3 measured errors and 2 warmup errors',
                          (output/'report.md').read_text())
            self.assertIn('pressure,transact_get,60,0,3,', (output/'windows.csv').read_text())

try:
    from websockets.asyncio.server import serve
except ImportError:
    serve = None


@unittest.skipIf(serve is None, 'install requirements-cloudflare.txt for bridge checks')
class CloudflareBridge(unittest.IsolatedAsyncioTestCase):
    async def test_preserves_native_headers_and_large_binary_stream(self):
        source = Path(__file__).resolve().parents[2]/'deploy/cloudflare/agent.py'
        spec = importlib.util.spec_from_file_location('beyonddb_cloud_agent', source)
        agent = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(agent)
        observed = []
        async def echo(ws):
            observed.append((ws.request.path, ws.request.headers.get('Authorization')))
            async for payload in ws:
                await ws.send(payload)
        async with serve(echo, '127.0.0.1', 0, compression=None) as remote:
            settings = {'worker_url': f'ws://127.0.0.1:{remote.sockets[0].getsockname()[1]}',
                        'tunnel_token': 'offline-token'}
            proxy = await asyncio.start_server(lambda r,w: agent.peer_forward(r,w,2,settings), '127.0.0.1', 0)
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', proxy.sockets[0].getsockname()[1])
                payload = (b'POST /peer/tunnel HTTP/1.1\r\nUpgrade: cellar-tunnel\r\n\r\n'
                           + bytes(range(256))*1024)
                writer.write(payload[:7]); await writer.drain()
                writer.write(payload[7:]); await writer.drain()
                received = await asyncio.wait_for(reader.readexactly(len(payload)), 10)
                self.assertEqual(received, payload)
                self.assertEqual(observed, [('/_peer/2', 'Bearer offline-token')])
                writer.close(); await writer.wait_closed()
            finally:
                proxy.close(); await proxy.wait_closed()
