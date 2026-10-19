#!/usr/bin/env python3
"""Run the existing ordered-range workload against three Cloudflare Containers.

Uses private deployment.json/secrets.json in an ignored benchmark directory.
Run agent.py --tunnels against secrets.json for the native private control path.
Cloudflare/R2 credentials come only from the calling environment.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import threading
import urllib.error
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ReadTimeoutError, ClientError

from bench import key, prepare, run_case, save, verify
from control import Control, ACCOUNT_NS, DATA_NS
from topology import Topology


class CloudControl(Control):
    def __init__(self, root):
        self.root = Path(root)
        self.meta = json.loads((self.root/'deployment.json').read_text())
        self.credentials = json.loads((self.root/'secrets.json').read_text())
        self.topology = Topology(self.meta['nodes'], self.meta['cells_per_node'])
        self.key = bytes.fromhex(self.credentials['peer_key'])
        self.bucket, self.prefix = self.meta['bucket'], self.meta['prefix']
        self.s3 = boto3.client('s3', endpoint_url=os.environ['CF_S3_API_ENDPOINT'], region_name='auto',
                               aws_access_key_id=os.environ['CF_ACCESS_KEY_ID'],
                               aws_secret_access_key=os.environ['CF_SECRET_ACCESS_KEY'],
                               config=Config(retries={'total_max_attempts': 1}, request_checksum_calculation='when_required'))

    def sdk(self, index=0, pool=128):
        client = boto3.client('dynamodb', endpoint_url=self.credentials['worker_url'], region_name='us-east-1',
                              aws_access_key_id=self.credentials['access_key'],
                              aws_secret_access_key=self.credentials['secret'],
                              config=Config(max_pool_connections=pool, connect_timeout=10, read_timeout=45,
                                            retries={'total_max_attempts': 1}, user_agent_extra='BeyondDBBenchmark/0.1'))
        def ingress(request, **kwargs):
            request.headers['x-beyonddb-node'] = str(index+1)
        client.meta.events.register('before-sign.dynamodb', ingress)
        return client

    def admin(self, index, action, method='GET'):
        request = urllib.request.Request(self.credentials['worker_url'] + f'/_admin/{index+1}/{action}',
                                         headers={'Authorization': 'Bearer '+self.credentials['admin_token'], 'User-Agent': 'BeyondDBBenchmark/0.1'}, method=method)
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            return body.decode() if action == 'logs' else json.loads(body)

    def resources(self):
        result = {self.topology.node_names[i]: self.admin(i, 'status') for i in self.topology.indices}
        account = os.environ['CF_ACCOUNT_ID']
        base = f'https://api.cloudflare.com/client/v4/accounts/{account}/containers/applications'
        headers = {'Authorization': 'Bearer '+os.environ['CF_API_TOKEN']}
        if 'application_id' not in self.meta:
            with urllib.request.urlopen(urllib.request.Request(base, headers=headers), timeout=30) as response:
                applications = json.load(response)['result']
            app = next(a for a in applications if a['name'] == self.meta['worker']+'-beyondnode')
            self.meta['application_id'] = app['id']
            save(self.root/'deployment.json', self.meta)
        api = f"{base}/{self.meta['application_id']}/instances"
        request = urllib.request.Request(api, headers=headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            instances = json.load(response)['result']['instances']
        for index, name in enumerate(self.topology.node_names):
            value = result[name]
            if not value['running'] or value['agent']['exit_code'] is not None:
                raise RuntimeError(f'{name} is not running')
            if min(value['agent']['agent_nofile_limit']) < 65536:
                raise RuntimeError(f'{name}: nofile limit is below the qualified fixture setting')
            instance = next(i for i in instances if i['name'] == f'node-{index+1}')
            allocation = instance['configuration']
            if allocation != {'vcpu': 1, 'memory': 3072, 'disk': 5000}:
                raise RuntimeError(f'{name}: unexpected Cloudflare instance allocation')
            value['cloudflare_instance'] = instance
            # Guest cpu_count/MemTotal can include platform scheduling and VM
            # overhead; retain them without treating them as cgroup limits.
        return result




def resume_creation(control, root):
    plan = json.loads((root/'table-plan.json').read_text())
    if plan.get('creation_returned'):
        raise RuntimeError('Seeding may already have started; do not replay ambiguous item writes')
    if any(root.glob('seed-range-*.json')):
        raise RuntimeError('A prior seed submission exists; retain its outcome instead of resubmitting')
    deadline = time.monotonic()+180
    while True:
        _, record = control.query(0, ACCOUNT_NS, bytes(4), 7, plan['table'])
        if record and record['id'] != plan['table_id']:
            raise RuntimeError('Catalog generation differs from the original creation plan')
        if record:
            status = control.sdk().describe_table(TableName=plan['table'])['Table']['TableStatus']
            if status == 'ACTIVE': break
        if time.monotonic() >= deadline:
            raise RuntimeError('Original table creation has not become ACTIVE; retain its ambiguous outcome')
        time.sleep(3)
    for row in plan['ranges']:
        partition = bytes.fromhex(row['partition'])
        _, state = control.query(row['node'],DATA_NS,partition,4,None)
        if state is None or state['spec']['table']['id'] != plan['table_id']:
            raise RuntimeError('Original range installation does not match the persisted plan')
        row['installed_spec'] = state['spec']
        _, usage = control.query(row['node'],DATA_NS,partition,7,None)
        if usage['item_count'] != 0:
            raise RuntimeError('Fixture already contains items; do not replay ambiguous seed writes')
    verify(control, plan)
    clients = [control.sdk(i) for i in control.topology.indices]
    stop = threading.Event()
    def seed(ordinal):
        row=plan['ranges'][ordinal]; attempted=[]
        try:
            for sample in range(4):
                for sk in (1,2):
                    if stop.is_set():return
                    item_key=key(ordinal,sample,sk,control.topology)
                    attempted.append({'key':item_key,'outcome':'submitted'})
                    save(root/f'seed-range-{ordinal}.json',attempted)
                    clients[row['node']].put_item(TableName=plan['table'],Item={**item_key,'payload':{'S':'x'*1024}})
                    attempted[-1]['outcome']='acknowledged'
                    save(root/f'seed-range-{ordinal}.json',attempted)
        except Exception:
            stop.set();raise
    order=[node*control.topology.cells_per_node+offset
           for offset in range(control.topology.cells_per_node) for node in control.topology.indices]
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(seed,order))
    if stop.is_set(): raise RuntimeError('Seed submission failed; retain uncertain outcomes without retry')
    plan['seed_items']=control.topology.cells*8
    plan['creation_recovered_after_timeout']=True
    save(root/'table.json',plan)
    save(root/'ownership-prepared.json',verify(control,plan))
    print(f"Recovered original creation; verified {control.topology.cells} ranges and seeded {plan['seed_items']} items",flush=True)


@contextmanager
def resource_samples(control, root, label):
    path = root/f'resource-samples-{label}.jsonl'
    if path.exists():
        raise RuntimeError('Resource samples already exist; use a fresh result label')
    stop = threading.Event()
    def collect():
        with path.open('x') as stream:
            while not stop.is_set():
                row = {'observed_at': datetime.now(timezone.utc).isoformat(), 'nodes': {}}
                for index, name in enumerate(control.topology.node_names):
                    try: row['nodes'][name] = control.admin(index, 'status')
                    except Exception as error: row['nodes'][name] = {'sampling_error': type(error).__name__}
                stream.write(json.dumps(row)+'\n'); stream.flush()
                stop.wait(5)
    thread = threading.Thread(target=collect, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set(); thread.join(timeout=15)


def measure(control, root, args):
    destination = root/f'results-{args.label}.json'
    if destination.exists():
        raise RuntimeError('Result label already exists; preserve previous measurements and select a fresh label')
    manifest = json.loads((root/'table.json').read_text())
    result = {'platform': 'cloudflare-containers-r2', 'topology': control.topology.to_dict(),
              'source_revision': control.meta['source_revision'], 'model': 'closed_loop',
              'profile': 'release', 'payload_bytes': 1024, 'strong_reads': True,
              'sdk_retries': 0, 'client_read_timeout_s': 45, 'transaction_items': 2,
              'transaction_placement': 'different cells and nodes', 'warmup_s': args.warmup,
              'durability': 'R2 bucket publication; follower durability disabled',
              'client_location': 'external workstation; YVR edge observed; HTTPS Worker ingress; WAN included',
              'label': args.label, 'operation_order': args.operations, 'client_order': args.clients,
              'warmup_errors': 0, 'cases': [],
              'started_at': datetime.now(timezone.utc).isoformat()}
    save(root/f'ownership-before-{args.label}.json', verify(control, manifest))
    clients = [control.sdk(i, max(args.clients)) for i in control.topology.indices]
    run_id = __import__('uuid').uuid4().hex[:12]
    for workers in args.clients:
        for operation in args.operations:
            phase = f'{args.label}-{operation}-{workers}'
            if args.warmup:
                warmup = run_case(clients, operation, workers, args.warmup, run_id, phase+'-warmup', control.topology)
                save(root/f'warmup-{phase}.json', warmup)
                result['warmup_errors'] += warmup['errors']
                save(destination, result)
            before = control.resources()
            case = run_case(clients, operation, workers, args.seconds, run_id, phase, control.topology)
            result['cases'].append(case)
            save(destination, result)
            case['resources_before'] = before
            case['resources_after'] = control.resources()
            checked = 0
            for cell, item_key in case['write_witnesses'].items():
                response = clients[control.topology.owner(int(cell))].get_item(TableName=manifest['table'], Key=item_key, ConsistentRead=True)
                if response.get('Item', {}).get('payload', {}).get('S') != 'x'*1024:
                    raise RuntimeError('Acknowledged write did not read back')
                checked += 1
            case['write_witnesses_verified'] = checked
            save(destination, result)
            print(json.dumps({k: case[k] for k in ('operation','clients','completed','errors','elapsed_s','requests_per_s','p50_ms','p95_ms','p99_ms','covers_all_cells')}), flush=True)
    save(root/f'ownership-after-{args.label}.json', verify(control, manifest))
    result['completed_at'] = datetime.now(timezone.utc).isoformat()
    save(destination, result)
    if result['warmup_errors'] or any(c['errors'] or not c['covers_all_cells'] for c in result['cases']):
        raise SystemExit('Failed/incomplete windows retained; inspect results')


def ready(control, indices, timeout):
    """Repeat only a pure query while a newly started node becomes ready."""
    for index in indices:
        deadline = time.monotonic() + timeout
        client = control.sdk(index)
        while True:
            try:
                client.list_tables()
                print(f'Node {index+1}: signed ListTables succeeded', flush=True)
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f'Node {index+1} did not become ready') from None
                time.sleep(3)


def check_writes(control, root, paths, label):
    """Read original acknowledged keys; never resubmit their mutations."""
    destination = root/f'write-check-{label}.json'
    if destination.exists():
        raise RuntimeError('Write-check label already exists; retain previous verification')
    manifest = json.loads((root/'table.json').read_text())
    checks = []
    for path in paths:
        for case in json.loads(path.read_text())['cases']:
            for cell, item_key in case['write_witnesses'].items():
                checks.append({'source': path.name, 'phase': case['phase'],
                               'cell': int(cell), 'key': item_key, 'kind': 'acknowledged-write'})
    if not checks:
        raise RuntimeError('No acknowledged write witnesses in the supplied results')
    for cell in range(control.topology.cells):
        checks.append({'cell': cell, 'key': key(cell, topology=control.topology), 'kind': 'seed'})
    result = {'started_at': datetime.now(timezone.utc).isoformat(),
              'planned_checks': len(checks), 'checks': []}
    save(destination, result)
    clients = [control.sdk(i) for i in control.topology.indices]
    def read(check):
        try:
            response = clients[check['cell'] % control.topology.nodes].get_item(
                TableName=manifest['table'], Key=check['key'], ConsistentRead=True)
            check['verified'] = response.get('Item', {}).get('payload', {}).get('S') == 'x'*1024
            if not check['verified']:
                check['error'] = 'Acknowledged value missing or different'
        except Exception as error:
            check.update(verified=False, error_type=type(error).__name__)
        return check
    with ThreadPoolExecutor(max_workers=6) as pool:
        for check in pool.map(read, checks):
            result['checks'].append(check)
            save(destination, result)
    result['verified'] = sum(c['verified'] for c in result['checks'])
    result['completed_at'] = datetime.now(timezone.utc).isoformat()
    save(destination, result)
    print(f"Verified {result['verified']} of {len(checks)} original values", flush=True)
    if result['verified'] != len(checks):
        raise SystemExit('Write recovery check failed; retained all observations')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start','ready','status','logs','stop','prepare','resume-creation','measure','verify','check-writes'])
    parser.add_argument('--run-dir', required=True, type=Path)
    parser.add_argument('--node', type=int, choices=[1,2,3])
    parser.add_argument('--bootstrap', action='store_true', help='first provisioning only; requires start --node 1')
    parser.add_argument('--operations', nargs='+', choices=['get','query','put','transact_get','transact_write'],
                        default=['get','query','put','transact_get','transact_write'])
    parser.add_argument('--clients', type=int, nargs='+', default=[1,8,30,60])
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--warmup', type=int, default=5)
    parser.add_argument('--label', default='main')
    parser.add_argument('--results', type=Path, nargs='+', help='original result files for check-writes')
    parser.add_argument('--ready-timeout', type=int, default=180)
    args = parser.parse_args()
    if min(args.clients) < 1 or args.seconds < 1 or args.warmup < 0 or args.ready_timeout < 1:
        parser.error('positive clients/seconds/ready-timeout and nonnegative warmup required')
    if args.bootstrap and (args.action != 'start' or args.node != 1):
        parser.error('--bootstrap requires start --node 1')
    if args.action == 'check-writes' and not args.results:
        parser.error('check-writes requires --results')
    control = CloudControl(args.run_dir)
    if args.action == 'prepare':
        if (args.run_dir/'table.json').exists() or (args.run_dir/'table-plan.json').exists():
            parser.error('Retained creation plan exists; observe it with resume-creation instead of recreating')
        try:
            prepare(control, args.run_dir)
        except (ReadTimeoutError, ClientError) as error:
            if not (args.run_dir/'table-plan.json').exists():raise
            plan=json.loads((args.run_dir/'table-plan.json').read_text())
            save(args.run_dir/'creation-failure.json',{'error_type':type(error).__name__,
                 'client_timeout_s':45,'creation_returned':plan.get('creation_returned',False),
                 'outcome':'uncertain; observe original generation without resubmitting CreateTable'})
            if plan.get('creation_returned'):raise
            resume_creation(control,args.run_dir)
    elif args.action == 'resume-creation':
        resume_creation(control,args.run_dir)
    elif args.action == 'measure':
        with resource_samples(control,args.run_dir,args.label):
            measure(control,args.run_dir,args)
    elif args.action == 'verify':
        print(json.dumps(verify(control,json.loads((args.run_dir/'table.json').read_text()))))
    elif args.action == 'ready':
        ready(control, [args.node-1] if args.node else control.topology.indices, args.ready_timeout)
    elif args.action == 'check-writes':
        check_writes(control,args.run_dir,args.results,args.label)
    else:
        indices = [args.node-1] if args.node else control.topology.indices
        for index in indices:
            action = args.action + ('?bootstrap=1' if args.bootstrap else '')
            result = control.admin(index,action,'POST' if args.action in ('start','stop') else 'GET')
            if args.action == 'logs':
                path=args.run_dir/f'node{index+1}.log';path.write_text(result);print(f'Saved {path}')
            else:
                save(args.run_dir/f'{args.action}-node{index+1}.json',result)
                print(json.dumps({'node':index+1,**result}))


if __name__ == '__main__':
    main()
