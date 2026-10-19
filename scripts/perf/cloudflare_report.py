#!/usr/bin/env python3
"""Report all retained Cloudflare windows, including failures and partial runs."""
import argparse
import csv
import json
from pathlib import Path

from report import LABELS


def warmup_errors(source, run):
    """Raw warmup files survive an interruption before the aggregate is saved."""
    paths = list(source.glob(f"warmup-{run['label']}-*.json"))
    if paths:
        return sum(json.loads(path.read_text())['errors'] for path in paths)
    return run.get('warmup_errors', 0)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',nargs='+',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--summary-clients',type=int,default=8)
    args=parser.parse_args()
    meta=json.loads((args.input[0].parent/'deployment.json').read_text())
    runs=[json.loads(p.read_text()) for p in args.input]
    cases=[]
    for source,run in zip(args.input,runs):
        for case in run['cases']:
            warmup_path=source.parent/f"warmup-{case['phase']}.json"
            errors=json.loads(warmup_path.read_text())['errors'] if warmup_path.exists() else None
            cases.append(dict(case,label=run['label'],warmup_errors=errors))
    args.output.mkdir(parents=True,exist_ok=True)
    columns=['label','operation','clients','completed','errors','elapsed_s','requests_per_s',
             'items_per_s','p50_ms','p95_ms','p99_ms','covers_all_cells','write_witnesses_verified','warmup_errors']
    with (args.output/'windows.csv').open('w') as stream:
        writer=csv.DictWriter(stream,fieldnames=columns,extrasaction='ignore',lineterminator='\n');writer.writeheader();writer.writerows(cases)
    warmups=[warmup_errors(p.parent,r) for p,r in zip(args.input,runs)]
    warmup=sum(warmups)
    failures=sum(c['errors'] for c in cases)
    successes=sum(c['completed'] for c in cases)
    complete=all('completed_at' in r for r in runs)
    lines=['# BeyondDB on Cloudflare Containers and R2','',
           f"Runtime source `{meta['source_revision']}`; binary SHA-256 `{meta['binary_sha256']}`.",'',
           '**Three database nodes: 1 vCPU, 3072 MiB memory, 5000 MB ephemeral disk each.** '
           'One measured table spans 30 ordered ranges, ten per node. Metadata/coordinator Cells '
           'and the peer bridge share those node budgets. Native balancing is paused for controlled placement.','',
           'Durability: direct R2 S3 bucket publication; follower durability disabled. '
           'Uniform 1-KiB payloads, strong GetItem/Query, two-item cross-node transactions and disabled SDK retries. '
           'External workstation traffic includes WAN, HTTPS Worker ingress and private WSS peer transport.','',
           f"**{len(cases)} retained windows: {successes:,} successful calls, {failures:,} measured errors and {warmup:,} warmup errors.** "
           f"All requested runs reached final inventory verification: **{complete}**.",'',
           'Earlier setup failures and their empty generation are retained in the local run record. '
           'The empty failed table was retired through the normal API before the measured generation. '
           'Disk, metadata and resource observations include retained earlier roots.','',
           f'## At {args.summary_clients} clients','',
           '| API | Requests/s | p50 ms | p95 ms | p99 ms | Successes | Errors | Coverage |',
           '| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |']
    def row(c,include_label=False):
        values=[LABELS[c['operation']],c['requests_per_s'],c['p50_ms'],c['p95_ms'],c['p99_ms'],
                c['completed'],c['errors'],'all 30' if c['covers_all_cells'] else 'incomplete']
        return '| '+' | '.join('—' if v is None else str(v) for v in values)+' |'
    for op in LABELS:
        for c in cases:
            if c['clients']==args.summary_clients and c['operation']==op:lines.append(row(c))
    lines+=['','![Successful throughput and successful-call p99; failed windows remain in the tables](throughput-latency.svg)','',
            'Percentiles use nearest rank over successful calls only. Fewer than 1,000 successes '
            'give a weak p99 estimate. Failed calls retain their own latencies in raw JSON. '
            'Zero or missing successful samples do not establish a latency distribution.','',
            'Throughput divides successful calls by actual elapsed time, including completion drain. '
            'Query and transaction requests each contain two items; Get/Put contain one. '
            'Workers stop after three errors. Failed/incomplete windows are not healthy capacity.','',
            '## Every retained window','']
    for r,w in zip(runs,warmups):
        lines += [f"### {r['label']}",'',
                  '| API / clients | Requests/s | p50 ms | p95 ms | p99 ms | Successes | Errors | Coverage |',
                  '| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |']
        for c in r['cases']:
            lines.append(row(c).replace(LABELS[c['operation']],f"{LABELS[c['operation']]} / {c['clients']}",1))
        lines += ['',f"Separate warmup errors: **{w}**. Completion recorded: **{'completed_at' in r}**.",'']
    samples=[]
    for r,p in zip(runs,args.input):
        path=p.parent/f"resource-samples-{r['label']}.jsonl"
        if path.exists():samples += [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    lines+=['## Resource observations','',
            '| Node | Location(s) | Peak sampled process RSS MiB | Maximum sampled disk use GB | Peak sampled open FDs |',
            '| --- | --- | ---: | ---: | ---: |']
    for i in range(1,4):
        values=[s['nodes'][f'perf30-node{i}']['agent'] for s in samples
                if s['nodes'].get(f'perf30-node{i}',{}).get('agent')]
        rss=[]
        for v in values:
            for line in (v.get('process_status') or '').splitlines():
                if line.startswith('VmRSS:'):rss.append(int(line.split()[1])/1024)
        locations=sorted({v['platform']['CLOUDFLARE_LOCATION'] for v in values})
        disk=max((v['disk']['used']/1e9 for v in values),default=0)
        fds=max((v.get('process_open_fds') or 0 for v in values),default=0)
        lines.append(f"| {i} | {', '.join(locations) or 'unavailable'} | {max(rss,default=0):.1f} | {disk:.3f} | {fds} |")
    lines+=['','These are sampled observations, not hard peaks. Process RSS excludes other guest memory. '
            'Cloudflare instance configuration is the allocation proof; guest CPU count/MemTotal '
            'can include platform scheduling and VM overhead. Full guest/process/cgroup observations '
            'and sampling errors remain in the raw artifacts.','',
            'Closed-loop characterization only: no coordinated-omission correction, arrival-rate SLO '
            'or peak fleet-capacity claim. Recovery and broader production qualification require separate evidence.','']
    (args.output/'report.md').write_text('\n'.join(lines))
    combined=dict(runs[0],cases=cases,warmup_errors=warmup,
                  runs=[{'label':r['label'],'completed': 'completed_at' in r} for r in runs])
    (args.output/'combined-results.json').write_text(json.dumps(combined,indent=2)+'\n')
    print(f'Reported {len(cases)} windows; {failures} measured errors; {warmup} warmup errors; complete={complete}')


if __name__=='__main__':main()
