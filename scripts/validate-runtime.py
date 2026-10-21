#!/usr/bin/env python3
"""Fail closed on any omitted, failed or skipped real integration check."""
from collections import Counter
import json
import os
import sys

EXPECTED = '''writes occurred while real snapshot SELECT remained active
real snapshot and binlog r/c/u/d captured
real binlog file and position captured
snapshot-plus-binlog exact full-row reconciliation
duplicate replay detected in transactional inbox
duplicate-replay exact full-row reconciliation
generation group topic mismatch rejected without pollution
database committed before process kill
broker offset uncommitted at kill barrier
real subprocess terminated by SIGKILL
restart re-delivers committed-database event
crash-restart exact full-row reconciliation
stopped consumer leaves projection observably stale
backlog drained after consumer restart
backlog-catchup exact full-row reconciliation
real ALTER and subsequent binlog update consumed
added field reaches query document
schema-evolution exact full-row reconciliation
isolated new snapshot rebuild consumed
full-rebuild exact full-row reconciliation
live generation preserved during rebuild
both actual connectors and tasks RUNNING'''.splitlines()
r = json.load(open(sys.argv[1]))
assert r['status'] == 'PASS' and r['skipped'] == 0
assert r['check_count'] == len(EXPECTED) == 22
assert Counter(c['name'] for c in r['checks']) == Counter(EXPECTED)
assert all(c['passed'] is True for c in r['checks'])
assert r['source_revision'] == os.environ['BOOK_SOURCE_SHA']
assert r['run_url'] == os.environ['BOOK_RUN_URL']
assert r['versions']['kafka'].startswith('3.9.1')
assert all(r['versions']['images'][name] for name in ('mysql','kafka','connect','postgres'))
print('PASS: all 22 real checks, no skips, exact source revision, actual service versions and four image digests')
