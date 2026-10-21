#!/usr/bin/env python3
"""Fail-closed integration against real Docker MySQL/Debezium/Kafka/Postgres."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

import pymysql
import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT/'lab'
EVIDENCE = Path(os.environ.get('CDC_EVIDENCE_DIR', str(LAB/'evidence')))
EVIDENCE.mkdir(parents=True, exist_ok=True)
checks = []


def check(name, condition, details=None):
    record = {'name': name, 'passed': bool(condition), 'details': details}
    checks.append(record)
    (EVIDENCE/'checks.json').write_text(json.dumps(checks, ensure_ascii=False, indent=2))
    if not condition:
        raise AssertionError(record)
    print('PASS', name, flush=True)


def eventually(fn, seconds=180):
    deadline = time.monotonic()+seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except Exception as exc:
            last = str(exc)
        time.sleep(1)
    raise TimeoutError(f'Integration dependency did not become ready: {last}')


def mysql():
    return pymysql.connect(host='127.0.0.1', port=23316, user='root', password='cdc-book-local', autocommit=True,
                           cursorclass=pymysql.cursors.DictCursor)


def sql(connection, query, args=None):
    with connection.cursor() as cursor:
        cursor.execute(query, args)
        return cursor.fetchall()


def request(path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request('http://127.0.0.1:28083'+path, data=data,
                                 headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.load(response)


def connector(name, prefix, slow=False):
    cfg = {'connector.class': 'io.debezium.connector.mysql.MySqlConnector', 'tasks.max': '1',
      'key.converter': 'org.apache.kafka.connect.json.JsonConverter', 'key.converter.schemas.enable': 'false',
      'value.converter': 'org.apache.kafka.connect.json.JsonConverter', 'value.converter.schemas.enable': 'false',
      'database.hostname': 'mysql', 'database.port': '3306', 'database.user': 'cdc',
      'database.password': 'cdc-book-local', 'database.server.id': '184055' if slow else '184056',
      'topic.prefix': prefix, 'database.include.list': 'inventory', 'table.include.list': 'inventory.products',
      'schema.history.internal.kafka.bootstrap.servers': 'kafka:9092',
      'schema.history.internal.kafka.topic': prefix+'.schema-history', 'snapshot.mode': 'initial',
      'include.schema.changes': 'false', 'topic.creation.default.replication.factor': '1',
      'topic.creation.default.partitions': '1', 'heartbeat.interval.ms': '1000'}
    if slow:
        cfg['snapshot.select.statement.overrides'] = 'inventory.products'
        cfg['snapshot.select.statement.overrides.inventory.products'] = 'SELECT * FROM inventory.products WHERE SLEEP(0.003)=0'
    request('/connectors', {'name': name, 'config': cfg})
    return cfg


def worker(group, generation, topic, extra=()):
    admin = AdminClient({'bootstrap.servers':'127.0.0.1:29092'})
    eventually(lambda: topic in admin.list_topics(timeout=10).topics)
    command = [sys.executable, str(LAB/'consumer.py'), '--group', group, '--generation', generation,
               '--topic', topic, *extra]
    result = subprocess.run(command, capture_output=True, text=True, timeout=150)
    (EVIDENCE/(group+('-replay' if '--replay' in extra else '')+'.log')).write_text(result.stdout+'\n'+result.stderr)
    if result.returncode:
        raise RuntimeError(f'Consumer failed ({group}): {result.stdout}\n{result.stderr}')
    return json.loads(result.stdout.strip().splitlines()[-1])


def source_rows(source):
    rows = sql(source, 'SELECT * FROM inventory.products ORDER BY id')
    return rows


def projection_rows(db, generation):
    return [row[0] for row in db.execute('SELECT document FROM products_projection WHERE generation=%s AND NOT deleted ORDER BY id', (generation,)).fetchall()]


def reconcile(source, db, generation, label):
    a = source_rows(source)
    b = projection_rows(db, generation)
    dump = lambda rows: json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    evidence = {'source_rows': len(a), 'projection_rows': len(b), 'source_sha256': hashlib.sha256(dump(a)).hexdigest(),
                'projection_sha256': hashlib.sha256(dump(b)).hexdigest(), 'all_rows_all_fields_equal': a == b}
    (EVIDENCE/(label+'-reconciliation.json')).write_text(json.dumps(evidence, indent=2))
    check(label+' exact full-row reconciliation', a == b, evidence)
    return evidence


def main():
    start = time.time()
    source = eventually(mysql)
    pg = eventually(lambda: psycopg.connect('postgresql://book:cdc-book-local@127.0.0.1:25436/book', autocommit=True))
    eventually(lambda: request('/connector-plugins'))
    versions = {'mysql': sql(source, 'SELECT VERSION() AS version')[0]['version'],
                'postgres': pg.execute('SELECT version()').fetchone()[0],
                'connect_plugins': request('/connector-plugins'), 'python': sys.version,
                'kafka': (EVIDENCE/'kafka-version.txt').read_text().strip(),
                'images': {name:json.loads((EVIDENCE/('image-'+name+'.json')).read_text()) for name in ('mysql','kafka','connect','postgres')}}
    (EVIDENCE/'versions.json').write_text(json.dumps(versions, indent=2))
    for statement in (LAB/'schema.sql').read_text().split(';'):
        if statement.strip():
            sql(source, statement)
    with source.cursor() as cursor:
        cursor.executemany('INSERT INTO inventory.products VALUES (%s,%s,%s,%s,%s)',
                           [(i, 'sku-'+str(i), i*17, 1, 1) for i in range(1,2001)])
    pg.execute((LAB/'projection.sql').read_text())
    cfg = connector('book-source', 'book', slow=True)
    (EVIDENCE/'connector-config.json').write_text(json.dumps(cfg, indent=2))
    def active_scan():
        return [r for r in sql(source, 'SHOW FULL PROCESSLIST') if r.get('Info') and 'SELECT * FROM inventory.products WHERE SLEEP' in r['Info']]
    before = eventually(active_scan)
    timestamp_before = sql(source, 'SELECT UTC_TIMESTAMP(6) AS t')[0]['t'].isoformat()
    sql(source, "INSERT INTO inventory.products VALUES (3001,'snapshot-concurrent-insert',321,1,1)")
    sql(source, "UPDATE inventory.products SET price_cents=777,revision=2 WHERE id=1")
    sql(source, 'DELETE FROM inventory.products WHERE id=2')
    timestamp_after = sql(source, 'SELECT UTC_TIMESTAMP(6) AS t')[0]['t'].isoformat()
    after = active_scan()
    overlap = {'before': before, 'after': after, 'write_start_mysql_utc': timestamp_before,
               'write_end_mysql_utc': timestamp_after, 'write_ids': [3001,1,2]}
    (EVIDENCE/'snapshot-write-overlap.json').write_text(json.dumps(overlap, default=str, indent=2))
    check('writes occurred while real snapshot SELECT remained active', bool({r['Id'] for r in before} & {r['Id'] for r in after}), overlap)
    first = worker('main', 'live', 'book.inventory.products')
    (EVIDENCE/'initial-consume.json').write_text(json.dumps(first, indent=2))
    check('real snapshot and binlog r/c/u/d captured', all(first['ops'].get(op,0)>0 for op in ('r','c','u','d')), first['ops'])
    check('real binlog file and position captured', any(p.get('file') and p.get('pos') for p in first['source_positions']))
    reconcile(source, pg, 'live', 'snapshot-plus-binlog')
    replay = worker('main', 'live', 'book.inventory.products', ['--replay'])
    check('duplicate replay detected in transactional inbox', replay['duplicates'] == first['messages'] and replay['fresh'] == 0, replay)
    reconcile(source, pg, 'live', 'duplicate-replay')
    before_invalid = (pg.execute('SELECT count(*) FROM products_projection').fetchone()[0], pg.execute('SELECT count(*) FROM delivery_history').fetchone()[0])
    rejections=[]
    for group,generation,topic in [('main','forbidden-new','book.inventory.products'),('other','live','book.inventory.products'),('main','live','wrong-topic')]:
        bad=subprocess.run([sys.executable,str(LAB/'consumer.py'),'--group',group,'--generation',generation,'--topic',topic],capture_output=True,text=True,timeout=15)
        rejections.append({'group':group,'generation':generation,'topic':topic,'returncode':bad.returncode,'explicit_binding_error':'Generation must bind' in bad.stderr})
    after_invalid = (pg.execute('SELECT count(*) FROM products_projection').fetchone()[0], pg.execute('SELECT count(*) FROM delivery_history').fetchone()[0])
    check('generation group topic mismatch rejected without pollution', all(r['returncode']!=0 and r['explicit_binding_error'] for r in rejections) and before_invalid==after_invalid and pg.execute('SELECT count(*) FROM consumer_binding').fetchone()[0]==1, rejections)
    # Fresh group: one durable DB transaction, then forcibly kill the real process
    # while its code is blocked before consumer.commit().
    marker = EVIDENCE/'kill-ready.json'
    command = [sys.executable, str(LAB/'consumer.py'), '--group', 'crash', '--generation', 'crash-gen',
               '--topic', 'book.inventory.products', '--crash-after-db', '--ready-file', str(marker)]
    with (EVIDENCE/'crashed-process.log').open('w') as log:
        child = subprocess.Popen(command, stdout=log, stderr=log)
        eventually(lambda: marker.exists(), 60)
        position = json.loads(marker.read_text())
        check('database committed before process kill', pg.execute('SELECT count(*) FROM delivery_history WHERE consumer_group=%s', ('crash',)).fetchone()[0] == 1)
        probe = Consumer({'bootstrap.servers':'127.0.0.1:29092','group.id':'crash','enable.auto.commit':False})
        committed = probe.committed([TopicPartition(position['topic'],position['partition'])], timeout=10)[0].offset
        probe.close()
        check('broker offset uncommitted at kill barrier', committed < 0 or committed <= position['offset'], {'broker_next_offset':committed, **position})
        child.kill()
        child.wait(timeout=10)
        check('real subprocess terminated by SIGKILL', child.returncode == -signal.SIGKILL)
    restarted = worker('crash', 'crash-gen', 'book.inventory.products')
    check('restart re-delivers committed-database event', restarted['duplicates'] >= 1, restarted)
    reconcile(source, pg, 'crash-gen', 'crash-restart')
    # No consumer runs while these real source transactions create backlog.
    for i in range(100):
        sql(source, 'UPDATE inventory.products SET price_cents=price_cents+1,revision=revision+1 WHERE id=%s', (10+i,))
    sql(source, 'DELETE FROM inventory.products WHERE id=10')
    sql(source, "INSERT INTO inventory.products VALUES (4001,'after-outage',222,1,1)")
    stale = source_rows(source) != projection_rows(pg, 'live')
    check('stopped consumer leaves projection observably stale', stale)
    caught = worker('main', 'live', 'book.inventory.products')
    check('backlog drained after consumer restart', caught['ops'].get('u',0)>=100 and caught['ops'].get('d',0)>=1, caught)
    reconcile(source, pg, 'live', 'backlog-catchup')
    sql(source, "ALTER TABLE inventory.products ADD COLUMN note VARCHAR(80) NULL")
    sql(source, "UPDATE inventory.products SET note='new schema field',revision=revision+1 WHERE id=1")
    # Existing untouched rows receive the schema default on rebuild; live schema
    # migration normalizes the same null default before reading new events.
    pg.execute("UPDATE products_projection SET document=document || '{\"note\":null}'::jsonb WHERE generation='live'")
    evolved = worker('main', 'live', 'book.inventory.products')
    check('real ALTER and subsequent binlog update consumed', evolved['ops'].get('u',0)>=1)
    check('added field reaches query document', pg.execute("SELECT document->>'note' FROM products_projection WHERE generation='live' AND id=1").fetchone()[0] == 'new schema field')
    reconcile(source, pg, 'live', 'schema-evolution')
    connector('book-rebuild', 'rebuild', slow=False)
    rebuilt = worker('rebuild', 'new-generation', 'rebuild.inventory.products')
    check('isolated new snapshot rebuild consumed', rebuilt['ops'].get('r',0)==len(source_rows(source)), rebuilt['ops'])
    reconcile(source, pg, 'new-generation', 'full-rebuild')
    check('live generation preserved during rebuild', projection_rows(pg,'live') == projection_rows(pg,'new-generation'))
    statuses = {name: request('/connectors/'+name+'/status') for name in ('book-source','book-rebuild')}
    check('both actual connectors and tasks RUNNING', all(s['connector']['state']=='RUNNING' and len(s['tasks'])==1 and all(t['state']=='RUNNING' for t in s['tasks']) for s in statuses.values()), statuses)
    result = {'status':'PASS','skipped':0,'checks':checks,'check_count':len(checks),'duration_seconds':round(time.time()-start,3),
              'versions':versions,'run_url':os.environ.get('BOOK_RUN_URL'),'source_revision':os.environ.get('BOOK_SOURCE_SHA'),
              'boundaries':['single Kafka broker; one data partition','real Debezium initial snapshot and MySQL binlog',
                            'local teaching credentials only','no multi-node failover or binlog-loss proof',
                            'cross-store delivery is at-least-once with projection convergence; not end-to-end exactly-once']}
    (EVIDENCE/'runtime.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps({'status':'PASS','checks':len(checks),'seconds':result['duration_seconds']}))


if __name__ == '__main__':
    main()
