#!/usr/bin/env python3
"""Real Kafka consumer; database transaction commits before broker offset commit."""
import argparse
import json
import os
import signal
import time
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, KafkaError

DSN = 'postgresql://book:cdc-book-local@127.0.0.1:25436/book'


def apply(db, group, generation, message):
    value = json.loads(message.value()) if message.value() is not None else None
    key = json.loads(message.key()) if message.key() else None
    # Kafka tombstones are compaction markers; the preceding Debezium delete
    # envelope, not the null value, carries the source position and operation.
    with db.transaction():
        fresh = db.execute('INSERT INTO delivery_history VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING 1',
                           (group, message.topic(), message.partition(), message.offset())).fetchone()
        if fresh and value:
            assert value['op'] in ('r', 'c', 'u', 'd'), value
            after = value['after']
            row = after if after is not None else value['before']
            assert key['id'] == row['id']
            document = {k: row[k] for k in ('id', 'sku', 'price_cents', 'active', 'revision')}
            if 'note' in row:
                document['note'] = row['note']
            # Same primary key is always on one partition; no cross-partition
            # numeric offset comparison is claimed here.
            db.execute('''INSERT INTO products_projection VALUES (%s,%s,%s,%s,%s,%s)
              ON CONFLICT(generation,id) DO UPDATE SET document=EXCLUDED.document,
              deleted=EXCLUDED.deleted,source_partition=EXCLUDED.source_partition,
              source_offset=EXCLUDED.source_offset
              WHERE products_projection.source_partition=EXCLUDED.source_partition
              AND products_projection.source_offset<EXCLUDED.source_offset''',
              (generation, row['id'], psycopg.types.json.Jsonb(document), value['op'] == 'd', message.partition(), message.offset()))
        db.execute('''INSERT INTO delivery_checkpoint VALUES (%s,%s,%s,%s)
            ON CONFLICT(consumer_group,topic,partition) DO UPDATE
            SET next_offset=GREATEST(delivery_checkpoint.next_offset,EXCLUDED.next_offset)''',
            (group, message.topic(), message.partition(), message.offset()+1))
    return bool(fresh), value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--group', required=True)
    parser.add_argument('--generation', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--crash-after-db', action='store_true')
    parser.add_argument('--ready-file')
    parser.add_argument('--replay', action='store_true')
    parser.add_argument('--idle-seconds', type=float, default=3)
    args = parser.parse_args()
    consumer = Consumer({'bootstrap.servers': '127.0.0.1:29092', 'group.id': args.group,
                         'enable.auto.commit': False, 'auto.offset.reset': 'earliest',
                         'enable.auto.offset.store': False})
    def assigned(c, partitions):
        if args.replay:
            for partition in partitions:
                partition.offset = 0
        c.assign(partitions)
    consumer.subscribe([args.topic], on_assign=assigned)
    db = psycopg.connect(DSN, autocommit=True)
    totals = {'messages': 0, 'fresh': 0, 'duplicates': 0, 'ops': {}, 'source_positions': []}
    last = time.monotonic()
    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                if totals['messages'] and time.monotonic()-last > args.idle_seconds:
                    break
                if time.monotonic()-last > 60:
                    raise TimeoutError('No messages available; integration may not be skipped')
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            fresh, value = apply(db, args.group, args.generation, msg)
            totals['messages'] += 1
            totals['fresh' if fresh else 'duplicates'] += 1
            if value:
                totals['ops'][value['op']] = totals['ops'].get(value['op'], 0)+1
                if len(totals['source_positions']) < 8:
                    totals['source_positions'].append(value['source'])
            if args.crash_after_db:
                if args.ready_file:
                    Path(args.ready_file).write_text(json.dumps({'topic': msg.topic(), 'partition': msg.partition(), 'offset': msg.offset()}))
                print('database committed; waiting for controller SIGKILL', flush=True)
                while True:
                    time.sleep(1)
            consumer.commit(message=msg, asynchronous=False)
            last = time.monotonic()
    finally:
        consumer.close()
        db.close()
    print(json.dumps(totals, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
