# 官方来源查阅记录

全部页面实际抓取，缓存被gitignore排除，不再分发第三方全文。

## Debezium connector for MySQL :: Debezium Documentation

https://debezium.io/documentation/reference/3.1/connectors/mysql.html

抓取：2026-10-21，451476 bytes，SHA256 `c0bbd46322c17b13af3169a846889002eaf90ea31ba9d49ff7711274de01ab80`。

原创核对摘要：快照阶段、ROW与FULL配置、删除包络、binlog与schema历史，支持本书接入协议与边界。

## Customization of Kafka Connect automatic topic creation :: Debezium Documentation

https://debezium.io/documentation/reference/3.1/configuration/topic-auto-create-config.html

抓取：2026-10-21，67229 bytes，SHA256 `cd6a756422a321c2085b16451aa3224b7f29ad109e3c10127a8622d6a99e029b`。

原创核对摘要：数据topic自动创建的分区和副本参数，配置不等于已验证扩容。

## MySQL :: MySQL 8.4 Reference Manual :: 19.2.1 Replication Formats

https://dev.mysql.com/doc/refman/8.4/en/replication-formats.html

抓取：2026-10-21，95566 bytes，SHA256 `cec93eaf3126ce2e0095b0f43f9787f75625d64eca7927527b543ed389e125a2`。

原创核对摘要：行日志、语句日志和混合格式的区别，本实验选用ROW。

## MySQL :: MySQL 8.4 Reference Manual :: 7.4.4 The Binary Log

https://dev.mysql.com/doc/refman/8.4/en/binary-log.html

抓取：2026-10-21，115458 bytes，SHA256 `64b8343defeb64df350ecd1a0601546487803683c7e8f60128c8a686ddb9e45d`。

原创核对摘要：二进制日志记录变更并有保留边界，日志不等于完整备份。

## Design | Apache Kafka

https://kafka.apache.org/39/design/design/

抓取：2026-10-21，133444 bytes，SHA256 `f8cc14d4a821a9cbc50ef7560d65f83531f79886d3c68f64b7395eeedf817a19`。

原创核对摘要：分区内有序、消费者位点与交付语义；外部系统原子性需要额外协议。

## confluent_kafka API &mdash; confluent-kafka 2.16.0 documentation

https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

抓取：2026-10-21，1185995 bytes，SHA256 `0bee7fda8bc589d9d2304c856e6464023831ccb9a3bbb3ac35b3eff74d907055`。

原创核对摘要：Consumer.poll/commit/committed接口及offset语义。实际抓取文档为2.16.0，实验固定2.10.0，所用API由真实实验核验。

## PostgreSQL: Documentation: 17: INSERT

https://www.postgresql.org/docs/17/sql-insert.html

抓取：2026-10-21，45605 bytes，SHA256 `f6f2d215c4938e754fd2c08be1dce0c1b7e7f147a367153d4890ce4c57a0df8e`。

原创核对摘要：ON CONFLICT唯一约束与条件更新，支撑数据库内幂等和防倒退。

## PostgreSQL: Documentation: 17: 13.2. Transaction Isolation

https://www.postgresql.org/docs/17/transaction-iso.html

抓取：2026-10-21，47618 bytes，SHA256 `e3475df0c76b214dbda0bc681015099763b83778e1355e17ee7b908e6fd38a26`。

原创核对摘要：数据库事务与可见性，不延伸为跨系统原子提交。
