# 附录二 官方来源与阅读方法

本书使用下列一手文档核对概念和配置。作者实际抓取的页面摘要、地址和SHA256见下载区sources.json及仓库research/sources.md。完整第三方页面仅保存在被忽略的本地缓存，不随仓库或电子书再分发。页面内容可能随官方维护改变，抓取摘要标识的是当次查阅内容，不代表永久版本。

- [Debezium 3.1 MySQL连接器](https://debezium.io/documentation/reference/3.1/connectors/mysql.html)：初始快照、日志配置、schema历史和删除包络。书中实验只覆盖明确列出的配置。
- [Debezium topic自动创建](https://debezium.io/documentation/reference/3.1/configuration/topic-auto-create-config.html)：连接器数据topic的创建参数，帮助解释分区与副本设置。
- [MySQL 8.4复制格式](https://dev.mysql.com/doc/refman/8.4/en/replication-formats.html)：区分行、语句和混合日志，理解ROW要求。
- [MySQL 8.4二进制日志](https://dev.mysql.com/doc/refman/8.4/en/binary-log.html)：日志用途、保留与位置边界，避免把日志当作无限备份。
- [Kafka 3.9文档](https://kafka.apache.org/39/design/design/)：分区顺序、消费位点和交付语义。Kafka事务保证不能自动扩展到任意外部数据库。
- [Confluent Python客户端API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)：Consumer轮询、同步提交、已提交位点与管理接口。抓取文档为2.16.0，实验依赖固定为2.10.0，所用API由真实实验核验。
- [PostgreSQL 17 INSERT](https://www.postgresql.org/docs/17/sql-insert.html)：ON CONFLICT与条件更新，支撑事件唯一约束和投影防倒退。
- [PostgreSQL 17事务隔离](https://www.postgresql.org/docs/17/transaction-iso.html)：数据库内可见性边界。单库事务不能被表述为跨MySQL、Kafka、PostgreSQL的全局事务。

阅读官方文档时，应先确认版本，再区分默认值、可选行为与实验配置。若官方支持某能力，而随书代码没有实现和验证，就不能把那项能力列为本书实验结论。相反，真实报告通过也只支持记录的版本、环境和故障范围。
