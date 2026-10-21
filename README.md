# 业务数据同步工程：从 CDC 到可重建索引

作者：Aias00。18章原创中文正文，前言、练习答案、官方来源、术语与完整代码附录。

通过真实MySQL8.4.11、Debezium3.1.2.Final、Kafka3.9.1、PostgreSQL17.11，验证快照期间写入、binlog增量、重复、SIGKILL恢复、停机追平、删除、Schema迁移与隔离重建。默认浅色，支持低饱和深色、搜索、字号和移动端无JS阅读。

- 在线阅读：https://aias00.github.io/cdc-data-pipeline-book/
- EPUB：https://aias00.github.io/cdc-data-pipeline-book/downloads/cdc-data-pipeline-book.epub
- 实验：见 [lab/README.md](lab/README.md)

环境均为合成数据与本地教学凭据。单broker/单分区，非全文搜索引擎或生产授权系统，不宣称端到端exactly-once。

## 构建

Python3.12、Pandoc3.12.1、JDK21（EPUBCheck）和Node22/Playwright1.64.1。

```sh
python3 scripts/build.py
python3 scripts/verify.py
sh scripts/epubcheck.sh
```

产品流水线执行真实集成、出版完整性、EPUBCheck零错警、独立ZIP完整重跑、全部章节320/390无JS与交互浏览器验收后部署。Release必须使用同次成功CI的book-downloads原产物。个人版权保留，许可见LICENSE。
