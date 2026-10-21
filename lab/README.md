# 真实 CDC 实验

需要 Linux/macOS Docker Compose、Python3.12、至少约4GB可用内存、可访问镜像与Python依赖源。端口23316/29092/28083/25436必须空闲。凭据仅限本地教学；不要在已有业务数据库上执行初始化SQL。

```sh
export CDC_COMPOSE_PROJECT=cdc-book-learning-001
sh lab/run.sh
```

入口安装固定依赖、创建项目独占容器、保存实际镜像摘要与版本，执行真实MySQL→Debezium→Kafka→PostgreSQL集成，最后只清理指定Compose项目及其卷。不要并行运行两份（端口固定）。失败时evidence保留检查与容器日志，不会skip缺失依赖。重跑应使用空的evidence目录和新的项目身份，避免旧kill-ready.json污染控制信号；正式CI在独立目录执行。

实验依次验证快照期间真实写入、r/c/u/d、重复重放、三种绑定误用拒绝、DB提交而broker未提交时实际SIGKILL/重启、停机100次更新追平、删除、真实ALTER与显式投影默认值迁移、新connector全量重建、六次逐行逐字段对账。runtime.json包含22项检查及实际环境。数据是2000行合成商品；固定镜像digest与依赖见compose.yml和requirements.txt。

消费者是有限实验运行器，读到数据后空闲3秒结束，再由对账判定收敛；不是生产持续服务。单broker/单分区，未验证多机故障、日志过期、跨表原子可见性、任意DDL、权限撤回。PostgreSQL投影不是全文搜索引擎。数据库事务与Kafka位点分开提交，保证为至少一次交付下的幂等收敛，不是端到端exactly-once。

ZIP只包含源码、SQL、依赖、Compose和说明，不携带运行evidence/marker。出版CI会在独立解压目录从空容器环境重跑同一入口。独立证据通过发布artifact和下载区validation.json提供。
