# 交付记录

《业务数据同步工程：从 CDC 到可重建索引》，作者Aias00，个人版权保留。

- 源码产品提交：`c39816db6f92728eeaf5946dcde9a3e1300f8ada`
- 同次完整成功CI：[39598024894](https://github.com/Aias00/cdc-data-pipeline-book/actions/runs/39598024894)，build与deploy均success。
- [在线阅读](https://aias00.github.io/cdc-data-pipeline-book/) · [EPUB](https://aias00.github.io/cdc-data-pipeline-book/downloads/cdc-data-pipeline-book.epub) · [Release v1.0.0](https://github.com/Aias00/cdc-data-pipeline-book/releases/tag/v1.0.0)
- 18章正文、25446个中文汉字（独立Pandoc AST排除代码计数）、前言、3原创附录与完整代码附录，共23阅读单元、4附录。

## 真实实验与独立复现

真实MySQL8.4.11、Debezium3.1.2.Final、Kafka3.9.1、PostgreSQL17.11，全部镜像固定digest。Python3.12与固定客户端依赖。

源目录22项检查101.994秒；下载ZIP独立解压后，以另一独占Compose项目从空服务重跑22项102.082秒。两轮全部通过、零skip、同一产品source_revision。实际检查包括同一真实快照查询期间插入/更新/删除、r/c/u/d、binlog文件位置、重复重放、三种绑定错误拒绝无污染、数据库已提交而broker位点未提交时SIGKILL、重启重复、停机积压追平、真实ALTER和显式投影默认值迁移、独立connector全量重建及六次逐行逐字段对账。

原始双轮日志、检查、连接器配置、进程屏障、镜像与版本在该CI的`real-cdc-evidence` artifact；canonical源目录报告为[book/validation.json](book/validation.json)，也随下载发布。ZIP为9个源文件，与产品提交逐字节核对，排除evidence、缓存和旧marker。

## 公开产物验证

所有Release附件直接取自该同次成功CI的`book-downloads`原artifact，未本地重建。

| 文件 | 字节数 | SHA256 |
|---|---:|---|
| EPUB | 104676 | `affb3912ee9275a3261ecc4a35d9e339687105c9046ce09debcfb4f9b08b074f` |
| lab.zip | 11580 | `e3311a5080e3763271c7a2cf16aeccaa3426677c34d07cccfee75d545b955640` |

实际匿名GET下载Pages与Release，两文件均与CI原artifact逐字节相同。公共下载EPUB重新执行EPUBCheck5.4.0，零错误、零警告。全部23个公共阅读单元逐篇全文与源码独立渲染比较相同。

公共Playwright验收通过：320/390全部章节无横向溢出，两种宽度全部章节无JS可读，深色系统下默认浅色、共享主题/标签页同步、字号持久化、目录、搜索、禁用存储降级、下载入口与无JS错误。

## 审核与边界

根代理独立审阅18章、前言、3原创附录和全部实验代码/配置，核对8个官方抓取缓存摘要，未发现剩余阻塞。完整第三方页面不再分发。

单broker/单数据分区、合成商品、本地教学凭据。PostgreSQL查询投影，不是全文搜索引擎。至少一次交付下幂等收敛，不宣称跨系统exactly-once、多机容灾、任意DDL或权限撤回。固定Connect镜像的已观察可选REST扩展Provider提示及可接受范围见第15章，不能泛化为忽略任意异常。

canonical证据与本交付记录为`[skip ci]`回写，不重建、不更改已发布资产；之后产品变更须使用新的补丁版本。
