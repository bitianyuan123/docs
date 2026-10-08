# 用户 1：各业务接口的完整输入与输出

以下文件是同一次教学请求的具体 JSON，已展开公共字段，不含 `call_meta`、Python 表达式、变量名或省略号。请求标识为 `demo-user-1-001`；入口用户为 `"1"`，最终返回 4 个物品。数值来自[完整样例](../walkthrough_sample.json)，**不是线上日志或真实模型运行结果**。

| 调用方 → 被调用方 | 方法 | 完整请求 | 完整响应 |
|---|---|---|---|
| 客户端经 Nginx → PaiRec | `POST /api/recommend` | [请求](01_recommend.request.json) | [响应](01_recommend.response.json) |
| PaiRec → 特征服务 | `GetUserContext` | [请求](02_user_context.request.json) | [响应](02_user_context.response.json) |
| PaiRec → 特征服务 | `BatchGetItemRepresentations`：历史 ID 查 SID | [请求](03_history_sid.request.json) | [响应](03_history_sid.response.json) |
| PaiRec → 向量召回 | `Recall` | [请求](04_vector.request.json) | [响应](04_vector.response.json) |
| PaiRec → 稀疏召回 | `Recall` | [请求](05_sparse.request.json) | [响应](05_sparse.response.json) |
| PaiRec → 生成召回 | `Recommend` | [请求](06_generate.request.json) | [响应](06_generate.response.json) |
| 生成召回 → 特征服务 | `BatchGetItemRepresentations`：生成 SID 查 ID | [请求](07_generated_sid.request.json) | [响应](07_generated_sid.response.json) |
| PaiRec → 特征服务 | `BatchGetItemFeatures` | [请求](08_item_features.request.json) | [响应](08_item_features.response.json) |
| PaiRec 历史任务 → OneTrans 历史 | `POST /ingest` | [请求](09_ingest.request.json) | [响应](09_ingest.response.json) |
| PaiRec → OneTrans 候选 | `POST /rank` | [请求](10_rank.request.json) | [响应](10_rank.response.json) |

表格按接口组织，编号不是串行执行顺序。三路召回和历史任务的并行关系见[主时序](../../08_request_walkthrough.md)。生成服务处理第 06 项期间调用第 07 项，拿到物品 ID 后才返回第 06 项响应。

特征与召回文件表达目标业务载荷；它们不是已部署的接口抓包，也没有包含 bRPC 二进制封包。生成请求保留现有 `history[{value}]`，版本头属于待接入的目标扩展，响应保留 `recommendations`。OneTrans 两个 HTTP 载荷沿用当前源码字段，不附加目标特征版本头；其历史提供器和本地 TSV 覆盖是本教学例子的独立前提。

固定的预算和计时值只用于说明字段。历史写入的 `checksum` 为 64 个零字符的占位值，不是一次真实计算的校验值。完整数据来源与示例边界见主样例的 `metadata`。

这些文件由 [export_request_example.py](../export_request_example.py) 从完整样例导出；修改示例后先重新导出，再运行文档校验。映射清单见 [manifest.json](manifest.json)。
