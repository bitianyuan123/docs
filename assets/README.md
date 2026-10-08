# 当前接口与数据附件

本目录保存本轮可审阅的逻辑合同、真实输入证据与文档验证结果。**没有创建在线特征服务，也没有执行模型请求。**

| 文件 | 用途与边界 |
|---|---|
| [完整请求教学样例](walkthrough_sample.json) | 本轮新增；用户 1 从请求到返回的具体字段，向量/SID/模型分数为标记清楚的教学值 |
| [按接口拆分的完整输入输出](request_example/README.md) | 同一次请求的 10 组具体 JSON 请求与响应；公共字段均已展开 |
| [源码与前期资料快照](source_snapshots/README.md) | 文中引用的 37 份只读文件，保留原行号、提交与校验值，支持独立 clone 阅读 |
| [教学样例关系检查](walkthrough_checks.json) | 检查字段来源、候选次序、缺失处理和分数归属，不执行服务或模型 |
| [contracts.py](contracts.py) | 目标特征接口的数据类型；当前 OneTrans HTTP 字段单列，两者不混接 |
| [场景配置模板](scene.feature_service.template.json) | 服务职责、三种查询、版本和预算；尚缺真实模型与服务地址绑定，不能上线 |
| [真实输入样例目录](feature_examples) | 用户 1、历史、词项、物品统计的键值及查询示例；发布状态仍为 BUILDING |
| [tenrec_sample_evidence.json](tenrec_sample_evidence.json) | 前 20 万行的来源与统计证据。其旧 candidate_dense/user_dense 字段仅是历史派生记录 |
| [source_manifest.json](source_manifest.json) | 复制来源及 SHA256，含已归档的旧附件 |
| [archive_v2](archive_v2/README.md) | 旧直连 Redis、完整特征精排、请求级 KV 等方案，已被本轮边界替代 |
| [审阅记录](review_report.md) | 跨文档审阅、具体问题和修正结果 |
| [静态检查](validation.json) | 默认检查仓库文档与附件；仅显式指定源工作区时重新核对数据和原工程，不是在线验收 |
| [渲染说明](RENDERING.md) | 用本地字体和 Mermaid 再生成阅读版、图源码、SVG |

`feature_examples` 的用户上下文示例只请求用户、历史和稀疏词项，明确将向量视图标为 `NOT_REQUESTED`。它演示可核实的数据，不代表完整三路召回已经就绪。历史 ID→SID 的请求模板缺少实际 `sid_version`，不可直接发送。该目录保留真实证据口径；新 `walkthrough_sample.json` 则按用户要求提供自洽的教学值，两者用途不同。

OneTrans 的示例单独保存 ID 与整数历史字段，并注明仅作接口构造；不证明其当前 Kafka/JSON/TSV 已装载这些用户或物品。物品 `4/1201` 来自真实曝光记录，用于接口说明，不是在线召回候选。

本轮以命名字段保存业务事实，缺失保留 null 与计数。旧附件的 15 维顺序与当前 OneTrans `/rank` 的本地 TSV 配方不同，禁止直接替换。
