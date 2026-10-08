# 源码核对、主要缺口与验收边界

本轮只修改架构文档、接口设计附件和图形，没有修改原工程、启动数据库或执行端到端模型验收。采用最新用户要求：OneTrans 按实际代码描述，其他模块按独立特征服务的理想架构规划。

## 1. 哪些决定更新了

| 原文安排 | 本轮有效安排 | 原因 |
|---|---|---|
| PaiRec 内部特征模块直接查 Redis | 独立 FeatureService，PaiRec/必要子服务调用业务接口 | 统一用户、历史、物品和表示的查询边界 |
| 业务特征固定两次读 | 两个业务阶段；正常三路请求共四次特征 RPC | 显式包含历史 ID 查 SID、生成 SID 反查 ID |
| 用户与物品统一为 15 维数组 | 特征库存命名字段、缺失标志和表示版本 | 避免业务事实绑定到单一模型的张量配方 |
| OneTrans 主链改 `/score`，使用新特征 | 主链保留 `/ingest`、`/rank` 及本地文件 | 按实际源码，不把规划当作已实现 |
| 请求级 KV、ReadyHandle、Release | 当前按模型与用户生成 KV key；无该 Release 协议 | 如实暴露历史一致性与生命周期缺口 |
| SID 映射作为模型服务本地业务表 | 原始 ID 绑定由特征服务正反向查询；码本和分词器仍是模型资产 | 统一业务表示，同时保留模型计算职责 |

前期文档只在与本表相容的范围内继续适用。完整字段与查询规则以[特征字典](10_feature_catalog.md)为准；OneTrans 实际接口以[源码视图](02_onetrans.md)为准。

## 2. 实际源码版本与事实

工程目录名仅用于定位证据，不作为系统组件名称。

| 核对目录 | 本次源码提交 |
|---|---|
| `pairec4tigerllm` | `cadc1855404b29eba40ab42b0278ebed9f91817a` |
| `pairec4tigerllm_8506` | `530f77327f0d7f7f15f54b1ddbfe422b58e94a88` |
| `pairec_sh` | `f7c4c49cbea73bada4dbb45fcd7d89078496a6e2` |
| `OneTrans_HSE_project` | `16aecd6f7bd1164bf7760603349e95ae716bf60e` |

新应用仍选官方 PaiRec v2.6.2 为固定依赖；这是既定选择，不宣称最新版，本轮没有创建新应用工程。[上游路由证据](assets/source_snapshots/prior_design/recommendation_design_assets/source_pairec_upstream_v2.6.2/route.go.html#L65)。

| 需要辨清的事实 | 源码证据及意义 |
|---|---|
| 已有用户特征前置接线 | [注册位置](assets/source_snapshots/pairec_sh/pairec-demo/src/dao/feature_brpc_redis_dao.go.html#L252)，不能再说完全没前置；也不能据此说独立特征服务已完成 |
| Redis 批读仍需修正 | [回复解析](assets/source_snapshots/pairec_sh/pairec-demo/src/cpp/brpcClients/redis_client.cpp.html#L136)按顶层回复数匹配 key；应解析数组并保留缺失位置 |
| 稀疏查询与目标配方不同 | [调用方](assets/source_snapshots/pairec_sh/pairec-demo/src/recall/brpc_sparse_recall.go.html#L68)传 `video_category`；[后端](assets/source_snapshots/pairec_sh/pairec-demo/src/cpp/brpcClients/opensearch_client.cpp.html#L98)为 `match.content`，不是结构化加权词项接口 |
| OneTrans 启动仍装本地表 | [启动加载](assets/source_snapshots/OneTrans_HSE_project/cpp/tools/server_main.cpp.html#L214)，`/score` 存在不等于默认去掉本地依赖 |
| OneTrans 实际字段配方不同 | [RankAssembler](assets/source_snapshots/OneTrans_HSE_project/cpp/src/serving/rank_assembler.cpp.html#L18)，用户和物品 TSV 真实列意义见专文；不可套用旧 15 维附件 |
| S 计算不是一律提前 ACK | 当前计算和写入完成后返回；旧编排闸门的完成通知不等于成功或历史一致性 |
| 原生 bRPC 与手写 Go 协议不同 | [现有 Go 客户端](assets/source_snapshots/pairec4tigerllm_8506/services/brpcwire/client.go.html#L155)，不能冒充新原生库调用链 |
| 视频类型不是主题品类 | [已保存作者回复](assets/source_snapshots/prior_design/recommendation_design_assets/tenrec_audit/official_sources/issue_11_comments.json.html#L31)，保留真实编码，不补造标题、商品属性或主题 |

## 3. 抓大的开发缺口

| 重要工作 | 最小交付 | 完成证据 |
|---|---|---|
| 独立特征服务及装载 | 三个方法、六组业务键、发布清单；真实用户/历史/物品与派生表示 | 实际 RPC、批量读回、顺序与缺失校验、版本固定 |
| PaiRec 完整编排 | 统一入口、三路召回、两阶段取数、历史结果等待、精排及召回合并与重排 | 由 Nginx 发起一条请求，逐服务可核对输入输出 |
| OneTrans 现状链路可验证 | 现有 HTTP 与本地资产、参数服务、DataSystem 的真实运行 | 历史输入来源、TSV 覆盖、参数维度、实际后端、KV 命中、模型前向证据 |
| OneTrans 当前代码风险 | 用户级 KV、旧闸门、miss→0.5、缺行补零和后端回退需要识别 | 不把旧 KV 或常量分数误判为本请求完成；并发一致性单列未解决项 |
| 生成召回查询边界 | 历史 ID 查 SID；推理后 SID 批量反查原始 ID，一对多保留 | 真实编码资产、模型执行、正反表一致、合法候选 |
| 向量与稀疏召回 | 真实离线用户向量和词项、物品索引、独立查询服务 | Milvus/OpenSearch 被真实调用，结果可关联特征目录 |
| 通信和人工配置 | 类型化 RPC、预算、错误语义；人可选版本、候选数、启停和回滚 | 失败不伪装空结果；配置可审阅，输入输出可追溯 |

OneTrans 接入特征服务是可以另行设计的后续迁移，不是本轮必须完成的代码改造。其并发与失败语义仍会影响真实全链路验收，不能因保留现状而忽略。初次调通可选择已核实覆盖的固定用户并限制同用户并发，但这些操作不修复历史 key 设计。

```python
# 工程实施顺序；这是任务划分，不是已执行记录。
freeze_feature_dictionary_and_rpc_contracts()
build_real_snapshot_and_model_representations()
load_features_indexes_and_current_onetrans_assets()
verify_feature_service_and_each_model_service()
run_request_from_nginx_through_pairec()
verify_history_consistency_failure_paths_and_resource_bounds()
# 训练质量、约束解码和索引优化可在真实链路完成后推进。
```

## 4. 最小验收及证据要求

| 检查对象 | 必须看到什么 | 不能据此代替验收 |
|---|---|---|
| 特征查询 | 同 release、同历史指纹、表示版本匹配，缺失逐位置保留 | JSON 格式正确但数据库未装载 |
| 向量、稀疏召回 | 实际查询索引及其返回候选 | 返回示例数组或本地兜底而标称数据库执行 |
| 生成召回 | 实际引擎、输入 tokens、合法输出、特征服务反查 | 伪造 SID、常量候选或只看 `executed` 字段 |
| OneTrans 历史 | 输入来源、计算日志、真实 KV 写入结果 | 仅异步任务结束或 HTTP 200 |
| OneTrans 精排 | 按 ID 装配、参数查询、实际 KV 身份及前向、候选分数 | 仅 `trace.kv_hit=true`；命中可能是旧历史 |
| 最终结果 | 只含合法已打分候选，数量真实，人工规则可解释 | 用物品 4/1201 的接口样例充当召回输出 |
| 持续运行 | 重复/并发请求、失败、超时、重启、缓存容量 | 单次请求成功推断回收和并发隔离正确 |

当前 `/rank` 对外是 sigmoid 后的分数，不是 `/score` 的两列 logits。验收脚本应针对真实接口取证；需要的观测若当前响应没有，就从日志或必要的后续观测改造取得，不能在文档里虚构字段。精排 `0.5` 也可能是合法模型输出，因此不能只按数值判断 miss；必须结合实际执行记录。

## 5. 文档审阅与验证范围

[审阅记录](assets/review_report.md)记录本轮从系统职责、数据语义、源码路径、时序和可读性发现的问题及修正。机器检查结果为[静态校验](assets/validation.json)和[图形渲染](assets/render_validation.json)。

本轮重新检查真实输入证据、链接、结构化附件及图形，未重跑历史 OneTrans 数值对拍或在线集群。示例前 20 万行的用户、历史及物品统计可复核；真实 DSSM 用户向量、SID 与在线模型输出仍须用对应模型资产和运行环境生成。
