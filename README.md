# 推荐系统架构：系统、模块与一次具体请求

更新日期：2026-10-08。本版按最新要求修订：**除 OneTrans 保留实际源码的数据路径外，其他模块的用户、物品、历史及派生表示统一通过独立特征服务查询。** 本目录是设计与源码核对文档，不是已部署系统验收报告；四个原工程未修改。

可打开[离线阅读版](index.html)浏览全部图与文档。Markdown 是编辑源，[Mermaid 图源码](diagrams)和[SVG 矢量图](rendered)由其生成。

## 克隆后如何阅读

```bash
git clone git@github.com:bitianyuan123/docs.git
cd docs
```

用浏览器直接打开仓库根目录的 `index.html`，无需启动服务或安装依赖。页面中的图已渲染，中文字体、源码证据快照和样例均随仓库提供；请保留整个目录。Git 平台或编辑器也可直接阅读 Markdown；若不支持 Mermaid，使用离线阅读版或 `rendered` 中的 SVG。

本仓库使用相对链接，不要求存在原作者的工作目录。源码引用打开的是保留行号与提交信息的[只读证据快照](assets/source_snapshots/README.md)，不是可构建的完整服务源码。修改图文后如何重新生成、怎样执行便携校验，见[维护说明](assets/RENDERING.md)。

本次重点修订[系统 4+1](01_system.md)和[用户 1 请求推演](08_request_walkthrough.md)：逻辑图表达业务能力依赖，开发图表达源码依赖，进程图表达并发执行，物理图明确进程到 Host 的归属，并用含数据库的场景验证；请求文档逐段解释字段来源、转换规则和模型计算含义。完整教学样例可在[JSON 附件](assets/walkthrough_sample.json)逐项核对。

图形符号见[图法与 UML 约定](DIAGRAM_NOTATION.md)：时序图按 UML 语义使用；普通结构和流程图明确标为非 UML。`call_meta` 等示意函数已经就地解释，另提供[10 组完整请求与响应](assets/request_example/README.md)，不必自行展开伪代码。

## 阅读顺序

| 先解决的问题 | 文档 |
|---|---|
| 特征服务需要几类数据库，各存什么，key/value 如何设计 | [特征数据库与接口字典](10_feature_catalog.md) |
| 系统整体如何划分职责和部署 | [系统 4+1](01_system.md) |
| 特征服务怎样查询、加工、装载、发布 | [特征服务 4+1](05_feature_data.md) |
| 一个具体请求经过每个服务时输入输出是什么 | [用户 1 的请求时序](08_request_walkthrough.md) |
| PaiRec 如何组织并发等待、召回合并与重排 | [编排层 4+1](03_pairec_orchestration.md) |
| 当前 OneTrans 到底从哪里取数、怎样计算 | [OneTrans 源码 4+1](02_onetrans.md) |
| 生成召回如何取得历史编码并还原候选 | [生成式召回 4+1](04_generative_recall.md) |
| 服务之间如何通信 | [RPC 4+1](06_rpc.md) |
| 向量、稀疏、网关如何工作 | [其他服务各自的 4+1](07_other_services.md) |
| 什么已有、什么待开发，怎样验收 | [证据与主要缺口](09_evidence_and_gaps.md) |
| 可复用的类型、配置和真实输入附件 | [附件说明](assets/README.md) |

## 本版统一边界

```yaml
请求入口: Nginx -> PaiRec
业务阶段: 召回 -> 召回合并 -> 精排 -> 重排
编排基线: 官方PaiRec v2.6.2固定依赖 + 自有RecommendEngine
目标特征服务:
  物理存储: Redis在线库 + 离线持久文件或对象存储
  逻辑数据: 用户、历史、物品、派生表示；另有发布管理数据
  查询方法: GetUserContext / BatchGetItemRepresentations / BatchGetItemFeatures
  调用方: PaiRec查公共输入；生成服务查推理后产生的SID
  正常样例: 两个业务取数阶段，PaiRec三次与生成服务一次特征RPC
OneTrans本轮:
  历史: 当前调用方的Kafka或本地JSON -> HTTP /ingest
  用户与候选特征: HTTP /rank只传ID，服务内部用本地TSV装配
  参数: 参数服务
  注意力状态: DataSystem
  完整特征score接口: 已存在，但没有接入特征服务，不作本轮默认主链
不属于特征库: 模型权重、参数表、模型计算产生的KV、检索引擎索引
```

离线预计算再装载的原则仍然合理：DSSM 用户向量、兴趣词项、物品语义编码等由离线生产，在线通过特征服务查询；Milvus/OpenSearch 的索引仍离线整体装载，模型权重仍由模型服务装载。特征服务保存有名称的业务字段，不强行统一成某个模型的 15 维数组。

## 图与术语

```yaml
逻辑视图: 核心业务职责、接口、对象及能力依赖
开发视图: 源码模块、静态依赖与构建产物
进程视图: 独立执行单元、并发、等待与状态所有权
物理视图: 进程实例到Host及执行环境的部署映射、网络与存储归属
场景视图: 用具体请求和异常检验上述四个视图
层级: 每种视图内部按职责或范围展开，不把调用、源码和部署混画
证据标记:
  目标: 需要实现或接入的设计
  当前源码: 已读实现，不等于当前已启动
  历史证据: 以前保存的运行或审计，本轮没有重新执行
  示例: 已有真实输入，或明确标记的教学数值；都不冒充模型运行记录
```

召回负责选择候选；召回合并负责融合、去重、资格过滤与限量；精排负责模型打分；重排应用分数与人工规则。SID 是物品语义编码，不是原始物品 ID。KV 是模型注意力的键值张量，不是用户画像。PS 是模型参数服务。OneTrans 的 S/P 表示历史计算，NS/D 表示候选计算；各文首次出现均解释，不能与生成模型的逐 token 解码混用。

## 版本与资料关系

本版覆盖前版“PaiRec 内部特征模块直连 Redis”“两次固定查询”“主链 OneTrans 改为完整特征 `/score`”等安排。旧的完整特征请求、15 维接口配方及请求级 KV/Release 设计已移至[历史附件](assets/archive_v2/README.md)，不作为当前接口合同。

仍保留前期的干净 PaiRec、自有固定编排、真实检索和计算、人工配置、外部产生测试负载等原则。前期资料包括[详细设计](assets/source_snapshots/prior_design/01_recommendation_detailed_design.md.html)、[开发指导](assets/source_snapshots/prior_design/02_recommendation_development_guide.md.html)、[旧版 4+1](assets/source_snapshots/prior_design/03_recommendation_4plus1_views.md.html)和[旧定稿契约](assets/source_snapshots/prior_design/recommendation_design_assets/contract_decisions.md.html)。前轮通过任务记录读取相关讨论；分享页未提供可读正文，未把打开链接算作读完讨论。

本轮的跨文档审阅与修正记录见[审阅记录](assets/review_report.md)。
