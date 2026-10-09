# pairec 编排进程：架构、负载与并发模型分析

> 聚焦上游 pairec 框架（`github.com/alibaba/pairec/v2`）**进程编排层内部**。
> 不涉及下游不成熟的 pairec4tigerllm 扩展，也不展开跨进程端到端流。
> 所有结论锚定 `pairec/` 源码，以 `file:line` 为据。

## 0. 分析边界与对象

本报告**仅分析上游 pairec 框架进程编排层内部**。研究对象是**一个 Go 进程内**的：4+1 视图、单请求数据流/时序、线程与并发模型、逐环节负载与瓶颈、节点规模放大、多请求并发影响。

pairec 是 Alibaba 开源的成熟推荐框架，其编排层设计稳定可考。本文档定位为 OS 下一代研究方向的输入与优化点分析的基础。

> 承重事实均经源码亲自核对（非仅转述子 agent 输出）：`service/user_recommend.go`、`service/pipeline/pipeline.go`、`service/recall.go`、`service/pipeline/user_recommend.go`、`service/pipeline/recall.go`、`service/rank/rank_service.go`、`service/feature/feature_service.go` 已逐行复核。

---

## 0.5 术语对照表

本文档面向 OS 研究者，读者不一定熟悉推荐系统与 pairec 框架行话。下表在**首次使用**前统一定义；正文不再逐一展开。术语按类别分组，"锚点"列给出源码位置以便复核。

### 0.5.1 推荐业务概念（推荐管线各 stage）

推荐服务把"为用户选 N 个物品"拆成一条管线，按执行顺序：

| 术语 | 译名/含义 | 在 pairec 里的角色 | 锚点 |
|---|---|---|---|
| **召回 (recall)** | 从海量候选(百万级)粗选出数百~数千个候选 item | 管线第 1 段；多路并发(热门/协同过滤/向量等)，每路返回一批 item 后合并 | `service/recall.go:54` |
| **过滤 (filter)** | 去重、去已曝光、去黑名单、多样性约束等 | 管线第 2 段；按 scene 查注册的 filter 链顺序执行 | `filter/filter.go:70` |
| **粗排 / 粗排 (general rank)** | 用轻量模型对召回的数千 item 打分、裁剪到数百 | 管线第 3 段；`GeneralRankService` | `service/general_rank/general_rank.go:216` |
| **特征装配 (feature)** | 给每个 item 拉取/计算模型所需特征(用户特征、item 特征、交叉特征) | 管线第 4 段；隐藏的 I/O 长杆，DAO fan-out 取数 | `service/feature/feature_service.go:77` |
| **精排 (rank)** | 用重模型(如 EAS 深度模型)对数百 item 精细打分，写回 `item.Score` | 管线第 5 段；两级 goroutine 嵌套并发调 EAS | `service/rank/rank_service.go:102` |
| **冷启排 (cold start rank)** | 对新 item/新用户的专门打分策略(冷启动 = 历史数据不足) | 管线第 6 段；**仅 pipeline 路径有**，主路径无 | `service/rank/cold_start_rank.go:14` |
| **重排 (sort)** | 在已有精排分数上做最终排序：去重、多样性、流量控制、业务规则 | 管线末段；DPP/SSD/ItemScore 等多种 sort 串行 | `sort/sort.go:65` |

### 0.5.2 框架配置概念

| 术语 | 含义 | 锚点 |
|---|---|---|
| **scene（场景）** | 请求级字符串，标识"这次推荐用在什么场景"（如"首页推荐""详情页推荐"）。**来自 HTTP 请求 body 的 `scene_id` 字段**，是 pairec 配置分发的**主键**——召回名、精排配置、过滤器、排序器都按 scene 查。一个 scene 对应一套推荐策略。 | 请求来源 `web/recommend_controller.go:36`；配置主键见下方各表 |
| **category（类目）** | 请求级字符串，scene 下的二级分类（如"服饰""数码"），缺省 `"default"`。与 scene 组成 `scene+category` 两级查召回名。**主路径用，pipeline 路径不用**。 | `web/recommend_controller.go:38-42` |
| **RecallNames / FilterNames / SortNames** | 字符串列表，指定本 scene/类目**用哪些已注册的召回/过滤/排序实例**。是"组件组合"的配置载体。 | `recconf.go:769`(CategoryConfig), `recconf.go:54-55` |
| **RankConf** | 精排配置（模型地址、算法列表、打分表达式）。**主路径按 scene 单槽**：`recconf.Config.RankConf[scene]`，一个 scene 只能一套。 | `recconf.go:79`, `rank_service.go:154` |
| **SceneConfs** | `map[scene]map[category]CategoryConfig`——主路径的配置源，scene→category→(RecallNames...) 两级映射。 | `recconf.go:78` |
| **PipelineConfs** | `map[scene][]PipelineConfig`——pipeline 路径的配置源，**scene 挂切片**，一个 scene 可挂 N 套独立编排。与 SceneConfs 是两套并行配置。 | `recconf.go:90` |
| **PipelineConfig** | pipeline 路径的单条编排配置块，自带 Name/RecallNames/FilterNames/RankConf/SortNames。**per-pipeline 独立**的是 RecallNames 和 RankConf；Filter/Sort 因 PipelineName 是死字段而退回 scene 级（见 §3.6.2）。 | `recconf.go:1100` |
| **CategoryConfig** | 主路径某 scene 某 category 的配置（RecallNames、Fallback、输出字段等）。 | `recconf.go:768` |
| **recconf** | `recconf.RecommendConfig`，整个 pairec 配置反序列化成的单一根结构体，含 SceneConfs/PipelineConfs/RecallConfs/SortConfs/... 全部子表。 | `recconf/recconf.go` |
| **AB 实验 / ExperimentResult** | AB 测试分流结果，挂在 `context.ExperimentResult` 上，让同 scene 的请求按实验桶注入不同参数（如覆盖 `pipelines.<name>.RecallNames`）。是热改配置的运行时通道。 | `context/*.go` |
| **fallback** | 真正的兜底机制，在 `TryRecommendWithFallback` 里，基于 `fallback.DefaultFallbackService`。**`/api/recommend` 不走它**，与双路径并行合并是两套独立机制。 | `user_recommend.go:206` |

### 0.5.3 并发与 OS 概念

| 术语 | 含义 |
|---|---|
| **stage** | 管线的一段（召回/过滤/.../重排各是一个 stage）。 |
| **G0 / G-pipe / G-recall 等** | 本文档对 goroutine 的记号：G0 = 主请求 goroutine，G-pipe = pipeline 路径 goroutine，G-recall = 召回扇出的子 goroutine。Go 原生词是 goroutine（用户态轻量线程，M:N 调度到 OS 线程）。 |
| **GMP** | Go 运行时调度模型：G(goroutine) M(OS 线程) P(调度器，容量=GOMAXPROCS)。网络 I/O 经 netpoller，G 阻塞时解绑 M 不膨胀线程；仅阻塞 syscall/文件 I/O 才占 M。详见 §3.2。 |
| **GOMAXPROCS** | P 的数量上限 = 可同时跑 CPU 的 goroutine 数。pairec 用 `automaxprocs` 按容器 CPU 配额自动设。 |
| **netpoller** | Go runtime 的网络 I/O 等待机制，底层在 Linux 用 epoll。goroutine 等网络数据时**解绑 M 挂到 netpoller，不占线程**——这是 pairec M(线程)数不随 QPS 膨胀的根因。详见 §3.2。 |
| **epoll** | Linux I/O 多路复用机制。一个线程用 epoll_wait 同时监听大量 fd，谁就绪返回谁。netpoller 即基于 epoll，pairec 4 万个 G 等网络响应实际只有少量 M 在轮询 epoll。 |
| **futex** | Linux "fast user-space mutex"，用户态快速互斥/等待原语。channel 和 sync.WaitGroup 的等待/唤醒底层用 atomic + futex wake，走纯内存交接（不经 epoll）。 |
| **eventfd** | Linux 轻量事件通知原语。Go runtime 内部偶用于唤醒 netpoller 轮询线程，pairec 业务代码不直接接触。 |
| **Grunnable / Gwaiting / Grunning** | goroutine 的三种状态：可运行（在 P 队列等跑）、等待（挂 netpoller/channel/wg，不占 M）、运行中（占 P-M-CPU）。 |
| **fan-out / fan-in** | fan-out = 一个任务拆成多个子任务并发；fan-in = 多个子任务结果合并回一处。pairec 用"buffered channel + 固定计数循环"做 fan-in。 |
| **fire-and-forget** | 派生 goroutine 后不等返回（用于日志/埋点等不阻塞响应的副作用）。 |
| **singleflight** | Go 标准库 `golang.org/x/sync/singleflight`：并发同 key 请求合并成一次后端调用。**pairec 未用**，是高并发缓存击穿的根因（§3.4、§6.4）。 |
| **RTT** | Round-Trip Time，一次网络往返时延（此处理论指到 Redis/Hologres/EAS 的往返）。 |
| **STW** | Stop-The-World，Go GC 暂停所有 goroutine 的阶段，是 p99 尾部抖动来源。 |
| **p99** | 延迟分布的 99 分位（还有 p999 等），衡量尾延迟。高 QPS 下 p99 抬升是本文关注核心。 |
| **clone（AB clone）** | pairec 的 AB 实验机制：`CloneWithConfig(params)` 用每实验参数克隆一个实例副本，不污染共享基实例。 |
| **AST** | Abstract Syntax Tree。精排的 `RankScore` 打分表达式以 AST 形式逐 item 求值（CPU）。 |
| **admission filter** | TinyLFU 缓存的准入策略，过滤掉大概率不会被再次访问的 key，降低长期 miss 率。 |
| **matmul** | 矩阵乘法。DPP 排序的 CPU 热点核心（gonum dense 实现）。 |

### 0.5.4 后端与算法概念

| 术语 | 含义 | 锚点 |
|---|---|---|
| **EAS** | 阿里云 PAI-EAS（Elastic Algorithm Service）模型推理服务。pairec 通过 `eas.PredictClient.BytesPredict` HTTP/gRPC 调用在线模型（精排、向量召回等）。 | `algorithm/eas/easyrec_request.go:17,38` |
| **DAO** | Data Access Object，数据访问对象。pairec 每种数据需求一个 `<thing>_dao.go` 接口 + 多后端实现（Redis/Hologres/ClickHouse/...），后端由配置选。 | `module/*_dao.go` |
| **TinyLFU / go-cache** | in-proc 两级缓存：TinyLFU（`goburrow/cache`，带 admission filter 的高效缓存）+ go-cache（纯内存 TTL 缓存）。位于 Redis/Hologres 之前。 | — |
| **DPP** | Determinantal Point Process（行列式点过程），一种多样性排序算法，保证推荐结果既相关又多样。CPU 复杂度 O(n²~n³)（含 matmul），n>200 时成 CPU 长杆。 | `sort/dpp_sort.go:29` |
| **SSD** | `SSDSort`，slate-based submodular 多样性排序（`useSSDStar` 为其变体），与 DPP 同属重排阶段的多样性 sort。 | `sort/ssd_sort.go:28` |

> 读者若遇到表外术语，可查 §8 速查索引（按 file:line 锚点反查源码）。

---

## 1. 4+1 视图（聚焦 pairec 进程内部）

### 1.1 逻辑视图

pairec 进程内是"分层 + 注册抽象"结构。核心是**一个请求经编排层串起多个 stage service，stage 通过全局注册表查实例、通过 AB clone 注入实验参数**：

```
┌────────────────── HTTP 路由层 (web/ + controller.go) ──────────────────┐
│ net/http ServeMux → ControllerRegister.ServeHTTP (controller.go:29)    │
│   defer recover → 500                                                  │
│ Route("/api/recommend") → RecommendController (reflect 建 instance)    │
│   Process: 读 body → UUID requestId → CheckParameter → makeContext     │
│            → doProcess → 序列化 ItemData[] 响应                        │
├──────────────────── 编排层 (service/) ─────────────────────────────────┤
│ UserRecommendService.Recommend (user_recommend.go:46)  ← 唯一编排心    │
│   ├─ LoadUserFeatures          (user 特征预取)                          │
│   ├─ go pipeline.Recommend     (第二路径, 并发)                         │
│   ├─ RecallService.GetItems    (主路径召回, 并发扇出)                   │
│   ├─ Filter                    (主路径过滤)                             │
│   ├─ GeneralRankService.Rank    (粗排/冷启分流)                         │
│   ├─ FeatureService.LoadFeatures (item 特征装配)                        │
│   ├─ RankService.Rank           (批 EAS 精排)                           │
│   ├─ wg.Wait + mergePipelineItems (双路合并)                           │
│   ├─ Sort                       (多策略重排)                           │
│   └─ 截断 size + fire-and-forget 日志/hook                             │
├─────────────── 可插拔注册层 (register.go + 各 *.Load) ────────────────┤
│ 全局 map[name]instance 工厂模式:                                       │
│   recall.RegisterRecall / sort.RegisterSort / filter.RegisterFilter    │
│   Load(config): 按 Type 大 if-else 分发 New<Type>(conf)                 │
│   md5-sign 跳过: 配置未变则不重建 (recallSigns, sortSigns)              │
│   ICloneRecall/ICloneSort: AB 参数 override 不污染基实例               │
├──────── 数据访问层 (module/*_dao.go + persist/ + datasource/) ────────┤
│ 一接口多后端: feature_dao → redis/hologres/mysql/clickhouse/hbase/...   │
│ persist/cache: localCache(go-cache) / redis                             │
│ module/feature_dao.go: in-proc TinyLFU(goburrow) + 100ms 超时           │
│ algorithm/: eas/tfserving/milvus/faiss/seldon (外部模型服务 HTTP/gRPC) │
└────────────────────────────────────────────────────────────────────────┘
```

关键抽象：**一个数据需求 = 一个接口 + N 个后端实现**。后端选择由配置驱动，换后端是改配置不是改代码。这决定了负载画像可由配置完全重塑——同一编排层，配 Redis 还是 Hologres，I/O 画像天差地别。

### 1.2 进程视图（单进程内的运行单元）

pairec 进程内的运行单元划分（OS 调度的直接对象）：

| 运行单元 | 创建点 | 生命周期 | 资源画像 |
|---|---|---|---|
| HTTP server goroutine | `app.go:101` `go ListenAndServe` | 进程级 | 阻塞在 accept |
| 每连接处理 goroutine | net/http 框架 | 每请求 | 随请求并发数线性增 |
| 配置热加载 goroutine | `configloader.go:176` `go loopLoadConfig` | 进程级,10s 轮询 | 低 |
| 日志清理 goroutine | `log.go:42` `go clearLoop` | 进程级,10s | 低 |
| fasttime ticker | `utils/fasttime/fasttime.go:10` | 进程级 | 微小 |
| AB/配置通知 goroutine | `recconf.go:1189` | reload 时瞬态 | 瞬态 |
| 每请求内 stage 扇出 goroutine | `user_recommend.go:76` `recall.go:138` `rank/rank_service.go:241,265,271` 等 | 每请求内 | **构成并发主体** |
| fire-and-forget 日志/hook | `user_recommend.go:169-174` | 每请求尾 | 短暂 |

**进程内无独立 worker pool**——所有并发都是"按请求即时派生 goroutine"模式,无全局长生命周期 worker 池。这是 pairec 并发模型的关键特征,决定了 §6 的并发负载形态。

### 1.3 物理视图（节点级）

单节点部署一个 pairec 进程,对外 HTTP(默认 8000,可配)。进程内依赖的外部后端(Redis/Hologres/MySQL/EAS/Milvus 等)通过配置注入,进程本身无状态——状态全在外部后端。进程内缓存(TinyLFU、go-cache)是 per-instance 的 in-process 缓存,进程重启即失。

### 1.4 开发视图(代码组织,仅编排层相关)

```
pairec/
├── pairec.go / app.go / register.go   启动序列 + 注册入口
├── controller.go / route.go          HTTP 路由 + 反射建 controller
├── web/                               HTTP 控制器 (RecommendController 等)
├── service/                           ← 编排层主体
│   ├── user_recommend.go             ★ 编排心 (Recommend/双路/merge)
│   ├── recommend.go                   基础 Filter/Sort/PreSort
│   ├── recall.go                      主路径召回并发扇出
│   ├── scene.go / category.go         scene→category→recallNames 解析
│   ├── pipeline/                      第二路径 (pipeline.go 扇出 + 各 stage)
│   ├── rank/  general_rank/  feature/ 精排/特征 service
│   └── debug/  fallback/  hook/       可观测/降级/hook
├── service/recall/*.go                召回实现 (BeRecall/Vector/Hologres/...)
├── sort/  filter/                     重排/过滤实现
├── module/*_dao.go                    一接口多后端 DAO
├── persist/  datasource/  algorithm/  后端 client + 连接池 + 模型服务
└── recconf/                          RecommendConfig 全量 schema
```

### 1.5 场景视图

用一个 `POST /api/recommend` 请求把内部串起来(§2、§3)。

---

## 2. 单请求:数据流、处理过程、时序

### 2.1 处理过程(编排层内部 stage 语义)

```
POST /api/recommend  {uid, size=10, scene_id, category}
    │
    ▼ [web/recommend_controller.go:Process]
    生成 requestId(UUID) → CheckParameter(默认 size=10, scene=default_scene)
    → makeRecommendContext(建 RecommendContext + AB MatchExperiment)
    → NewUserRecommendService().Recommend(ctx)   ← 编排心入口
    │
    ▼ [user_recommend.go:46 Recommend]
    LoadUserFeatures(user, ctx)                   user 特征预取
    │
    ├─(goroutine) pipeline.Recommend(user,ctx)   ← 第二路径 [pipeline.go:45]
    │     scene → userRecommendSceneMap[scene] → N 个 pipeline.UserRecommendService
    │     每个并发跑: Recall→Filter→GeneralRank→Feature→Rank→ColdStartRank→Sort
    │
    ├─(同步 主路径):
    │   RecallService.GetItems(user,ctx)         [recall.go:54] 并发扇出
    │     scene+category → recallNames → 每路一个 goroutine (defer recover)
    │     → buffered chan 合并 → items[]
    │   Filter(items)                            [recommend.go:33] 顺序过滤
    │   GeneralRankService.Rank(items)            粗排/冷启分流
    │   FeatureService.LoadFeatures(items)       item 特征装配 (隐藏 I/O 热点)
    │   RankService.Rank(items)                   批 EAS 精排
    │
    ▼ wg.Wait() [user_recommend.go:143]
    mergePipelineItems(items, pipelineItems)     [user_recommend.go:189]
       (同 id 合并属性, pipeline-only 追加)
    │
    ▼ Sort(items) [recommend.go / sort.Sort]    多策略重排
    截断 size → items[:size]
    │
    ▼ fire-and-forget: FeatureLog / LogSampleResult / CleanHooks (各起 goroutine)
    │
    ▼ [web/recommend_controller.go:doProcess]
    序列化 ItemData[] → io.WriteString (注意: 响应体只含 item_id/item_type/retrieve_id, 不含 score)
```

**关键细节(源码确认)**:
- 主路径与 pipeline 路径**并行**而非串行(`user_recommend.go:74-83` 的 `go func` + `wg.Wait`)。两路召回解耦:主路径用 `RecallService`(按 category 解析召回名),pipeline 路径按 scene 选 pipeline 配置。
- merge 是 **属性合并**(同 id 的 item 合并 properties,pipeline-only 的追加),非简单拼接(`user_recommend.go:189-204`)。
- **无 PreRecall/PreSort 阶段**:`RecommendService.PreSort`(`recommend.go:44`)存在但全代码无调用。
- 响应体 `ItemData` 只有 `item_id`/`item_type`/`retrieve_id`,**不含 score**——排序算出的 `item.Score` 序列化时丢弃。

### 2.2 数据流(进程内对象 + 后端交互)

```
uid
 │
 ├─► LoadUserFeatures ─► UserFeatureService ─► UserFeature DAO (Redis GET / Hologres SELECT)
 │   (user.Properties 填充, 供后续 stage 复用)
 │
 ├─► [主路径] RecallService.GetItems
 │     scene+category → recallNames (scene.go:11 + category.go:11)
 │     每路 recall.GetCandidateItems:
 │       BeRecall ─► BE engine HTTP Read
 │       VectorRecall ─► Vector DAO (Redis/Hologres) + FAISS web
 │       HologresVectorRecall ─► Hologres SQL (pm_approx_inner_product_distance)
 │       UserCollaborativeFilterRecall ─► Redis MGET / Hologres 4-goroutine fan-out
 │       GraphRecall ─► iGraph HTTP
 │       ...
 │     每路有 per-user 异步缓存写 (go func cache.Put)
 │     → items[] (各路合并)
 │
 ├─► Filter
 │     UniqueFilter (CPU map dedup)
 │     BloomFilter (Redis BITFIELD/BF.MEXISTS, batch=500)
 │     ItemStateFilter (Hologres SQL, batch=500, 4 goroutine, 200ms 超时)
 │     SnakeFilter (CPU O(n log n) 轮询交织)
 │     ConditionFilter (CPU 路由)
 │
 ├─► GeneralRank ─► 粗排 EAS (冷启 vs 正常分流, 并发)
 │
 ├─► FeatureService.LoadFeatures  ← 隐藏 I/O 热点
 │     每 Feature config 一个 DAO, async=true 时全并发
 │     Hologres: chunk=600, ceil(n/600) goroutine, 每个一次 SELECT IN(...)
 │     Redis: chunk=100, ceil(n/100) goroutine, 每个 MGET / pipeline HMGET
 │     in-proc TinyLFU 命中则免后端 RTT
 │     → items[i].Properties 填充
 │
 ├─► RankService.Rank ─► 批 EAS
 │     batchCount=100 → ceil(S/100) 批, 每批含全部候选
 │     每批 × len(RankAlgoList) algo, 全并发 goroutine (两级嵌套)
 │     每批一次 HTTP POST (protobuf, 非逐 item)
 │     AST RankScore 表达式逐 item 求值 → item.Score
 │
 ├─► mergePipelineItems (CPU map 合并)
 │
 ├─► Sort ─► 多策略
 │     ItemScoreSort/AlgoScoreSort (CPU O(n log n))
 │     BoostScoreSort (CPU O(n·conditions))
 │     DPPSort (CPU O(n²·d) kernel + O(n³) matmul; + Hologres embedding I/O)
 │     SSDSort (CPU O(T·n·d))
 │     TrafficControlSort (CPU O(n log n) + PID, 含 12ms 超时赛跑)
 │     DiversityRuleSort (CPU O(K·n·W), 可逼近 O(n²))
 │
 └─► 截断 → 响应
```

### 2.3 时序图(进程内,一次请求)

```
HTTP goroutine ──●──────────────────────────────────────────────────────●── write resp
                  │                                                     │
                  ├─ LoadUserFeatures ──(User DAO RTT)                  │
                  │                                                     │
                  ├─ go pipeline.Recommend ──┐                         │
                  │   (N pipeline goroutine)  │ 各跑 Recall→Filter     │
                  │                           │ →GeneralRank→Feature  │
                  │                           │ →Rank→ColdStart→Sort   │
                  │                           └───── channel 合并 ─────┤ (wg.Wait join)
                  │                                                     │
                  ├─ RecallService.GetItems (主路径)                     │
                  │   ├ go recall₁ ──(后端 RTT, defer recover)──┐      │
                  │   ├ go recall₂ ──(后端 RTT)──────────────────┤     │
                  │   └ go recallₙ ──(后端 RTT)──────────────────┤     │
                  │   ◄──── buffered chan 合并 ─────────────────┘     │
                  ├─ Filter (顺序, Bloom/State 含 RTT)                 │
                  ├─ GeneralRank (EAS RTT)                            │
                  ├─ FeatureService.LoadFeatures (多 DAO 并发 RTT)     │
                  ├─ RankService.Rank (批 EAS 并发 RTT + AST CPU)     │
                  │                                                     │
                  ├─ wg.Wait() ◄── (等 pipeline 路径) ─────────────────┤
                  ├─ mergePipelineItems (CPU)                          │
                  ├─ Sort (CPU, DPP/SSD 可能 O(n²)~O(n³))               │
                  ├─ 截断                                                │
                  └─ go FeatureLog / go LogSample / go CleanHooks (fire-and-forget)
```

**时序的关键特征**:
- 主路径各 stage 大体**串行**(filter→generalRank→feature→rank),但每个 stage 内部**并发扇出**(召回、特征、rank 都并发)。
- pipeline 路径与主路径**并行**,在 `wg.Wait()` 处 join——若 pipeline 慢,整个请求被拖到 pipeline 完成。
- **无请求级超时**(除 HTTP server 30s Read/Write Timeout, `app.go:95-96`);`TryRecommendWithFallback`(`user_recommend.go:206`)有 timer 但 `/api/recommend` 不走它。

---

## 3. 线程模型与并发模型(进程内)

> 本章按"总→分→端到端实战"展开。§3.1 用一次请求的端到端伪代码把 G/goroutine、M/线程、P/调度器、CPU 在每个时刻的状态、阻塞、唤醒、竞争一次讲透(总);§3.2-§3.4 拆解 GMP 模型、goroutine 计数与并发原语(分);§3.5 把前面所有概念放进高并发场景端到端走一遍(实战);§3.6 讲双路径。承接 §2 的时序图——本章回答的是"那些 stage 在 G/M/P/CPU 层面究竟怎么跑"。
>
> **承重精度修正(核源码后)**:pairec 热路径**无 CGO、无文件 I/O**(文件 I/O 仅在尾部 `debug_service.go` 日志,非热路径),所有后端调用(`net/http` 调 EAS、`redigo` 调 Redis、`lib/pq`/`database/sql` 调 Hologres)都是**网络 I/O,走 Go netpoller(epoll)**。网络 I/O 阻塞时 goroutine 解绑 M 挂到 netpoller,**不占用 M、不膨胀线程数**——这区别于"阻塞 syscall 绑 M"的一般印象。所以 pairec 的 M(线程)数其实**不会因网络 I/O 大幅膨胀**,真正膨胀的是"可运行 goroutine 队列"和"待唤醒计数"。

### 3.1 总:一次请求的端到端伪代码与 G/M/P/CPU 状态流转

下面是编排心主路径(`service/user_recommend.go:46` `Recommend`)的简化伪代码,**每步右侧标注该时刻 G0(主 goroutine)、M(线程)、P(调度器)、CPU 的状态,以及阻塞如何被唤醒**。pipeline 路径(§3.6)结构同构,暂按下不表。

```go
// === 入口:net/http 为每个 accept 的连接派生一个 goroutine,即 G0 ===
// t0: HTTP accept → G0 创建, 进入 Process (web/recommend_controller.go:72)
//     状态: G0=Grunnable, 占一个 P, 绑一个 M, M 在 CPU 跑解析请求

func Recommend(ctx *RecommendContext) []*Item {
    // t1: 加载用户特征 (user_recommend.go:64)
    userFeatures := userFeatureService.LoadUserFeatures(user, ctx)
    //     ↑ 内部走 DAO → Redis/Hologres 网络读
    //     状态: G0 调到 net.Conn.Read → 解绑 M, 挂到 netpoller 等数据 (Gwaiting)
    //           M 释放回 runtime 线程池(不膨胀), P 让给别的可运行 G
    //     唤醒: 后端响应到达 → epoll 就绪 → netpoller 把 G0 标记 Grunnable
    //           → 进入 P 的运行队列 → 被 P-M-CPU 重新调度执行 (异步, 非阻塞 M)
    //     竞争: G0 醒后要重新抢 P 才能跑; 若 P 队列前有大 CPU 任务(如别人的 DPP), G0 排队

    // t2: 派生 pipeline 路径 (user_recommend.go:75)
    wg.Add(1)
    go func() { pipelineItems = pipeline.Recommend(...) }()  // G-pipe 派生
    //     状态: G-pipe=Grunnable 入队; G0 不阻塞, 继续往下 (派生本身不 yield)

    // t3: 主路径召回 (user_recommend.go:83 → recall.go:54)
    items := recallService.GetItems(user, ctx)
    //     ↓ GetItems 内部 fan-out (recall.go:132-171), 分三层看时机:
    //
    //     [代码层]                  [goroutine 层]              [G0 在哪/状态]
    //     ch := make(chan, R)       —                           G0 在跑(占 P), 建完 channel 继续
    //     for j:=0;j<R;j++ {       —                           G0 仍在跑, 逐个派生
    //       go func() { ──────────▶ G-recall[j] 诞生(Grunnable) G0 派生完不 yield, 继续 for 循环
    //         recall.GetItems()──▶   └─ 调后端 → net.Conn.Read
    //                               └─ G 挂 netpoller(Gwaiting)   G0 此时已离开派生循环
    //         ch <- items ◀─────────   后端响应到→G 醒→投 ch       G0 已在等(见下行)
    //       }()
    //     }
    //     for j:=0;j<R;j++ {      —                           ★G0 到此★: 跑到 <-ch
    //       items = <-ch  ◀────── 收 G-recall 投的数据          G0 Gwaiting 等 channel
    //     }                                                   循环收 R 次, 每次被投数据唤醒
    //
    //     关键时机:
    //     - G-recall 派生点在 recall.go:138 (go func), 发生在 G0 跑第一个 for 循环时
    //     - G-recall 阻塞点在 recall.go:156 (GetCandidateItems 内 net.Conn.Read), 挂 netpoller
    //     - G0 阻塞点在 recall.go:169 (<-ch), 挂 channel 等待队列
    //     - G-recall 唤醒 G0: recall.go:165 的 ch<-items, runtime 把等在 <-ch 的 G0 标 Grunnable
    //     - G0 收 R 次后 recall.go:172 close(ch), 返回 items, G0 继续 t4
    //     状态: R 个 G-recall 各自挂 netpoller 等后端 (Gwaiting, 不占 M)
    //           G0 在 <-ch 处 Gwaiting 等 channel (不占 M)
    //           P 此时调度 G-pipe / 别的请求的 G (不闲)
    //     竞争: R 个 G-recall + G0 + G-pipe + 别请求 G 争抢 P; GOMAXPROCS 是闸口
    //     失败语义: 某路 panic→defer recover(recall.go:141)→空切片→ch<-[]; 不影响其他路, 不报错

    // t4: 过滤 (user_recommend.go:105) — CPU 为主, 部分过滤查 Redis
    items = Filter(user, items, ctx)   // UniqueFilter(CPU) / BloomFilter(Redis 网络) / ...
    //     状态: CPU 段 G0 占 P-M-CPU 跑; 偶发 Redis 读则同 t1 挂 netpoller

    // t5: 粗排 (user_recommend.go:116)
    items = generalRankService.Rank(user, items, ctx)  // 轻模型 EAS, 网络

    // t6: 特征装配 (user_recommend.go:129 → feature_service.go:77)
    items = featureService.LoadFeatures(user, items, ctx)
    //     ↑ 若 FeatureSceneAsyncMap[scene]=true: 派生 F 个 G-feat + F×cpuCount 个 DAO worker
    //     feature_service.go:88 async := ...; 90-106 for each Feature: wg.Add(1); go fea.LoadFeatures
    //     DAO 内部: cpuCount 个 worker 抢工作队列 chan (feature_hologres_dao.go:513)
    //     状态: G0 在 wg.Wait() 等 (Gwaiting); F×cpuCount 个 G 挂 netpoller 等 Hologres
    //     唤醒: 各 DAO G 后端响应→醒→写结果→wg-- ; wg 归零时 runtime 唤醒 G0
    //     竞争: 此处并发后端调用最多 (F×cpuCount), 是进程内"可运行 G 密度"高峰

    // t7: 精排 (user_recommend.go:137 → rank_service.go:102) 两级嵌套
    rankService.Rank(user, items, ctx)
    //     ↓ 内部:
    //     requestCh := make(chan, B)   // B=批数=ceil(S/100)  rank_service.go:254
    //     responseCh := make(chan, B)                          // :255
    //     for b := 0; b < B; b++ { requestCh <- batch[b] }    // :260 投递工作
    //     for b := 0; b < B; b++ {                             // :264 起 B 个外层 worker
    //         go func() {
    //             algoData := <-requestCh                      // 抢工作(竞争点)
    //             for _, algo := range algoList {             // :269
    //                 wg.Add(1)
    //                 go func(algo) {                          // :271 内层每 algo 一个 G
    //                     ret := algorithm.Run(algo, ...)      // EAS HTTP 网络
    //                     algoData.SetAlgoResult(ret)
    //                 }()
    //             }
    //             wg.Wait()                                    // :286 内层合并
    //             responseCh <- algoData                       // :287
    //         }()
    //     }
    //     for b := 0; b < B; b++ { algoData := <-responseCh ... }  // :309 外层合并
    //     状态: 外层 B + 内层 B×A 个 G 挂 netpoller 等 EAS; G0 在 <-responseCh 等
    //     唤醒: EAS 响应→内层 G 醒→wg--→外层 G 把结果投 responseCh→G0 醒收
    //     竞争点: requestCh 是工作队列, B 个外层 G 抢 B 个 batch (此处恰 1:1, 但若 B>worker 则排队);
    //             AlgorithmFactory.mutex RLock 是每 algo 调用前的全局热锁 (algorithm.go:37)
    //     CPU 段: 全部 algo 回来后, AST 求值逐 item 算 RankScore (rank_service.go AST 求值)

    // t8: 汇合 pipeline 路径 (user_recommend.go:143)
    wg.Wait()   // 等 G-pipe
    //     状态: 若 G-pipe 未回, G0 在此 Gwaiting; P 让给别人
    //     唤醒: G-pipe 跑完它的整条链→wg--→归零唤醒 G0
    //     长尾点: 无超时, 慢 pipeline 拖整个请求 (§3.6.5)

    // t9: 合并 + 重排 (user_recommend.go:144,151)
    items = mergePipelineItems(items, pipelineItems)  // 按 id 并集+属性叠加
    items = Sort(user, items, ctx)                    // DPP O(n²~n³) / ItemScore / Diversity
    //     状态: 纯 CPU 段, G0 占 P-M-CPU 满载; 若 DPP n>200 此段是 CPU 长杆
    //     竞争: G0 独占一个 P; 其余 N×(数十) 个 G 排队等 P → 调度延迟累积

    // t10: 截断 + 响应 + fire-and-forget (user_recommend.go:168-174)
    items = items[:size]
    go feature_log.FeatureLog(...)        // 派生, 不等
    go LogSampleResult(...)               // 派生, 不等
    for _, hf := range CleanHooks { go hf(...) }  // 派生, 不等
    return items                          // G0 返回, net/http 回写响应后 G0 结束
    //     尾部 G: 日志写文件 (debug_service.go) — 这是热路径里唯一"绑 M 的阻塞 I/O", 但在响应后
}
```

**总览结论(回答"主 goroutine 阻塞吗?线程呢?CPU 呢?")**:
- **G0(主 goroutine)**:整条链反复在"网络 I/O 等待 / channel 等待 / CPU 计算"间切换,阻塞点在 t1/t3/t6/t7/t8。阻塞时 G0 解绑 M,挂到 netpoller 或 channel 等待队列——**不占线程、不占 CPU**。
- **M(线程)数**:网络 I/O 不膨胀 M(netpoller 接管);仅尾部文件日志短暂绑 M。**进程内 M 数 ≈ GOMAXPROCS + 少量空闲复用,不会随 QPS 线性膨胀**——这是与"每连接一线程"模型的关键区别,也是 pairec 能扛高并发的根因。
- **CPU 利用率**:由"可运行 G 数 vs GOMAXPROCS"决定。I/O 密集期可运行 G 少(都在 netpoller 挂着),CPU 可能**空闲**(请求被 I/O 卡);DPP/AST 期 G0 占 P 满载。
- **唤醒与竞争**:G 的唤醒来自两条异步通道——① netpoller(后端响应到达,epoll 就绪);② channel/wg(其他 G 投数据或 wg 归零)。唤醒后的 G 进 P 运行队列,与所有其他可运行 G **争抢 P 槽**(数量=GOMAXPROCS)。竞争失败的 G 排队,等待时间累积成 p99。

### 3.2 分:GMP 模型与三类阻塞/唤醒

**goroutine 是 Go runtime 管理的用户态轻量执行单元**(栈初始 2KB、可增长,创建约几百纳秒),与 OS 线程、CPU 由 GMP 模型桥接:

```
  G (goroutine) ──┐                    ┌── CPU 物理核
                  │ 被 P 调度           │ (执行指令的唯一实体)
  G ──────────────┤                    │
  G ... (无上限)  ├─ P (调度器) ───────┤── M (OS 线程) ─绑定─┘
                  │  容量=GOMAXPROCS   │  (OS 调度的实体, 复用)
  全局运行队列 ◄───┘                    │
                  └── netpoller (epoll): 网络就绪的 G 在此等, 不占 M
                     channel 等待队列: 等 <-ch / wg 的 G 在此, 不占 M
```

- **G : P : M : CPU核 = N : K : K : 核数**(K=GOMAXPROCS≤核数)。G 数无上限,M 数 ≈ K + 复用。
- `GOMAXPROCS` 由 `automaxprocs`(`pairec.go:44`)按 cgroup 配额校准。假设容器 4 核 → P=4,**同时占 CPU 的 G 最多 4 个**。

**三类阻塞及其唤醒/竞争(修正版)**:

| 阻塞类型 | G 去哪 | M(线程) | P | CPU | 唤醒的底层机制 | 竞争逻辑 |
|---|---|---|---|---|---|---|
| **网络 I/O**(Redis/Hologres/EAS,热路径全部) | 解绑 M,挂 netpoller | **释放,不膨胀** | 让给别的 G | 跑别的 G 或空闲 | **epoll**:内核网卡收到包→硬中断→软中断→epoll_wait 就绪→netpoller 把 G 标 Grunnable | 醒后入 P 队列争 P 槽 |
| **channel/wg 等待**(`<-ch`/`wg.Wait`) | 挂 channel 等待队列 | 不占 | 让给子 G | 跑子 G 或空闲 | **futex + atomic**:生产者原子改 channel 状态,若发现有人在等,通过 futex wake 唤醒;wg 是 atomic 计数,futex wake | 同上,与所有可运行 G 争 P |
| **CPU 计算**(DPP/AST/Sort) | Grunning 占 P-M | 占住跑 | **被占** | 满载 | **无需唤醒**:G 主动在 CPU 上跑,时间片到被 runtime 抢占(preemption)回 Grunnable | G0 独占 P,其余 G 排队(长杆阻塞他人) |
| 文件 I/O(尾部日志,非热路径) | 绑 M 阻塞 | 占住陷内核 | 让出 | 跑别的 | 阻塞 read/write,内核 I/O 完成后唤醒 | 仅尾部,影响小 |

**唤醒/等待的底层机制详解(结合 pairec 实际使用)**:

Go runtime 把"等待/唤醒"分成两条完全不同的路径,pairec 都用到了。理解这两条路径是理解 pairec 并发模型的关键:

**(1) 网络等待 → netpoller(epoll + 硬中断)**

pairec 热路径的召回(`recall.go:156` GetCandidateItems 调 Redis/Hologres)、精排(`rank_service.go:273` algorithm.Run 调 EAS)、特征(`feature_hologres_dao.go` 调 Hologres)全是网络 I/O。Go 把它们统一交给 **netpoller**,底层是 Linux `epoll`:

```
goroutine 调 redisConn.Get(key)           // redigo 内部最终调 net.Conn.Read
  → Go runtime 把 fd 注册到 epoll (epoll_ctl ADD)
  → goroutine 解绑 M,状态变 Gwaiting,挂到 netpoller 的等待结构
  → M 释放回线程池(P 也释放给别的 G)         ← 不占线程,这是关键

  ...后端处理...

  → 网卡收到响应包 → 触发硬中断(IRQ)
  → 内核软中断(softirq)处理协议栈 → 把数据放进 fd 的 socket buffer
  → epoll_wait 返回该 fd 就绪
  → Go runtime 的 netpoller 轮询(或被 sysmon 唤醒)发现就绪 fd
  → 把对应的 G 标记 Grunnable,放进 P 的运行队列
  → 该 G 重新争 P-M-CPU 执行,拿到数据继续跑
```

关键点:**这条路径的"等待方"完全不占 M**。epoll 一个线程管所有 fd(Go runtime 内部只用少量 M 轮询 epoll),所以 pairec 即使有 4 万个 G 同时在等网络响应,M 数也不会涨——这是与"每连接一个阻塞线程"模型的本质区别。**唤醒的成本**:一次硬中断 + 软中断 + epoll_wait 返回 + runtime 调度,是异步的、批量的(epoll 一次可返回多个就绪 fd,所以"一波响应同时到"只触发一次 epoll_wait)。

**(2) channel/wg 等待 → futex + atomic(纯用户态,不走 epoll)**

pairec 的 fan-in 合并(召回 `recall.go:169` `<-ch`、pipeline `pipeline.go:62` `<-ch`、精排 `rank_service.go:309` `<-responseCh`、`wg.Wait()` 多处)都是 channel/sync.WaitGroup 等待。它们的底层是 **futex**(Linux fast user-space mutex)+ atomic,与网络 I/O 的 epoll 路径完全不同:

```
// channel 等待(recall.go:169 G0 在 <-ch 等)
G0 调 <-ch:
  → Go runtime 检查 channel 的环形缓冲,空 → 无数据可拿
  → 把 G0 挂到 channel 的等待队列(一个 sudog 结构),状态 Gwaiting
  → G0 解绑 M,P 让给别人                ← 同样不占 M
  → runtime 调度别的 G 跑

// channel 唤醒(recall.go:165 G-recall 投 ch <- items)
G-recall 调 ch <- items:
  → 把数据放进 channel 环形缓冲
  → 原子操作发现有人在等(等待队列非空)
  → runtime 把等待的 G0 弹出队列,标记 Grunnable,放进 P 运行队列
  → 若目标 G 在别的 M/P 上,可能触发 futex wake 或向那个 P 发检查信号
     (runtime 用 IPI/信号让别的 P 尽快调度它,但这不是每用户调用必发)
  → G0 醒后从 <-ch 返回,拿到 items 继续
```

关键点:channel 等待**也不占 M**(G 挂起解绑 M)。唤醒靠 runtime 内部的等待队列 + atomic + 必要时 futex,不走 epoll(因为这不是网络事件,是内存中的数据交接)。`sync.WaitGroup` 同理:wg.Wait 检查 atomic 计数,非零则挂起;wg.Done 原子减一,减到零时 runtime 唤醒所有 Wait 的 G。

> **为什么不提 eventfd**:Go runtime 在 Linux 上**用 epoll 监听网络 fd**,但 channel/wg 的等待**不通过 eventfd**,而是 runtime 自己管理的用户态等待队列 + futex。eventfd 主要用于"唤醒 runtime 的 netpoll 轮询线程"这类场景(pairec 不直接接触)。所以 pairec 的等待唤醒实际是:**网络等 epoll+中断、内存交接等 futex+atomic、CPU 计算无需唤醒**——没有第三种。

**(3) CPU 计算 → 无等待,靠抢占(preemption)**

DPP/AST/Sort 这种纯 CPU 段,G 一直在 Grunning 跑,没有"等待"也没有"唤醒"。Go 1.14+ 的**异步抢占**保证:即使 G0 在跑长 CPU 循环(如 DPP 的 O(n³) matmul),runtime 的 sysmon 线程会发现它跑太久,通过发信号(基于信号的抢占)把它打断回 Grunnable,避免独占 P。所以 pairec 的 DPP 长杆**不会饿死整个 P**,但会让其他 G 等一个抢占周期。

> **核心修正**:网络 I/O 经 netpoller,G 阻塞时解绑 M,不占线程。**pairec 的 M 数稳定在 GOMAXPROCS 量级**,高并发真正的代价是"可运行 G 堆积→争 P→调度延迟",而非线程爆炸。这对 OS 研究很重要:不能把 pairec 当成"每连接一线程"模型来测线程开销。

**对 OS 研究的承重启示**(把上面三类映射到 OS 观测点):

| pairec 现象 | OS 层实际在发生什么 | OS 该观测什么 |
|---|---|---|
| 4 万 G 等 I/O | 4 万 fd 在 epoll 树上,M 不涨 | epoll fd 数、`/proc/<pid>/netstat` 软中断计数,**不是**线程数 |
| 一波响应到 → p99 抖 | 批量 epoll_wait 返回 → 大量 G 同时 Grunnable → 争 P | P 运行队列长度、调度延迟(`/proc/schedstat`、`runtime/trace`) |
| DPP 长杆 | 1 个 G 跑 O(n³),被 sysmon 异步抢占 | CPU 核占用、抢占信号数(`signal 信号`) |

### 3.3 分:N 请求的 goroutine 计数与调度竞争

设 N 并发请求,每请求 R 路召回、P 个 pipeline、S 候选、batch=100、A 个 algo、F 个 Feature、cpuCount 个 DAO worker。**单请求派生数**:

```
单请求 ≈ 1(G0) + 1+P(pipeline入口+扇出) + R(主召回) + R×P(pipeline召回)
       + F(特征 async) + F×cpuCount(DAO worker) + ceil(S/100)×A(rank 内层)
       + 同上(pipeline rank) + C(customRanks) + 3(尾部 fire-and-forget)
```
- 量级(R=5,P=1,S=200,A=3,F=4,cpuCount=4):召回10 + rank 6+6 + 特征4+16 + 入口尾部5 ≈ **40~50/请求**
- N=1000 QPS → 进程内瞬时 G ≈ 4~5 万。**但占 CPU 的 ≤ GOMAXPROCS(4)**,其余在 netpoller 挂着或排队。
- **调度竞争公式**:可运行 G 数 = (已就绪等跑的 G);它们争 GOMAXPROCS 个 P 槽。当 I/O 批量返回(如一波 EAS 响应同时到),瞬间大量 G 转 Grunnable,**P 队列爆发式排队**——这是 p99 尾部的主因,而非线程数。

### 3.4 分:并发原语画像

**同步原语争用**:

| 锁/原语 | 位置 | 守护 | 争用 |
|---|---|---|---|
| `AlgorithmFactory.mutex` RWMutex | `algorithm/algorithm.go:37` | 全局 algo 注册表 | **热路径唯一进程级热锁**(每 EAS algo 调用 RLock),稳态低、热加载尖峰 |
| per-recall `cloneMu` | `recall.go:124` | 每 recall 的 AB-clone 缓存 | per-instance 分片 |
| per-DAO `itemStmtMap` RWMutex | `feature_hologres_dao.go:37` | 预编译语句缓存 | warmup 后低 |
| `RecommendContext.mu` | `context/recommend_context.go:27` | per-request map | per-request 无跨请求争用 |
| `module/item.go` RWMutex | `item.go:23` | item 属性/algoscore | 中等(rank/sort 并发改 item) |
| PID controller `sync.Map` | `sort/pid_controller.go:32` | PID 状态 | 低 |

**channel 模式**:全部 buffered(容量=生产者数,发送方不阻塞),fan-in 用固定计数循环 `for i<N { <-ch }`,无 select 合并(除 `user_recommend.go:217` fallback 一处)。`rank_service.go:254,255` 是工作队列+结果队列双 channel。失败语义:panic 被 `defer recover` 兜成空切片(`recall.go:141`),不跨 goroutine 传播错误。

**三个对 OS 研究重要的缺位**:
1. **无 singleflight**(全树无 import):并发同 key cache miss 不合并→热点 key 击穿直打后端。
2. **无请求级 context 传播**:`RecommendContext` 不 wrap `r.Context()`,客户端断连/超时不取消在途召回/DAO/EAS;DAO 超时硬编码 100-200ms 根植 `context.Background()`;超时=部分结果不报错。
3. **无全局 worker pool**:并发按请求即时派生,无池化复用,高 QPS 累积创建开销。

### 3.5 实战:高并发(QPS=1000)下一请求的端到端处理流程

把 §3.1-§3.4 的概念放进一个具体场景:容器 4 核(P=4),1000 QPS 持续注入,每请求 §3.3 的 40~50 个 G。跟踪**其中一个请求 G0**从到达到响应,看分发/并发/阻塞/唤醒/竞争:

```
[t0 到达] 1000 个 G0/秒涌入 net/http accept。每个 G0 进入 Process。
           进程内已有 ~4万 G:其中绝大多数挂在 netpoller 等 I/O,
           可运行(争 P)的瞬时可能只有几十~几百个。
           → G0a(我们跟踪的这个)进 P 运行队列,与其他可运行 G 争 4 个 P 槽。

[t1 用户特征] G0a → LoadUserFeatures → DAO Redis 读。
           G0a 解绑 M 挂 netpoller (Gwaiting)。M 释放给别的 G。
           → 此时 G0a 不占 CPU。若 P 队列前有别人的 CPU 任务,别的 G 在 CPU 上跑;
           若都在 I/O,4 个 P 可能部分空闲 (CPU 利用率下降 — I/O bound 期)。
           唤醒: Redis 响应到 → netpoller 标 G0a runnable → 入队争 P。

[t3 召回]   G0a 派生 5 个 G-recall (R=5),各自挂 netpoller 等 5 路后端。
           G0a 在 <-ch 等 (Gwaiting)。
           → 此刻这 6 个 G 都不占 CPU。进程内同时有 ~1000 个 G0 × 类似状态。
           唤醒竞争: 5 路 Redis/Hologres 响应陆续到, 每到一个醒一个 G-recall,
           ch<-items 后结束。G0a 被 channel 唤醒, 重新争 P。
           关键: 若 5 路后端都慢, G0a 长挂; 若某路 panic→recover→空切片, 不报错, 不影响其他。

[t6 特征]  G0a 派生 4 个 G-feat, 每个 G-feat 再 fan-out 4 个 DAO worker (cpuCount=4),
           共 16 个 G 挂 netpoller 等 Hologres。G0a 在 wg.Wait 等。
           → 这是单请求内"并发后端调用密度"高峰: 16 个并发 SQL。
           进程级: 1000 请求 × 16 = 1.6万 并发 SQL 在途 → Hologres 连接池(maxOpen=100)
           成天花板, 后到的 SQL 在池里排队 (不在 netpoller, 在 db.Wait 排队) → 链式延迟。
           唤醒: SQL 返回 → DAO worker 醒 → 写结果 → G-feat wg-- → G0a 醒。

[t7 精排]  G0a 派生 外层2×内层3=6 个 rank G + customRanks, 各挂 netpoller 等 EAS。
           → EAS MaxIdleConnsPerHost=2000, 1000 请求 × 6 = 6000 并发 EAS 在途,
           超 2000 的部分排队。AlgorithmFactory.mutex RLock 每次 algo 调用抢一次
           (进程级唯一热锁, 6000 并发轮流 RLock, 稳态争用低但热加载写锁时全堵)。
           唤醒: EAS 响应到 → 内层 G 醒 → wg-- → 外层 G 投 responseCh → G0a 醒。
           CPU 段: 全部回来后 AST 求值逐 item 算分 (CPU, G0a 占 P)。

[t8 汇合]  G0a wg.Wait 等 G-pipe。若 G-pipe 跑得慢 (它也要走召回→…→精排),
           G0a 在此长挂 — 无超时, 长尾放大点 (§3.6.5)。

[t9 重排]  G0a 跑 Sort (DPP)。纯 CPU, G0a 独占一个 P。
           → 若 DPP n>200, 此段 CPU 长杆: G0a 占 P 跑几十 ms,
           期间其他 3 个 P 服务别的请求, 但第 4 个 P 槽被 G0a 长占,
           排队的可运行 G 多等一个 P 槽 → 调度延迟累积。
           (注: DPP 用 gonum dense, 无 LockOSThread 绑核, 无 cache 局部性)

[t10 响应] G0a 截断 + 派生 fire-and-forget(FeatureLog 等写文件, 绑 M 但响应已发)
           + return。net/http 回写响应, G0a 结束。
```

**实战结论(端到端看到的进程内图景)**:
- **G 总数 4~5 万,M(线程)数稳定 ~4**:不会线程爆炸,因为网络 I/O 全走 netpoller。OS 若按"线程数"观测 pairec 会得出"压力不大"的错误结论——真实压力在 **P 调度队列长度**和 **netpoller 待唤醒 G 数**。
- **三处 P 竞争爆发点**:① 一波 EAS/Redis 响应同时到,瞬间大量 G 转 runnable;② 特征 DAO 16 路并发同时 SQL 返回;③ DPP 长杆独占 P。这三点决定 p99。
- **两处连接池天花板链式延迟**:Hologres maxOpen=100、EAS MaxIdleConnsPerHost=2000——超限的在 db/EAS 客户端内部排队(非 netpoller,非 P 队列),是另一种阻塞,DAO 100ms 硬超时兜底→部分结果。
- **无取消传播的代价**:1000 请求里若客户端断连,G0a 在途的召回/SQL/EAS **不取消**,白跑完才丢弃——QPS 高时这是可观无效负载,是 OS 层"取消传播"优化的输入(§7.2)。

---

## 3.6 主路径 vs pipeline 路径:配置维度、编码差异与协作关系

编排心 `UserRecommendService.Recommend`(`service/user_recommend.go:46`)在 `:75` 派生一个 goroutine 跑 pipeline 路径,自身继续跑主路径,`:143` `wg.Wait` 汇合后 `:144` 合并。这一节把两条路径**到底差在哪、为什么需要 pipeline**讲透——这是后续做扩展层(生成式召回/精排)时的架构判断基座。
### 3.6.1 框定修正:不是"保底 vs 可配置",而是"并列完整管线"

> **承重更正**:不要用"保底召回 vs 可配置多策略召回"框定两条路径。这个说法自相矛盾(既说两者都是完整管线,又说一个是另一个的 fallback),且与源码不符。正确框定见下。

两条路径都是完整的推荐管线(召回→过滤→粗排→特征→精排→重排),**主路径必跑、pipeline 路径可选**(scene 在 `PipelineConfs` 未配时 `pipeline.Recommend` 直接返回 nil,整条路径不存在,`pipeline.go:48`)。配了的话两者**并列并行**,不是保底关系——主路径召回空了不会自动切到 pipeline 顶上,主路径空列表照样走完后续 stage。注册层(全局工厂 `map[string]Recall` / `map[string]ISort` / filter)是两者的**共同底座**,不存在"主路径一套注册、pipeline 另一套"。

真正的 fallback 语义在另一个函数 `TryRecommendWithFallback`(`user_recommend.go:206`,基于 `fallback.DefaultFallbackService`),**`/api/recommend` 不走它**——两条路径的并行合并与 fallback 是两套独立机制,不要混为一谈。

### 3.6.2 决定性差异:配置维度(主路径 scene 单槽 vs pipeline 多编排链)

这是两条路径存在的**真正理由**,也是"为何需要 pipeline"的答案所在。差异不在编码,在**配置绑定维度**:

```
主路径:所有 stage 配置按 scene 一个维度解析(运行时动态取)
  召回名:  context.ExperimentResult.Get(categoryName+".RecallNames")        recall.go:65
           或 scene.GetCategory(categoryName).GetRecallNames()              recall.go:90
  RankConf: recconf.Config.RankConf[scene]                                  rank_service.go:154
  过滤器:   fs.Filters[scene]                                               filter.go:97
  排序器:   ss.SortStrategies[scene]                                       sort.go:99
  ↑ 一个 scene 一个槽 —— 召回名单/精排配置/过滤/排序各只能有一套

pipeline 路径:配置构建时按 PipelineConfig 绑定(静态,自带完整配置块)
  PipelineConfig{Name, RecallNames, FilterNames, RankConf, SortNames, ...}  recconf.go:1100
  ↑ PipelineConfs[scene] 是 []PipelineConfig(切片,非单值)
  ↑ 一个 scene 可挂 N 个 PipelineConfig → N 套独立编排并行跑
```

**关键推论**:主路径的 scene 是**单配置槽**——一个 scene 只能配一套召回、一套精排、一套排序。若你想在同一次请求里**并行跑多套不同的召回名单 + 不同精排模型**,主路径结构上塞不下(`RankConf[scene]` 一个 scene 只有一个槽)。pipeline 路径就是开给这个的口子:`PipelineConfs[scene]` 是切片,挂 N 个 pipeline,每个自带独立 `RecallNames` 和 `RankConf`,N 条链并行、各自产 items、最后 `mergePipelineItems` 合并进重排候选池。

**承重事实(PipelineName 是死字段)**:`FilterData.PipelineName`(`filter.go:30`)和 `SortData.PipelineName`(`sort.go:31`)看似让 pipeline 路径能按 pipeline 名配不同过滤/排序,**但配置查找时根本不用它**——`FilterService.Filter` 仍按 `scene` 取(`filter.go:97` `fs.Filters[scene]`),`SortService.Sort` 仍按 `scene` 取(`sort.go:99` `ss.SortStrategies[scene]`)。PipelineName **只用于 `log.Info` 区分日志**(`sort.go:212-213` 等日志行)。也就是说,**pipeline 路径真正能 per-pipeline 独立配置的只有 RecallNames 和 RankConf;过滤和排序退回了 scene 级共享**。这修正了"两条路径各自有完整独立配置"的常见误解。

### 3.6.3 编码层差异:同构 + 3 点实质差异

逐行比对两条路径的 stage 链,结论是**编码层面几乎同构**,实质差异只有 3 点:

| 阶段 | 主路径 | pipeline 路径 | 差异性质 |
|---|---|---|---|
| 召回 | `recallService.GetItems` | `recallService.GetItems` | 仅 RecallNames 解析 key 不同(scene+category vs `pipelines.<name>`) |
| 过滤 | `filterData` 无 PipelineName | `filterData` 带 PipelineName | **仅日志标签**(配置查找不用,见 §3.6.2) |
| 粗排 | `generalRankService.Rank` | `generalRankService.Rank` | 同构 |
| 特征 | `featureService.LoadFeatures` | `featureService.LoadFeatures` | 同构 |
| 精排 | `rankService.Rank` | `rankService.Rank` | 同构,但取的 RankConf 来源不同(scene-keyed vs PipelineConfig 内嵌) |
| 冷启 | **无** | `coldStartRankService.Rank` | pipeline 多这一步(`pipeline/user_recommend.go:71`) |
| 重排 | `sortData` 无 PipelineName | `sortData` 带 PipelineName | **仅日志标签**(配置查找不用) |

**3 点实质差异**:
1. **配置绑定时机**:主路径运行时按 scene 动态取,pipeline 构建时按 PipelineConfig 静态绑定——这是唯一根本差异,也是 pipeline 存在的理由。
2. **ColdStartRank**:pipeline 路径精排后多一步 `coldStartRankService.Rank`,主路径没有。
3. **空召回短路**:pipeline 路径召回 `len==0` 直接 return(`pipeline/user_recommend.go:47-50`),主路径空召回继续跑后续 stage。

补充差异(非核心,但承重):

| # | 差异点 | 主路径 | pipeline 路径 | 锚点 |
|---|---|---|---|---|
| 4 | **LoadUserFeatures** | 有(编排心 `:64` 调用) | 无,复用主路径 user | `user_recommend.go:64` |
| 5 | **debug 日志** | 全 stage 写(含 rank/sort/recommend) | 仅写 recall/filter/general | `user_recommend.go:146,156,171` |
| 6 | **并发承载** | stage 内扇出 | N pipeline × stage 内扇出(乘数效应) | `pipeline.go:56` |

### 3.6.4 多路召回 vs 多套编排:回答"为何需要 pipeline"

这两个是**不同维度**的问题,混淆它们就会得出"pipeline 没用"的错误结论:

- **多路召回** = 一套编排内,召回阶段并行多路。**主路径原生支持**:`RecallNames` 是 `[]string`,挂几路召回就配几个名字,`recall.go:132` `make(chan, len(recalls))` 每路一个 goroutine 并发。生成式召回只要 `RegisterRecallWithConfig` 注册成某个名字塞进 `RecallNames` 即可,**不需要 pipeline**。pairec4tigerllm 的 `GenerativeRecall`/`MilvusRecall`/`QuotaMultiRecall` 就是这么挂进主路径的。
- **多套编排** = 一次请求里**并行跑多条完整推荐链**,各自有独立召回名单/独立精排/独立重排,最后合并。**主路径做不了**:一个 scene 只有一个 `RankConf` 槽、一个 `SortStrategies` 槽。要在同一次请求并行跑两套不同精排模型,必须靠 `PipelineConfs[scene]` 挂多个 pipeline。

**一句话**:多路召回是"召回阶段内并行多路",pipeline 是"编排级别并行多链"。前者主路径够,后者必须 pipeline。

### 3.6.5 执行流与唯一合并点

**并行执行**,唯一合并点在**精排之后、重排之前**:

```
G0 (主请求 goroutine)
├── :64  LoadUserFeatures (阻塞 G0,用户特征加载)
├── :75  go func() { pipelineItems = pipeline.Recommend(...) }  ← 派生 G-pipe
├── :83  recallService.GetItems   (主路径召回, G0 同步, 内部 M goroutine 扇出)
├── :105 Filter                    (G0)
├── :116 generalRankService.Rank   (G0)
├── :129 featureService.LoadFeatures(G0, 内部可能 async 扇出)
├── :137 rankService.Rank          (G0, 内部两级 goroutine 嵌套)
├── :143 wg.Wait()                 ← 唯一 join 点:等 G-pipe 完成
├── :144 mergePipelineItems(items, pipelineItems)  ← 唯一合并点
├── :151 Sort                      (G0, 合并后统一重排)
└── :168 items[:size] 截断 → 响应

  G-pipe 内部(pipeline.Recommend, pipeline.go:45):
  ├── scene → userRecommendSceneMap[scene] → N 个 pipeline service
  ├── N 个 goroutine 各跑完整 stage 链(含 ColdStartRank)
  └── 固定计数循环合并 N 路结果 → pipelineItems
```

**关键时序事实**:
- 主路径 5 个 stage(召回→过滤→粗排→特征→精排)与 pipeline 路径**物理时间重叠**——主路径跑召回的同一时刻,pipeline 路径也在跑自己的召回→…→精排。
- 合并语义(`mergePipelineItems`, `:189-204`):**按 item id 的并集 + 属性叠加**。同 id item 把 pipeline 路径算出的 properties 叠加到主路径 item;不同 id item 直接追加。**不是择优、不是去重**,所以两条路径 score 不需要可比——最终顺序由合并后唯一的 Sort 决定。
- **无超时 join**:主路径精排早跑完,只要 pipeline 路径没回来,`wg.Wait()` 阻塞 G0,Sort 推迟。**慢路径决定请求延迟**,长尾在此放大——这是双路径架构的固有代价。

### 3.6.6 决策表:做生成式召回 + 生成式精排,要不要用 pipeline

| 诉求 | 仅主路径 | 用 pipeline |
|---|---|---|
| 多路召回(含生成式召回) | ✅ `RecallNames` 列表 + `RegisterRecallWithConfig` | 杀鸡用牛刀 |
| 生成式精排 | ✅ 注册成 rank,配进 `RankConf[scene]` | 同上,不划算 |
| 同一 scene 同一次请求并行跑**多套不同精排模型** | ❌ 一个 scene 一个 RankConf 槽 | ✅ 每个 pipeline 独立 RankConf |
| 召回结果与主路径**并行叠加**(不替换上线) | ❌ 主路径单链 | ✅ `wg.Wait`+`mergePipelineItems` 并集合并 |
| 实验性召回链补冷启动 | ❌ 主路径无 ColdStartRank | ✅ pipeline 自带 `coldStartRankService` |

**对扩展层(生成式召回/精排)的架构判断**:如果"生成式召回"只是多一路召回源、"生成式精排"只是换/加一个精排模型,**主路径一条链足够,不要引 pipeline**——pipeline 的双路径、`wg.Wait` 长尾放大、合并语义、ColdStartRank 分支是不划算的复杂度。**只有当需要在同一次请求并行跑多套编排不同(召回名单不同、精排模型不同)的推荐链且要并集合并时**,才动用 pipeline。这是 pairec 上游设计 pipeline 的本意。

### 3.6.7 协作关系图

```
         ┌─── 主路径 (sync, G0) ──────────────────────┐
         │   recall→filter→generalRank→feature→rank   │
         │   各 stage 内并发扇出                        │
         │   配置按 scene 单槽解析                      │
         └──────────────────┬─────────────────────────┘
                            │
    wg.Wait() ◄──── 两者在此 join (user_recommend.go:143, 无超时)
                            │
         ┌─── pipeline 路径 (go, G-pipe) ─────────────┐
         │   N 个 pipeline 并发, 每个跑完整 stage 链   │
         │   含 ColdStartRank                          │
         │   RecallNames/RankConf 按 pipeline 名独立  │
         │   Filter/Sort 退回 scene 级(PipelineName   │
         │   仅日志标签)                              │
         └──────────────────┬─────────────────────────┘
                            │
                   mergePipelineItems (user_recommend.go:189)
                   ├─ 同 id item: 合并 Properties(属性叠加)
                   └─ pipeline-only item: 追加(并集)
                   (不是择优/去重, score 不需可比)
                            │
                   Sort (全局, 唯一) → 截断 → 响应
```

> **潜在 race(源码未显式同步)**:pipeline goroutine 在 `:76` 启动,而 `LoadUserFeatures` 在 `:64` 调用——两者在主路径 G0 上顺序,但 `LoadUserFeatures` 内部若 async 加载用户特征(`user_feature_service.go` 有 async 路径),则 pipeline goroutine 启动时 user 特征可能尚未加载完,存在对 `user.Properties` 的并发读写风险。真实存在的潜在 race 点。

---

## 4. 逐环节负载与瓶颈分析(进程内)

### 4.1 总表(CPU / IO / 内存 / 同步)

| 环节 | 主资源 | 复杂度 / 成本形态 | 瓶颈触发条件 | 锚点 |
|---|---|---|---|---|
| 召回(cache miss) | 网络 I/O | 每类 1 RTT,N 类并发 | 后端慢、无 singleflight 击穿 | `recall.go:138` |
| 召回(cache hit) | CPU | O(R) 字符串解析 | — | `vector_recall.go:35` |
| Bloom filter | 网络 I/O | Redis BITFIELD/BF.MEXISTS, batch=500 | Redis 慢 | `bloomfilter/redis_bitset.go:91` |
| ItemState filter | 网络+CPU | Hologres SQL batch=500, 4 goroutine, 200ms | 冷缓存、大候选集 | `item_state_filter_hologres_dao.go:145` |
| Unique/Snake filter | CPU | O(n)~O(n log n) | — | `unique_filter.go:26` |
| **特征装配** | 网络 I/O | Hologres chunk=600 / Redis chunk=100, Σ chunk RTT, 100ms | **冷缓存、多 Feature config** | `feature_hologres_dao.go:513` |
| Rank (EAS) | 网络 I/O | ceil(S/100)×len(algos) HTTP, 全并发, 100ms | EAS 慢、algo 多 | `rank/rank_service.go:273` |
| Rank AST 求值 | CPU | O(S) 表达式 | — | `rank_service.go:356` |
| **DPP sort** | CPU | **O(n²·d) kernel + O(n³) matmul** | S>200 | `dpp_sort.go:464-473` |
| SSD sort | CPU | O(T·n·d) | 大 T | `ssd_sort.go:346` |
| DiversityRule | CPU | O(K·n·W), 可逼近 O(n²) | 大窗口 | `diversity_rule_sort.go:116` |
| TrafficControl | CPU | O(n log n)+PID, 含 12ms 超时赛跑 | — | `traffic_control_sort.go:424` |

### 4.2 三个真正的瓶颈(进程内)

1. **特征装配是隐藏的 I/O 长杆**。在 rank 前运行,多个 Feature config 时总 RTT = Σ chunk,每个 100ms 超时兜底。冷缓存下可吃满多个并发 SQL 连接(Hologres maxOpen=100)。`async=true` 时全并发,瞬时连接飙升。
2. **DPP/SSD 重排是 CPU 长杆**(O(n²)~O(n³), `dpp_sort.go:464-473`)。靠 `candidateCnt`/`minScorePercent` 预截断 + `abortRunCnt` 短路缓解;配置不当让 n>200 时 CPU 飙升。gonum dense matmul 在主 goroutine 跑,阻塞该 pipeline goroutine 对应的 P。
3. **`AlgorithmFactory.mutex` 是热路径唯一进程级锁**。每次 EAS 调用 RLock,QPS 高时累积争用可见。虽是 RLock(读共享),但热加载时的写锁会阻塞所有读。

### 4.3 每环节"打"在什么系统资源上

- **召回/特征/精排的 I/O 路径**:打在 TCP 栈、连接池、后端网络 RTT。Go 侧网络 I/O 走 netpoller,不膨胀 M(线程);GOMAXPROCS 钳住 P,瓶颈转化为"可运行 G 争 P 的调度延迟"。
- **DPP/AST sort 的 CPU 路径**:打在 CPU 核 + 内存带宽(n² 矩阵)。无亲和性绑定。
- **in-proc 缓存(TinyLFU/go-cache)**:打在进程堆内存 + GC。Go GC STW 与缓存容量权衡。

---

## 5. 节点规模放大分析(单系统内)

"规模变大"在编排层有三处含义:候选集 S 变大、特征数 F 变大、召回路数 N 变大。逐一分析对**单节点资源**的影响:

| 规模维度 | 影响环节 | 资源影响(单节点内) |
|---|---|---|
| **S↑(候选数)** | DPP/SSD | CPU **二次~三次方**膨胀, `dpp_sort.go:464` matmul 是热点; n>200 后 CPU 长杆可能反超 I/O 长杆 |
| S↑ | Rank EAS | batch 数 `ceil(S/100)` 线性增, EAS 调用数线性增; 网络连接/RTT 线性 |
| S↑ | 特征 DAO | Hologres chunk=600, `cpuCount=ceil(S/600)` goroutine 线性增; 连接池 maxOpen=100 是天花板, 超了排队 |
| S↑ | Bloom/state filter | batch=500 固定, RTT 数 `ceil(S/500)` 线性增 |
| **F↑(特征数)** | FeatureService | Feature config 数线性增, 总 RTT=Σ chunk; `async=true` 时全并发→瞬时连接飙升 |
| F↑ | in-proc TinyLFU | `CacheSize` 决定内存占用; F 大且 `CacheSize` 未调→命中率降→后端压力升 |
| **N↑(召回路数)** | RecallService | 召回 goroutine 线性增, 并发后端连接线性增 |
| N↑ | merge | merge 是 sub-ms CPU, 不构成瓶颈 |

**单节点内的拐点**:

- **CPU 拐点**:DPP 让 n 越过 ~200 时, CPU 长杆超过 I/O 长杆。这是 OS 研究里"CPU 调度 vs I/O 调度"的切换点。
- **连接池拐点**:Hologres maxOpen=100、EAS MaxIdleConnsPerHost=2000、Redis MaxIdle(配置驱动,**无代码默认→0 是隐患**)。规模大到并发 DAO 批次超过池上限时, I/O 排队, p99 抬升。
- **内存拐点**:TinyLFU `WithMaximumSize` + DPP 500k embedding LRU(`dpp_sort.go:77`)+ Redis pool。规模↑ + 命中率↓ → 内存占用涨、GC 压力涨(Go GC STW)。`itemStmtMap` 按 IN-list 长度 key 的预编译缓存**无 eviction**(`feature_hologres_dao.go:581`), 规模下 batch size 抖动会让该 map 无界增长——这是已确认的 gap。

---

## 6. 多请求并发分析(QPS↑)

并发上升时,资源争用画像(基于 §3 的扇出结构 + §4 的池上限):

### 6.1 goroutine 数与调度延迟

```
QPS↑ → 并发 HTTP goroutine↑ (1 conn = 1 goroutine)
     → 每请求再扇出: 召回 N + DAO cpuCount + rank batch×algo
     → 瞬时 goroutine 数 ≈ QPS × (N + cpuCount + batch×algo)
     → GOMAXPROCS (automaxprocs 按 cgroup) 钳住 P → 可运行 goroutine 排队 → 调度延迟↑
     → 网络 I/O (N×M 个并发后端调用) 走 netpoller 不膨胀 M, 但可运行 G 堆积 → 争 P → 调度延迟↑
```

**关键量化**:单请求派生数十 goroutine(§3.1),QPS=1000 时进程内 goroutine 数可达数万。GOMAXPROCS(如 cgroup 限 8 核)钳住 P=8,可运行 goroutine 排队,调度延迟成为 p99 抬升源。

### 6.2 连接池天花板

| 后端 | 池上限 | 并发超限后果 |
|---|---|---|
| Redis | `MaxIdle`(配置驱动,无默认→0 隐患) | 连接耗尽→排队 |
| Hologres | maxOpen=100(env 可调) | 并发 SQL 批次>100→排队 |
| EAS | MaxIdleConnsPerHost=2000(EasyRec SDK 路径 2000) | 高 QPS+多 algo 下可能触顶 |

### 6.3 锁争用累积

- `AlgorithmFactory.mutex` RLock:QPS 高时累积,热加载写锁时短暂阻塞所有读。
- 其余锁 per-instance,争用不随 QPS 显式恶化。

### 6.4 无 singleflight 的击穿

热点用户/物品的 cache miss 在并发下直打后端:多个请求同一 key 同时 miss→同时打后端→后端 QPS 雪崩。这是 OS 层"请求合并/dedup"的典型优化点。in-proc TinyLFU 的 TinyLFU admission filter 降低长期 miss 率,但**不合并并发 miss**。

### 6.5 失败模式与"部分结果"语义

无请求级 context、无 errgroup,超时=部分结果(`feature_hologres_dao.go:627` 的 `select ctx.Done() return`)。高并发下后端变慢→更多请求走到 100ms 超时→返回更少候选→上游 `code:299 items size not enough`。这是"超时传播但取消不传播"的典型系统行为——OS 研究可关注"部分降级下的资源回收"。

### 6.6 内存与 GC

QPS↑ → in-proc 缓存填充↑ → 堆涨 → GC 频率↑ → STW 抖动。无 GC 调优配置暴露,默认 GOGC=100。高 QPS 下 GC STW 是 p99 尾部的重要贡献。

---

## 7. 面向 OS 下一代研究的输入与优化点(编排层内部)

按"缺位即机会"组织,全部锚定在 pairec 编排层:

### 7.1 请求合并 / dedup(singleflight 缺位)
热点 key 的并发 cache miss 直打后端(`recall.go` 召回缓存、`feature_*_dao.go` 特征缓存均无 singleflight)。**OS 机会**:内核或运行时层的"并发相同请求合并"原语,对 KV/SQL 后端 RTT 去重。推荐场景的热点用户/物品使该优化收益高。

### 7.2 取消传播与请求级生命周期
`RecommendContext` 不接 HTTP `r.Context()`,客户端断连/超时不取消在途工作,DAO 超时硬编码在 `context.Background()`(`feature_hologres_dao.go:157,601` 等)。**OS 机会**:进程内的请求级取消传播(structured cancellation),让在途召回/DAO/EAS 能随客户端断连即时释放,砍掉高并发下的"幽灵计算"。

### 7.3 CPU 二次方热点与亲和性
DPP 的 O(n³) matmul(`dpp_sort.go:464-473`)在主 goroutine 跑、无核绑定。**OS 机会**:自动识别二次方 CPU 热点并卸载到 SIMD/加速器,或调度到隔离核组避免拖累 I/O 路径的 goroutine(P 抢占)。

### 7.4 缓存层级与内存层级
in-proc TinyLFU + go-cache + Redis + Hologres 是多层缓存,但层级间无统一淘汰策略,`itemStmtMap` 无界增长(`feature_hologres_dao.go:581`)。**OS 机会**:跨层缓存统一淘汰/预取策略、内存占用可观测与 GC 协同(Go GC STW 与 LRU 容量的权衡)。

### 7.5 连接池自适应弹性
Redis `MaxIdle` 无默认、Hologres maxOpen=100、EAS 2000,高并发下静默排队。**OS 机会**:连接池自适应弹性、网络栈层的跨请求批量合并(Redis pipelining 已用,但 SQL/EAS 的跨请求 batching 缺位)。

### 7.6 goroutine 池化与公平调度
所有并发按请求即时派生,无池化复用,高 QPS 下 goroutine 创建/回收开销累积,可运行 goroutine 排队。**OS 机会**:goroutine 池化复用、跨请求的公平调度(避免大请求饿小请求)。

---

## 8. 速查:关键 file:line 索引

| 结论 | 锚点 |
|---|---|
| 编排心(双路并行合并) | `service/user_recommend.go:74,143,189` |
| HTTP 每连接派生 G0 | `app.go:103`(ListenAndServe) |
| 召回并发扇出 + recover | `service/recall.go:132,138,141` |
| GMP 阻塞点三类(I/O/channel/CPU) | §3.2 状态表 |
| 唤醒底层机制(netpoller/epoll vs futex vs 抢占) | §3.2 详解 |
| goroutine 计数公式(单请求/N 请求) | §3.3 |
| pipeline 扇出(无超时/recover) | `service/pipeline/pipeline.go:53,56,61` |
| 主路径配置按 scene 单槽解析 | `rank_service.go:154`, `filter.go:97`, `sort.go:99` |
| pipeline 配置按 PipelineConfig 绑定 | `recconf.go:1100`(PipelineConfig), `pipeline.go:33-43` |
| PipelineName 是死字段(仅日志) | `filter.go:30`, `sort.go:31`, `sort.go:212-213` |
| pipeline 路径可选(无配返回 nil) | `pipeline.go:48` |
| 真正 fallback 在 TryRecommendWithFallback | `user_recommend.go:206` |
| rank 两级 goroutine 嵌套 | `service/rank/rank_service.go:241,265,271` |
| 无 singleflight / 无 errgroup | 全树无 import |
| DAO 超时硬编码 100ms, 根 Background() | `module/feature_hologres_dao.go:157,601` |
| DPP O(n³) matmul | `sort/dpp_sort.go:464-473` |
| EAS batched 调用 | `service/rank/rank_service.go:273`, `algorithm/eas/client.go:14` |
| automaxprocs | `pairec.go:44` |
| 唯一进程级热锁 | `algorithm/algorithm.go:37` |
| 连接池默认值 | `persist/redisdb/redis.go:117`, `persist/holo/postgres.go:60-76`, `algorithm/eas/client.go:14-26` |
| 响应体不含 score | `web/recommend_controller.go:55` |
| 无 PreRecall/PreSort(死代码) | `service/recommend.go:44` |

---

## 9. 总结

pairec 编排层是一个"深层 goroutine 扇出 + 注册抽象 + 多层缓存"的成熟 Go 推荐服务。单请求内并发遍布每个 stage(召回扇出、rank 两级嵌套、DAO 工作队列),但缺 singleflight、缺请求级取消传播、无全局长生命周期 worker pool。**双路径方面**:主路径与 pipeline 路径是"并列完整管线"而非"保底 vs 可配置",决定性差异在配置维度——主路径按 scene 单槽解析所有 stage,pipeline 按 PipelineConfig 绑定且 `PipelineConfs[scene]` 可挂多链;多路召回主路径一条链即可(无需 pipeline),多套编排并行才需 pipeline;pipeline 的 `PipelineName` 仅日志标签,过滤/排序退回 scene 级。进程内三个真瓶颈是:特征装配 I/O 长杆、DPP/SSD CPU 二次方长杆、`AlgorithmFactory` 进程级热锁。规模放大时 CPU 拐点(DPP n>200)与连接池拐点(Hologres maxOpen=100)是主要切换点;多请求并发下 goroutine 数与 GC 压力是 p99 尾部主因。**线程模型承重结论**:pairec 热路径全网络 I/O 走 netpoller,M(线程)数稳定在 GOMAXPROCS 量级不随 QPS 膨胀,真实并发代价在"可运行 G 争 P 的调度延迟"而非线程开销——OS 观测若只盯线程数会误判。OS 介入的最大机会在请求合并、取消传播、CPU 热点调度三处。

报告全部结论锚定 pairec 上游源码,无臆测。

---

## 附录 A:分析范围说明

本报告严格限定在 pairec 上游框架进程编排层内部。下列内容**不在本报告范围**,见姊妹文档 `pairec-e2e-llm-analysis.md`:
- 下游 pairec4tigerllm 的生成式召回/LLM 接入(非成熟自研)
- GPU/TRT-LLM 推理进程内部
- 跨进程 BRPC/futex/DataSystem 交互
- 端到端跨进程请求流

本报告定位为"编排层基线",为后续对照扩展层的负载差异提供基准。

---

## 附录 B:融合全景图(高并发 × 线程模型 × 数据流时序)

本附录把 §3 的并发模型、§2 的数据流时序、§6 的多请求并发三视角叠成一张全景图,展示任务分发与 goroutine 协作的全貌。`▌ ▌` 框住 goroutine 边界, `◀═══` 标注 channel 合并点, 右侧标注 OS 层映射。

### B.1 单请求并发全景(goroutine 树 + channel 合并 + 数据流)

```
═════════════════════════════════════════════════════════════════════════════════════════
 HTTP 连接层 (net/http, 每连接 1 goroutine)
═════════════════════════════════════════════════════════════════════════════════════════
  client ──POST /api/recommend──▶ [G0 HTTP handler goroutine]
                                    │  web/recommend_controller.go:Process
                                    │  UUID requestId → makeRecommendContext
                                    ▼
═════════════════════════════════════════════════════════════════════════════════════════
 编排心  UserRecommendService.Recommend  (user_recommend.go:46)
═════════════════════════════════════════════════════════════════════════════════════════
                                    │
                                    ├─ LoadUserFeatures ──(User DAO RTT, 阻塞 G0)
                                    │
                    ┌───────────────┴────────────────┐
                    │ wg.Add(1); go func() {...}       │  user_recommend.go:74-79
                    ▼                                  │
          ▌ G-pipe: pipeline.Recommend  │              │  ← 第二路径(并行)
          │ (pipeline.go:45)              │              │
          │  scene → userRecommendScene   │              │
          │  Map[scene] → N pipeline svc  │              │
          │                               │              │
          │  ch := make(chan []*Item, N)  │              │
          │  ┌─ G-pipe-k (k=1..N) ────────┤              │
          │  │  pipeline.UserRecommend   │              │
          │  │  Service.Recommend:         │              │
          │  │   Recall→Filter→General    │              │
          │  │   Rank→Feature→Rank→       │              │
          │  │   ColdStart→Sort            │              │
          │  │  ch <- result ──────────────┼──┐           │
          │  └────────────────────────────┘  │           │
          │  for i<N { <-ch; ret=append }    │           │  ← 固定计数合并
          │  (无超时/无recover)               │           │
          └──────────────────────────────────┘           │
                                                         │
                    ┌────────────────────────────────────┘
                    ▼  (主路径同步, G0 继续执行)
═════════════════════════════════════════════════════════════════════════════════════════
 ① 召回  RecallService.GetItems  (recall.go:54)
═════════════════════════════════════════════════════════════════════════════════════════
         ch := make(chan []*Item, len(recalls))   recall.go:132
         ┌─ G-recall-j (j=1..M) ─────────────────────────┐
         │  defer recover()  ← panic 兜成空切片          │  recall.go:141-154
         │  recall.GetCandidateItems(user, ctx)          │
         │    └─ (cache miss) 后端 RTT (Redis/Hologres/  │
         │        BE/Graph/EAS) 阻塞此 G                 │
         │    └─ (cache hit) CPU 字符串解析             │
         │  ch <- items ─────────────────────────────────┼──┐
         └────────────────────────────────────────────────┘  │
         for i<M { <-ch; ret=append }  ←─ 固定计数合并 ───────┘  recall.go:168-171
                    │
                    ▼ items[]
═════════════════════════════════════════════════════════════════════════════════════════
 ② 过滤  Filter  (recommend.go:33 / filter.Filter)  —— 顺序执行, G0
═════════════════════════════════════════════════════════════════════════════════════════
         UniqueFilter(CPU) → BloomFilter(Redis RTT) → ItemState(Hologres RTT)
         → Snake(CPU) → Condition(CPU)
                    │
                    ▼
═════════════════════════════════════════════════════════════════════════════════════════
 ③ 粗排  GeneralRankService.Rank  (user_recommend.go:116)  —— G0
═════════════════════════════════════════════════════════════════════════════════════════
         冷启 vs 正常分流 → 粗排 EAS RTT
                    │
                    ▼
═════════════════════════════════════════════════════════════════════════════════════════
 ④ 特征装配  FeatureService.LoadFeatures  (feature_service.go:77)  —— 隐藏并发层
═════════════════════════════════════════════════════════════════════════════════════════
         async = FeatureSceneAsyncMap[scene]   feature_service.go:88
         ┌─ if async ─────────────────────────────────────┐
         │  for each Feature fₖ (k=1..F):                  │  feature_service.go:90-106
         │    wg.Add(1); go G-feat-k { fea.LoadFeatures }   │
         │  wg.Wait()  ← 合并                              │
         └─ else: 顺序 fea.LoadFeatures ──────────────────┘
         每个 fea.LoadFeatures 内部:
           ┌─ DAO 内部 fan-out ───────────────────────────┐
           │  cpuCount = ceil(n/chunk) 个 goroutine       │  feature_hologres_dao.go:513
           │  工作队列 chan + 结果 chan, 100ms ctx 超时    │
           │  (Hologres chunk=600 / Redis chunk=100)      │
           └──────────────────────────────────────────────┘
                    │
                    ▼ items[i].Properties 填充
═════════════════════════════════════════════════════════════════════════════════════════
 ⑤ 精排  RankService.Rank  (rank_service.go:102)  —— 两级 goroutine 嵌套
═════════════════════════════════════════════════════════════════════════════════════════
         ┌─ customRanks 并发 (第三路) ─────────────────────┐  rank_service.go:236-246
         │  for each customRank cₖ:                        │
         │    rankWG.Add(1); go G-custom-k {              │
         │      cₖ.Rank → 自己的 EAS 调用 (如 ColdStart)  │
         │    }                                            │
         └────────────────────────────────────────────────┘
         batchCount=100; batch数 = ceil(S/100)
         requestCh := make(chan IAlgoData, B)   ← 工作队列  rank_service.go:254
         responseCh := make(chan IAlgoData, B)  ← 结果队列  rank_service.go:255
         ┌─ 外层 G-rank-b (b=1..B) ─────────────────────────────────┐  rank_service.go:264-289
         │  algoData := <-requestCh                                 │
         │  ┌─ 内层 G-algo-a (a=1..A, A=len(RankAlgoList)) ───────┐  │  rank_service.go:269-285
         │  │  wg.Add(1); go func(algo) {                         │  │
         │  │    ret, err := algorithm.Run(algo, ...)  ← EAS HTTP │  │
         │  │    algoData.SetAlgoResult(...)                       │  │
         │  │  }(algoName)                                         │  │
         │  └──────────────────────────────────────────────────────┘  │
         │  wg.Wait()  ← 内层合并                                    │
         │  responseCh <- algoData ─────────────────────────────────┼──┐
         └────────────────────────────────────────────────────────────┘  │
         for b<B { <-responseCh; 处理结果 }  ← 外层合并 ─────────────────┘
         AST RankScore 表达式逐 item 求值 (CPU, G0)  → item.Score
                    │
                    ▼
═════════════════════════════════════════════════════════════════════════════════════════
 ⑥ Join + Merge + Sort + 截断  (G0)
═════════════════════════════════════════════════════════════════════════════════════════
         wg.Wait()  ← 等第二路径 G-pipe 完成            user_recommend.go:143
         mergePipelineItems(items, pipelineItems)      user_recommend.go:144,189
           (同 id 合并属性, pipeline-only 追加)
         Sort(items)  —— G0 CPU
           ItemScoreSort O(n log n) / DPP O(n²~n³) / SSD / TrafficControl(PID) / Diversity
         items = items[:size]  截断
                    │
                    ├─ go feature_log.FeatureLog       ← fire-and-forget  user_recommend.go:169
                    ├─ go LogSampleResult              ← fire-and-forget  user_recommend.go:170
                    └─ for _, hf := range CleanHooks { go hf(...) }  user_recommend.go:173-175
                    │
                    ▼
         序列化 ItemData[] → io.WriteString → 响应  (web/recommend_controller.go)
         (响应体只含 item_id/item_type/retrieve_id, 不含 score)
═════════════════════════════════════════════════════════════════════════════════════════
```

### B.2 OS 层映射(每个 goroutine 打在什么资源上)

```
goroutine 层                         OS / 运行时层                              资源/瓶颈
─────────────────────────────────────────────────────────────────────────────────────────
G0 HTTP handler ──────accept/序列化─▶ M0 (长驻 OS 线程)                      ① CPU 核(调度)
                                      GOMAXPROCS 钳 P

G-pipe-k (N 个) ─────────────────────▶ 各占 G/M, 后端 RTT 时阻塞            ② 网络 RTT
G-recall-j (M 个) ──┤                                                                  Redis/Hologres
G-feat-k (F 个) ────┤                  阻塞 I/O → G 挂 M, P 让给其他 G       (连接池天花板:
G-custom-k ─────────┤                                                                  Hologres maxOpen=100
  内层 G-algo-a ─────┘                  EAS HTTP → 同 Transport,             Redis MaxIdle=0 隐患
  外层 G-rank-b                          MaxIdleConnsPerHost=2000             EAS 2000)

DAO 内部 G (cpuCount 个) ─────────────▶ fan-out 至连接池, 100ms ctx 超时     ③ 100ms 硬超时
                                          ctx.Done() → 部分结果(不报错)

DPP matmul ───────────────────────────▶ 主 goroutine CPU, gonum dense         ④ CPU 二次方热点
                                          无 LockOSThread, 无绑核             (n>200 反超 I/O)

fire-and-forget ──────────────────────▶ 短暂 G, 尾部日志                      ⑤ off-path
                                          (FeatureLog/LogSample/CleanHooks)

in-proc 缓存 (TinyLFU/go-cache) ──────▶ 堆内存, GC 管理                       ⑥ GC STW
                                          GOGC=100 默认, 无调优配置           (高 QPS p99 尾部)
─────────────────────────────────────────────────────────────────────────────────────────

无 singleflight: 并发同 key cache miss 不合并 → 击穿直打后端        ⑦ 缺位
无请求级 context: r.Context() 不传播 → 断连/超时不取消在途工作       ⑧ 缺位
无全局长生命周期 worker pool: 按请求即时派生, 无池化复用             ⑨ 缺位
```

### B.3 多请求并发叠加(QPS↑ 时全景)

```
═════════════════════════════════════════════════════════════════════════════════════════
 N 个并发请求 (QPS↑) → N 个 HTTP handler goroutine (G0_1 ... G0_N)
═════════════════════════════════════════════════════════════════════════════════════════
  每个请求再扇出, 瞬时 goroutine 总数:
    N × [ 1(pipeline扇出) + M(召回) + F(特征) + cpuCount(DAO) + B×A(rank嵌套) + C(custom) ]
    ≈ N × 数十   →  QPS=1000 时进程内 goroutine 数万
                ▼
  ┌──────────────────────────────────────────────────────────────────────────────────┐
  │ GOMAXPROCS (=cgroup CPU 配额, automaxprocs) 钳住 P                              │
  │   可运行 goroutine 排队 → 调度延迟↑ → p99 抬升                                   │
  │   网络 I/O (N×M 个并发后端调用) 走 netpoller → 可运行 G 堆积 → 争 P → 调度延迟↑  │
  └──────────────────────────────────────────────────────────────────────────────────┘
                ▼
  ┌──── 连接池天花板 ──────────────────────────────────────────────────────────────────┐
  │ Redis:    MaxIdle (配置驱动, 无默认→0 隐患)      超限→连接排队                    │
  │ Hologres: maxOpen=100 (env 可调)                 并发 SQL 批次>100→排队            │
  │ EAS:      MaxIdleConnsPerHost=2000               高 QPS+多 algo 可能触顶            │
  └──────────────────────────────────────────────────────────────────────────────────┘
                ▼
  ┌──── 锁争用累积 ──────────────────────────────────────────────────────────────────┐
  │ AlgorithmFactory.mutex RLock (每次 EAS 调用, 全局唯一进程级热锁)                  │
  │   QPS↑ → RLock 累积争用; 热加载写锁时短暂阻塞所有读                               │
  │ 其余锁 per-instance, 不随 QPS 显式恶化                                            │
  └──────────────────────────────────────────────────────────────────────────────────┘
                ▼
  ┌──── 缓存击穿 (无 singleflight) ─────────────────────────────────────────────────┐
  │ 热点 user/item: N 请求同时 cache miss → N 路直打后端 → 后端 QPS 雪崩             │
  │ TinyLFU admission filter 降长期 miss 率, 但不合并并发 miss                       │
  └──────────────────────────────────────────────────────────────────────────────────┘
                ▼
  ┌──── 内存与 GC ───────────────────────────────────────────────────────────────────┐
  │ in-proc 缓存填充↑ → 堆涨 → GC 频率↑ → STW 抖动 (GOGC=100 默认)                    │
  │ itemStmtMap 无 eviction → 规模下 batch size 抖动使该 map 无界增长                │
  └──────────────────────────────────────────────────────────────────────────────────┘
                ▼
  ┌──── 失败模式: "部分结果"语义 ────────────────────────────────────────────────────┐
  │ 后端变慢 → 更多请求走到 100ms DAO 超时 → 返回更少候选 → code:299 items not enough │
  │ (超时传播, 但取消不传播: r.Context() 不接, 在途工作不释放)                       │
  └──────────────────────────────────────────────────────────────────────────────────┘
```

### B.4 三视角融合要点

| 融合点 | 线程模型视角 | 数据流视角 | 高并发视角 |
|---|---|---|---|
| **召回扇出** | M goroutine + buffered chan + recover | 每路 1 RTT, channel 固定计数合并 | N 请求 × M 召回 → 连接池压力 + 击穿 |
| **rank 两级嵌套** | 外层 B + 内层 A goroutine + 双 channel | batch × algo EAS 调用, AST 求值回填 score | N × B × A → `AlgorithmFactory` 锁争用 + EAS 池天花板 |
| **特征 DAO fan-out** | cpuCount goroutine + 工作队列 chan | chunk 化批量取, 100ms ctx | N × cpuCount → SQL 连接池(100)天花板, 隐藏 I/O 长杆 |
| **pipeline 并行** | N goroutine + chan, 无超时/recover | 第二路径与主路径 `wg.Wait` join 后 merge | 慢 pipeline 拖整个请求, 无超时放大长尾 |
| **DPP sort** | 主 goroutine CPU, 无绑核 | O(n²~n³) matmul | n>200 时 CPU 长杆反超 I/O, 阻塞 P |
| **fire-and-forget** | 尾部短暂 goroutine | 日志/特征落盘 | off-path, 不阻塞响应 |

### B.5 关键的"并发交汇点"(最值得 OS 研究盯住)

1. **`wg.Wait()` join 点**(`user_recommend.go:143`):主路径与 pipeline 路径在此汇合。这是"长短杆"交汇——慢的路径决定请求延迟,且无超时,长尾在此放大。
2. **rank 双 channel**(`rank_service.go:254,255`):工作队列 + 结果队列的 fan-out/fan-in,是 rank 阶段并发的核心结构。内层 `wg.Wait()` 是 algo 粒度合并,外层固定计数循环是 batch 粒度合并。
3. **特征 async 分叉点**(`feature_service.go:88-106`):`FeatureSceneAsyncMap[scene]` 一个 bool 决定特征装配是并发还是顺序。隐藏的 I/O 长杆在此分叉/合并。
4. **连接池 × goroutine 数交汇**:并发 DAO goroutine 数(N × cpuCount)× 池上限(Hologres 100)——超了就排队,是高并发 p99 抬升的物理根因。
