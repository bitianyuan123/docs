# 生成式召回：4+1 视图

图中符号与连线遵循[图法与 UML 约定](DIAGRAM_NOTATION.md)，每张图前注明使用的图法。

本模块按目标设计更新：历史与物品编码关系来自特征服务；模型引擎、分词器和编码模型属于模型资产。当前源码仍使用本地映射文件，新查询接口尚待实现。OneTrans 的实际取数例外不延伸到本模块。

```yaml
职责: 从用户历史生成候选物品
输入: 历史物品语义编码、候选数、生成参数、请求与版本身份
输出: 原始物品ID、来源内次序、解码器分数、实际执行状态
物品语义编码: semantic_id，简称SID；当前模型使用四个整数表示一个物品
模型资产: TensorRT-LLM引擎、tokenizer分词器、编码模型或码本
业务表示数据: 原始物品ID与SID的双向绑定，统一由特征服务查询
计算缓存: 模型注意力KV，由运行时产生，使用DataSystem管理
```

## 1. 逻辑视图

图法：逻辑结构示意图（非 UML）。箭头表示需要对方提供的能力，不表示执行顺序；历史 ID 正查由编排负责，输入本模块时已经是历史 SID。

```mermaid
flowchart LR
    G[生成式召回] --> I[模型输入适配]
    G --> M[候选编码生成]
    G --> C[候选集合整理]
    G --> F[特征服务的编码反查能力]
    I --> T[模型分词规则]
```

| 职责 | 输入 → 输出 | 责任边界 |
|---|---|---|
| 历史物品编码查询 | 历史原始 ID → 有效 SID 列表、缺失位置 | PaiRec 调用特征服务 |
| 模型输入适配 | SID 列表 → 模型 tokens | 使用与引擎匹配的 tokenizer，不在线训练编码模型 |
| 候选编码生成 | tokens、生成参数 → 输出 tokens | 模型计算；不把输出编码直接当作原始物品 ID |
| 编码反查 | 有效输出 SID → 每个 SID 对应的 `item_ids[]` | 特征服务拥有业务映射，生成服务查询它 |
| 候选整理 | 映射结果 → 去重后的原始 ID 与来源次序 | 保存来源次序及缺失状态，不决定全局展示顺序 |



历史正查与生成反查的缺失处理不同：首期历史输入要求编码完整；生成输出中没有目录关联的 SID 则记录并丢弃，全部无法还原时返回真实空候选。

多个物品可能共享 SID。反查必须保留一个 SID 对应的物品列表，不能把它们写进单值 map 后只留下最后一个。稳定规则为：先保留生成 SID 的来源次序，同一 SID 内按原始 ID 的十进制数值排序，再去重和限量。该规则提供确定性，不代表编码碰撞已消失或生成质量提高。

## 2. 开发视图

图法：源码依赖示意图（非 UML）。节点是源码模块；箭头表示导入、链接或编译依赖，客户端模块不代表远端服务进程。

```mermaid
flowchart LR
    P[PaiRec 生成客户端] --> F[特征查询客户端]
    P --> R[生成RPC客户端]
    H[生成服务处理器] --> T[输入与输出token转换]
    H --> E[TRT Executor封装]
    H --> B[编码反查客户端]
    E --> K[模型缓存存储适配]
```

| 源码范围 | 交付产物 |
|---|---|
| PaiRec 生成客户端与字段适配 | 编入推荐程序的客户端代码 |
| 生成处理器、token 转换、Executor 封装及特征客户端 | 原生生成服务程序，配套引擎、tokenizer 与版本配置 |
| 模型缓存适配与依赖库 | 链接到生成程序或其运行库的代码；DataSystem worker 另行部署 |

```yaml
目标接口元数据:
  request_id: 全链路关联ID
  release_id: 固定的数据发布版本
  sid_version: 原始ID与SID绑定版本
  remaining_timeout_ms: 扣除已用时间后的调用预算
历史编码缺失: 首期固定fail_branch；任一历史ID缺SID则报告ID并使生成分支失败
编码反查缺失: 新生成SID可能无目录关联；记录缺失SID并不产生候选，禁止伪造原始ID
版本不匹配: 明确失败，不能回退到另一份本地映射文件
```

现有 [Recommend proto](assets/source_snapshots/pairec4tigerllm/proto/recommend.proto.html#L11)接收 `user_id/history/topk/temperature/beam_width/request_id`，历史每项是整数数组；返回 `item_id:int32`、`semantic_id` 和 `score`。它**没有完整的上述版本元数据**。增加字段或建立经过验证的服务启动绑定是开发工作，不能在文档中直接当作已有 wire schema。原始 ID 在新接口使用字符串；转入旧 `int32` 返回路径前必须检查范围。

| 当前实现依据 | 与目标的差距 |
|---|---|
| [TRT 推理初始化](assets/source_snapshots/pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp.html#L989) | 有真实模型执行基础；仍须绑定实际引擎、分词器、GPU 与运行库 |
| [本地反向映射](assets/source_snapshots/pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp.html#L970) | 改为特征服务批量反查；解决单值覆盖，报告映射覆盖率 |
| [当前候选构造](assets/source_snapshots/pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp.html#L1428) | 分层 token 收集后组合，不是完整 SID 序列概率 top-k；当前候选分数为常量 1.0 |
| [原生 DataSystem 接入说明](assets/source_snapshots/pairec4tigerllm/docs/F19_NATIVE_DATASYSTEM_ATTRIBUTION.md.html#L77) | 需要实际 TRT 基础修改与构建产物，归因补丁本身不足以复现 |

PaiRec 首期按来源配额和来源次序融合，不把生成的常量分数与向量、BM25 原始分数直接相加。完整约束解码与模型效果改进后置。

## 3. 进程视图

### 3.1 目标业务交互

图法：UML 时序图。生成服务处理器与 TRT 模型运行时是同一进程内的两个执行角色，二者间的消息不是额外网络调用；PaiRec、特征服务与 DataSystem 是外部角色。

```mermaid
sequenceDiagram
    participant P as PaiRec
    participant F as 特征服务
    participant G as 生成服务
    participant T as TRT模型运行时
    participant D as DataSystem
    P->>F: BatchGetItemRepresentations：历史原始ID、SID版本
    F-->>P: 同序SID或缺失状态
    P->>G: 历史SID、生成参数、请求身份
    G->>T: 模型tokens与生成预算
    opt 运行时需要换入已有模型缓存
        T->>D: 查询实际缓存块key
        D-->>T: 张量字节或未命中
    end
    T->>T: 实际前向和逐token生成
    opt 运行时触发缓存换出
        T->>D: 保存真实KV张量
        D-->>T: 写入结果
    end
    T-->>G: 输出tokens与执行状态
    G->>F: BatchGetItemRepresentations：生成SID、同一版本
    F-->>G: 每个SID对应的物品ID列表或缺失
    G->>G: 展开、去重、限制候选数量
    G-->>P: 原始候选ID、来源次序、当前解码分数
```

```python
# 目标数据流伪代码；前一段在PaiRec，后一段在生成服务执行。
# PaiRec：批量查询历史SID，再作为生成请求输入。
history_repr = feature.BatchGetItemRepresentations(
    lookup_by="raw_item_id", item_ids=history.item_ids,
    release_id=release_id, sid_version=sid_version)
require_all_history_codes_found(history_repr)  # 缺失即报告ID并使生成分支失败
history_sids = values_in_history_order(history_repr)
# 请求构造还须符合引擎输入容量；仅保留末尾完整物品，不能切断四层编码。
# 生成服务：使用收到的history_sids推理，再批量反查输出编码。
output_tokens = engine.generate(tokenizer.encode(history_sids), generation_options)
generated_sids = decode_complete_valid_sids(output_tokens)
raw_items = feature.BatchGetItemRepresentations(
    lookup_by="semantic_id", semantic_ids=stable_unique(generated_sids),
    release_id=release_id, sid_version=sid_version)
return expand_in_generated_order_then_deduplicate(raw_items, limit=topk)
```

模型缓存读写由真实块生命周期触发，不能要求每请求固定若干次 Set/Get。特征服务反查是业务数据查询，与模型缓存访问是两种不同操作。模型执行成功但反查数据库失败，应返回阶段失败；反查成功但无合法物品，可以返回真实空候选。

### 3.2 当前 C++ 进程：输入、处理、输出

以下 IPO（Input / Process / Output，即输入、处理、输出）限定 `trtllm_cpp` 后端，并关闭人工 Set/Get 探针。用户 1 的教学请求含 10 项、每项 4 个整数的历史 SID，生成路 `topk=10`；完整字段见[生成请求](assets/request_example/06_generate.request.json)。SID 与模型词表的匹配还需真实资产验证，不能假造 token ID 或把教学输出当执行记录。

| 源码函数与执行者 | 输入 | 处理及持有的数据 | 输出 |
|---|---|---|---|
| `NativeInferenceServiceImpl::Recommend`，RPC 处理任务 | protobuf 请求 | 同步进入后端；本次 response 保持到 `ClosureGuard` 调用完成回调 | RPC 响应或失败 |
| `BuildPromptTokens`，同一任务 | 10 项历史、只读 tokenizer | 过滤空编码，转换各层 token，加前后缀和分隔符；超长时逐项删旧历史 | 请求私有 `vector<int> prompt_tokens` |
| `Recommend` 中采样循环 | prompt、服务端采样配置 | 每次 `RunExecutor` 完成后才开始下一次；输出追加到请求私有 `sampled_tokens` | 多次输出 token 的合并数组 |
| `ParseOutputTokens`，同一任务 | 输出 token、只读 token→SID 表 | 按层收集、去重、排序，空层补 0，再生成全部层间组合 | `semantic_candidates`，可能多于 topk |
| `FillRecommendations`，同一任务 | SID 组合、本地反向表、请求历史 | 去历史、查原始 ID、去重，至 topk 停止；当前分数固定为 1.0 | `recommendations[]`；目标需改为特征服务反查 |

函数依据：[入口与完成回调](assets/source_snapshots/pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp.html#L1775)、[prompt 构造](assets/source_snapshots/pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp.html#L1221)、[编码组合与映射](assets/source_snapshots/pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp.html#L1427)。只剩一项历史但仍超长时，当前实现会从 token 数组前部截断；目标需要整项容量检查，不能把当前行为写成已保证完整输入。

```python
# 当前控制流的等价摘录；不是可直接运行的SDK。
prompt_tokens = BuildPromptTokens(request.history)
sampled_tokens = []
base_seed = reserve_seed_range(config.trt_num_samples)
for sample_index in range(config.trt_num_samples):
    sampled_tokens.extend(RunExecutor(prompt_tokens, base_seed + sample_index))
semantic_candidates = ParseOutputTokens(sampled_tokens)
response = FillRecommendations(request, semantic_candidates)
# reserve_seed_range对应循环前一次seed_.fetch_add，预留本请求的种子区间。
```

这里的采样次数、生成 token 上限、采样温度和采样 top-k 来自服务启动 `config`；原生 TRT 路径没有将请求 `temperature/beam_width` 直接传入采样配置。请求 `topk` 只在候选输出截取时使用。验收应记录实际生效配置。[采样配置](assets/source_snapshots/pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp.html#L1317)

### 3.3 Executor 提交、等待与完成

图法：UML 时序图，展示一次请求的成功路径。两条生命线都是同一生成进程的代码角色；Executor API 背后的线程、GPU 流和缓存执行者由所链接运行库管理，不凭包装层虚构线程数。

```mermaid
sequenceDiagram
    participant H as 当前RPC处理任务
    participant E as 同进程Executor API
    H->>H: BuildPromptTokens：10项SID转prompt
    loop sample_index小于服务端trt_num_samples
        H->>E: enqueueRequest(prompt,生成配置)
        E-->>H: executor_request_id
        loop 尚未收到final且未达到本次采样期限
            H->>E: awaitResponses(executor_request_id,10ms)
            Note over E: 等待结果；底层运行库推进计算
            E-->>H: 空结果或响应列表
        end
        H->>H: 复制最终outputTokenIds，追加sampled_tokens
    end
    H->>H: ParseOutputTokens，生成SID组合
    H->>H: FillRecommendations，本地映射和topk截取
    H->>H: 填充trace，作用域退出时执行done
```

`enqueueRequest` 返回的是运行库请求 ID，不是模型完成。`awaitResponses` 的 10 ms 是单次等待上限；收到 final 才继续业务代码，响应携带错误或本次采样超时调用 `cancelRequest`；异常捕获分支仅返回错误。调用结束不证明 GPU 或缓存回收已经完成；包装层没有将客户端取消绑定到统一的整次多采样期限。[提交、等待与取消](assets/source_snapshots/pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp.html#L1299)

| 并发或同步位置 | 代码可确认的事实 | 不能直接推导的结论 |
|---|---|---|
| RPC 处理任务 | 处理器内同步调用后端，完成回调在作用域退出时执行 | 不等于一次请求新建一个 OS 线程 |
| 共享 Executor | 多请求使用同一个 `executor_`；每请求按自己的 ID 等结果，同请求多次采样顺序提交 | `max_batch_size` 是运行库批约束，不是 RPC 总在途数上限 |
| 等待结果 | 调用所链接运行库的 `awaitResponses`；可能以结果可用或等待期满返回 | 包装层不足以判断底层条件变量、唤醒线程或是否让出 bRPC 工作 pthread；须核对运行库版本与等待栈 |
| 共享小状态 | seed 和归因 ID 用原子递增；tokenizer、映射表在启动后读取；诊断输出可持 mutex | 原子计数不是串行化所有推理；日志锁也不是已测瓶颈 |

逐次采样使一次请求的等待累加；同时处理多个请求是否能被运行库有效合批，需要测量。10 ms 等待参数不构成“CPU 每 10 ms 忙等”的证据。可选人工缓存探针产生的固定字节和 Set/Get 次数不计入模型业务负载，真实缓存传输仍由块生命周期决定。

## 4. 物理视图

图法：部署映射示意图（非 UML）。目标 Pod 包含容器，容器列出进程；双向连线表示网络连通。主机与副本数未定，DataSystem 内部部署见系统文档。

```mermaid
flowchart LR
    subgraph PP[Pod：推荐编排]
        P[容器：PaiRec<br/>进程：推荐程序]
    end
    subgraph GP[Pod：生成式召回]
        G[容器：生成服务<br/>进程：brpc_inference_server<br/>内含 TRT Executor]
    end
    subgraph FP[Pod：特征服务]
        F[容器：特征服务<br/>进程：特征查询程序]
    end
    subgraph WP[Pod 类型：模型缓存]
        K[容器：DataSystem worker<br/>进程：worker]
    end
    P <-->|bRPC| G
    P <-->|特征RPC| F
    G <-->|编码反查RPC| F
    G <-->|SDK连接| K
```

生成容器挂载匹配的模型引擎与 tokenizer，并获得所需 GPU；两者不是独立进程。原生生成容器已有[部署配置](assets/source_snapshots/pairec4tigerllm/k8s/deployment-inference-brpc-trtllm.yaml.html#L35)，特征客户端及完整联调仍待补。特征服务所用 Redis、worker 所用 etcd 的容器边界见[系统物理视图](01_system.md)，此图不重复展开。

```yaml
启动检查:
  模型: engine与GPU、TRT运行库匹配
  特征: release_id和sid_version已发布，绑定到同一模型编码空间
  数据库: 特征服务真实读回、DataSystem真实连接
  调用预算: 预留推理后的SID反查时间，不能推理耗尽全部请求预算
  容量: GPU缓存和外部缓存分别设上限
模型缓存回收: 遵循块引用关系；可能跨请求复用，不按业务特征TTL删除
```

当前外部缓存的有界回收仍需验证，见[缓存生命周期记录](assets/source_snapshots/pairec4tigerllm/docs/PERSISTENT_NATIVE_KEY_LIFECYCLE.md.html#L19)。不能仅凭一次推理成功宣布长期运行无泄漏。

## 5. 场景视图（+1）

图法：结构或流程示意图（非 UML，连线含义见本节）。

```mermaid
flowchart TD
    H[用户1的10项真实历史ID] --> M[特征服务查询实际SID]
    M --> Q{存在可用的模型历史}
    Q -->|否| E[明确空历史或失败]
    Q -->|是| G[TRT执行真实生成]
    G --> R[特征服务按生成SID反查物品列表]
    R --> D[按稳定次序展开并去重]
    D --> C[返回实际候选，允许不足topk]
```

真实历史 ID 见[请求推演](08_request_walkthrough.md)。第 08 篇提供明确标记的教学 SID 与候选，用于说明字段和顺序；这些数值不是本地已发布的模型产物。验收需分别证明：历史编码版本正确、TRT 实际执行、反向查询返回真实物品、碰撞处理稳定、缓存容量有界。

## 6. 业务负载怎样转化为 OS 诉求

先按上一节函数链统计工作量，再判断 CPU、GPU 或等待是否主导。以下公式不代表已测容量。

```text
λ = 每秒进入本进程的推荐数；S = 一请求实际完成的平均采样次数
Executor提交率约为 λ×S；失败、取消、重试需按实际提交另计
N ≈ λ×W，W为请求平均停留秒数，N为稳定状态平均在途推荐数
# 同请求顺序采样，不能把N×S当作同一时刻活跃模型请求数。

q[l] = 本次合并输出在第l层出现的不同编码数，空层按源码补成1
编码组合数 = q[0]×q[1]×q[2]×q[3]
# 教学演算：每层8种值 → 4096组；每层16种值 → 65536组。
# BuildCartesian先保存全部组合，之后FillRecommendations才按topk截取。
```

| 子系统 | 哪个业务步骤提出资源诉求 | 高并发下可能如何受限 | 核对什么 |
|---|---|---|---|
| CPU | prompt 构造、token 查表、组合枚举、protobuf 编解码 | 多采样增加输出 token；组合数乘法增长，可在 GPU 已结束后继续占 CPU | prompt/parse/map 分段 CPU 时间、组合数、输出字节；墙钟时间另计 |
| 调度与同步 | RPC 任务、运行库线程、结果等待及回调需及时运行 | 原生阻塞若占满工作线程，会延后新请求或完成处理；CPU 配额节流会延迟已就绪线程 | 各线程等待栈、可运行等待、配额节流、提交至 final 时长；先查实际运行库机制 |
| 内存 | 只读映射常驻；每请求保存 prompt、所有采样输出及 SID 组合 | 在途数与组合数共同放大堆内存；模型 HBM 和主机缓存又是另一组资源 | RSS、堆分配热点、各队列字节、HBM/主机缓存占用和回收；不只看最终候选大小 |
| I/O | 启动加载引擎、tokenizer、映射；运行中诊断日志和可能的缓存换入换出 | 冷启动文件读取、同步日志或缓存存储慢，会延长任务占用 | 启动与稳态分开；磁盘、日志、SDK 调用及等待栈分开 |
| 网络 | 入口 RPC、目标 SID 反查、实际远端缓存传输 | 缓存字节或反查响应增大，网络等待延长进程内对象生命周期 | 每种协议的调用数、字节、连接、重传；不把 Executor 等待全归为网络 |
| GPU 与驱动 | prefill、逐 token 解码、缓存搬运 | 模型批量、token 预算、HBM 容量和传输竞争；宿主 CPU 不及时提交也可能使 GPU 空闲 | 运行库队列、GPU 执行/传输、token 数、缓存命中及字节；与 OS CPU 调度对齐 |

系统需要可用 CPU 时间、可观测的调度等待、足够的内存和 I/O 能力；**限制在途请求、总 token 与组合数、传播取消并回收对象仍是应用和模型运行库的责任**。增加 bRPC 线程不能消除 GPU 排队或组合数组膨胀。先在固定模型、采样次数、历史长度下增加外部并发，再分别改变这些工作量变量，才能判断瓶颈来自哪里；本轮未执行这类测试。
