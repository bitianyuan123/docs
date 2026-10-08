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

图法：结构或流程示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    A[历史物品编码查询] --> B[模型输入构造]
    B --> C[自回归生成]
    C --> D[编码还原为物品]
    D --> E[合法候选整理]
```

| 职责 | 输入 → 输出 | 执行位置 |
|---|---|---|
| 历史物品编码查询 | 历史原始 ID → 有效 SID 列表、缺失位置 | PaiRec 调用特征服务 |
| 模型输入构造 | SID 列表 → 模型 tokens | 生成服务使用固定 tokenizer |
| 自回归生成 | tokens、生成参数 → 输出 tokens | TensorRT-LLM |
| 编码还原 | 有效输出 SID → 每个 SID 对应的 `item_ids[]` | 生成服务调用特征服务 |
| 候选整理 | 映射结果 → 去重后的原始 ID 与来源次序 | 生成服务 |

```python
# 目标伪代码。每次批量查询，而非逐物品RPC。
history_repr = feature.BatchGetItemRepresentations(
    lookup_by="raw_item_id", item_ids=history.item_ids,
    release_id=release_id, sid_version=sid_version)
require_all_history_codes_found(history_repr)  # 缺失即报告ID并使生成分支失败
history_sids = values_in_history_order(history_repr)
# 请求构造还须符合引擎输入容量；仅保留末尾完整物品，不能切断四层编码。
output_tokens = engine.generate(tokenizer.encode(history_sids), generation_options)
generated_sids = decode_complete_valid_sids(output_tokens)
raw_items = feature.BatchGetItemRepresentations(
    lookup_by="semantic_id", semantic_ids=stable_unique(generated_sids),
    release_id=release_id, sid_version=sid_version)
return expand_in_generated_order_then_deduplicate(raw_items, limit=topk)
```

历史正查与生成反查的缺失处理不同：首期历史输入要求编码完整；生成输出中没有目录关联的 SID 则记录并丢弃，全部无法还原时返回真实空候选。

多个物品可能共享 SID。反查必须保留一个 SID 对应的物品列表，不能把它们写进单值 map 后只留下最后一个。稳定规则为：先保留生成 SID 的来源次序，同一 SID 内按原始 ID 的十进制数值排序，再去重和限量。该规则提供确定性，不代表编码碰撞已消失或生成质量提高。

## 2. 开发视图

图法：模块关系示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    P[PaiRec 生成客户端] --> F[特征查询客户端]
    P --> R[生成RPC客户端]
    H[生成服务处理器] --> T[输入与输出token转换]
    H --> E[TRT Executor封装]
    H --> B[编码反查客户端]
    E --> K[模型缓存存储适配]
```

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

图法：UML 时序图。

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

模型缓存读写由真实块生命周期触发，不能要求每请求固定若干次 Set/Get。特征服务反查是业务数据查询，与模型缓存访问是两种不同操作。模型执行成功但反查数据库失败，应返回阶段失败；反查成功但无合法物品，可以返回真实空候选。

## 4. 物理视图

图法：部署映射示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    P[PaiRec进程] -->|原生bRPC目标接口| G[生成进程与GPU]
    P -->|特征RPC| F[特征服务进程]
    G -->|特征RPC| F
    F -->|Redis协议| R[(在线特征Redis)]
    A[(模型引擎与tokenizer文件)] --> G
    G <-->|DataSystem SDK| K[(模型缓存存储)]
```

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
