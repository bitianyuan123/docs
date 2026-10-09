# 一次请求：用户 1 的时序、输入与输出

本文沿着用户 1 的请求，展示取特征、三路召回、召回合并、精排、重排与返回。特征服务和召回使用目标接口；OneTrans 保留当前 `/ingest`、`/rank` 和本地取数方式。

本文是**教学样例，不是服务运行记录**：用户和历史沿用已有样例，向量、语义编码、召回结果和模型分数用于说明格式与字段传递。完整数据见[本次请求 JSON](assets/walkthrough_sample.json)，逐接口查看见[完整请求与响应](assets/request_example/README.md)。正文用 Python 表达数据构造、JSON 表达接口内容；它们共同说明流程，不是一份可直接运行的客户端。

本文的 Redis 交互按单分片、未拆批的查询展示；四次业务特征 RPC 不等于任何部署下都只有四条 Redis 命令。具体工作量、拆批及线程等待见 [Redis 负载分析](11_redis_workload.md)，PaiRec 和 OneTrans 内部执行见各自模块文档。

## 1. 请求怎样变成调用参数

### 1.1 原始请求与场景配置

```http
POST /api/recommend HTTP/1.1
Content-Type: application/json

{"uid":"1","scene_id":"home_feed","size":10}
```

PaiRec 生成本次 `request_id`，按 `scene_id` 选择场景，并固定该场景绑定的发布版本。`topk` 来自场景配置，是各路召回的候选上限；`size=10` 来自客户端，经场景校验后用于最终截断。

```python
external_request = {"uid":"1", "scene_id":"home_feed", "size":10}
request_id = "demo-user-1-001"       # PaiRec在请求入口生成
user_id = external_request["uid"]
scene = {"vector_topk": 50, "sparse_topk": 50,
         "generative_topk": 10, "candidate_limit": 50, "size": 10}

# 从场景绑定的发布清单取得；以下均为教学版本名。
release_id = "demo_tenrec_v1"        # 固定整套特征与索引数据
versions = {"embedding_space_id": "demo_dssm_64_v1",     # 向量计算方式
            "sparse_recipe_id": "video_type_binary_v1", # 兴趣词项配方
            "sid_version": "demo_sid_v1"}              # 物品语义编码规则
record_header = {"release_id": release_id,
                 "schema_version": "feature_v1"}       # 特征接口约定的字段格式
```

原始用户、物品 ID 在目标接口中使用字符串；OneTrans `/ingest` 沿用现有协议，其历史物品 ID 使用整数。

### 1.2 公共字段与每次调用的超时

`call_meta` 和 `feature_meta` 是本文定义的**辅助函数，不是 SDK 方法或 RPC 接口**。它们只生成公共字段，省去后文重复填写请求标识、发布版本和超时。

```python
# remaining_budget_ms()读取PaiRec为本次请求维护的剩余总时间。
def call_meta(stage_limit_ms):
    return {**record_header, "request_id": request_id,
            "remaining_timeout_ms": min(stage_limit_ms, remaining_budget_ms())}

def feature_meta():
    return call_meta(1000)  # 特征查询最多使用1000毫秒
```

| 代码 | 展开后的含义 |
|---|---|
| `call_meta(5000)` | 生成 `request_id/release_id/schema_version/remaining_timeout_ms` 四个字段；5000 是本阶段超时上限，单位为毫秒 |
| `**call_meta(5000)` | Python 字典展开：将这四个字段直接并入请求；发送的 JSON 中没有 `**` 或 `call_meta` 字段 |
| `**feature_meta()` | 同样展开四个字段，阶段上限改为 1000 毫秒 |

PaiRec 在入口用“单调时钟当前值＋场景总超时”确定截止时刻。每次调用前，`remaining_budget_ms()` 重新计算“截止时刻－当前值”；单调时钟用于计时，不受系统日期调整影响。剩余时间和阶段上限取较小者，耗尽时停止调用。第 4.1 节给出了这些字段完全展开后的请求和响应。

## 2. 完整主时序

图法：UML 时序图，符号见[图法约定](DIAGRAM_NOTATION.md)。实线消息发起调用，虚线消息返回结果；`par` 内各分支并行推进，各分支内按顺序等待。PaiRec 在提交 `/rank` 前等待候选和历史两路完成。

图中 `items[*].item_id` 表示数组内各项的物品 ID，`vector_request` 等对象在后文展开。数据库交互放在第 3～5 节子图。

```mermaid
sequenceDiagram
    autonumber
    participant U as 客户端
    participant N as Nginx
    participant P as PaiRec
    participant F as 特征服务
    participant V as 向量召回
    participant S as 稀疏召回
    participant G as 生成式召回
    participant H as OneTrans历史
    participant R as OneTrans精排
    U->>N: POST /api/recommend<br/>{uid:"1",scene_id:"home_feed",size:10}
    N->>P: 同一JSON；X-Gateway-Request-ID
    P->>P: request_id="demo-user-1-001"<br/>release_id="demo_tenrec_v1"
    par 候选准备
        P->>F: GetUserContext(user_request)，user_id="1"
        F-->>P: user_context {user,history,<br/>dense_query,sparse_query,view_status}
        par 向量召回
            P->>V: Recall(vector_request)<br/>query_vector = user_context.dense_query.vector
            V-->>P: items[*].item_id=["1201","9001","3","4"]
        and 稀疏召回
            P->>S: Recall(sparse_request)<br/>sparse_tokens = user_context.sparse_query.tokens
            S-->>P: items[*].item_id=["4","9002","9003"]
        and 生成式召回
            P->>F: BatchGetItemRepresentations(<br/>history_repr_request)
            F-->>P: results[*].value.semantic_id，按10项历史原顺序
            P->>G: Recommend(generation_request)，history=[{value:SID}]
            G->>G: generated_sids=[[21,31,41,51],[22,32,42,52]]
            G->>F: BatchGetItemRepresentations(<br/>generated_sid_lookup_request)
            F-->>G: results[*].item_ids=[["4","1201"],["9002"]]
            G-->>P: recommendations[*].item_id=[4,1201,9002]
            P->>P: recommendations转为generative_response.items，ID转字符串
        end
        P->>P: candidate_ids=<br/>["4","1201","9002","9001","9003"]
        P->>F: BatchGetItemFeatures(item_request)
        F-->>P: results：前4项FOUND；9003为NOT_FOUND
        P->>P: rankable_ids=["4","1201","9002","9001"]
    and 历史计算；数据来自现有Provider
        P->>P: GetUserHistory("1")，本例另行设定返回同10项历史
        P->>H: POST /ingest {user_id:"1",<br/>item_ids:[2,3,...,1202],timestamps:[0,...,9]}<br/>timestamps为序号占位，非事件时间
        H-->>P: ingest_response {accepted:true,checksum,reason:""}
    end
    P->>P: require(ingest_response.accepted)；候选与历史两路汇合
    P->>R: POST /rank，user_id="1"，items[*].item_id=rankable_ids
    R-->>P: items[*].score=[0.72,0.86,0.63,0.91]；trace.kv_hit=true
    P->>P: 重排：按score降序，items[*].item_id=["9001","1201","4","9002"]
    P-->>N: final_response {returned_size:4,shortfall:true,items}
    N-->>U: 同一推荐JSON响应
```

主图采用目标编排：检查 `/ingest.accepted` 后才提交 `/rank`。当前配套调用方则先尝试 `/rank`，未命中才等待历史任务并重查；两者区别见[现有 OneTrans 进程行为](02_onetrans.md)。

### 2.1 图中的字段从哪里来

| 下游请求字段 | 精确赋值来源 | 本例值或形状 |
|---|---|---|
| `user_request.user_id` | `external_request.uid` | `"1"` |
| `vector_request.query_vector` | `user_context.dense_query.vector` | `[0.125,-0.125]` 重复 32 次，64 维 |
| `vector_request.embedding_space_id` | `user_context.dense_query.embedding_space_id` | `"demo_dssm_64_v1"` |
| `sparse_request.sparse_tokens` | `user_context.sparse_query.tokens` | 两项 `{token,weight}` |
| `sparse_request.sparse_recipe_id` | `user_context.sparse_query.sparse_recipe_id` | `"video_type_binary_v1"` |
| `history_repr_request.item_ids` | `user_context.history.item_ids` | 10 个历史 ID |
| `generation_request.history[i].value` | `history_repr_response.results[i].value.semantic_id` | 一个物品的 4 个整数 |
| `generated_sid_lookup_request.semantic_ids` | 生成模型输出解析出的 `generated_sids` | 两个完整 SID |
| `item_request.item_ids` | PaiRec 召回合并、去重和已看过滤后的 `candidate_ids` | 5 个候选 ID |
| `rank_request.items[i].item_id` | 特征存在且通过资格规则的 `rankable_ids[i]` | 4 个候选 ID |
| `ingest_request.item_ids` | **现有历史 Provider** 的返回值 | 整数数组；不从特征服务响应赋值 |
| `ingest_request.timestamps` | 调用方生成 `list(range(len(provider_history_ids)))` | `0..9`；与物品逐项对应，不是点击时间 |
| `*.topk` | `scene` 中对应一路的配置 | 向量 50、稀疏 50、生成 10 |

新特征接口携带 `request_id/release_id/schema_version/remaining_timeout_ms`；本例召回目标封装复用此调用头，并携带本路表示版本。**当前生成 protobuf 没有完整版本头**，需新增字段或明确的服务绑定；当前 OneTrans HTTP 也不接受这套特征版本头。不能将公共字段无条件塞进所有旧接口。

## 3. 用户特征：从 Redis 值到召回输入

`required_views` 指接口中要查询的数据类别：用户属性 `USER`、历史 `HISTORY`、向量输入 `DENSE_QUERY` 和词项输入 `SPARSE_QUERY`。后两项由离线任务预先计算，在线只读取并检查版本。这里的“视图”与 4+1 架构视图无关。

下面先给出请求，再给出用户 1 的响应。`positions=1..10` 表示历史次序，不是事件时间；`history_hash` 是按约定格式计算的历史摘要，用于检查预计算向量、词项是否对应同一份历史。`source_data_row` 标记来源文件行，便于追溯。

```python
user_request = {
    **feature_meta(), "user_id": user_id,
    "required_views": ["USER", "HISTORY", "DENSE_QUERY", "SPARSE_QUERY"],
    "representation_versions": {k: versions[k] for k in
                                ("embedding_space_id", "sparse_recipe_id")},
}
history_ids = ["2","3","80936","781","111774","1230","26403","991","2362","1202"]
history = {**record_header, "user_id": "1", "item_ids": history_ids,
           "positions": list(range(1, 11)), "valid_length": 10,
           "time_semantics": "ordinal", "source_data_row": 1,
           "history_hash": "e00b29132a8ba7bd36e4a7a8296f5d579a139832cc58ba64047518e1b5424177"}
# user_context为目标响应；[0.125,-0.125]*32重复32次，组成64维教学向量。
user_context = {
    **record_header, "request_id": request_id,
    "view_status": {v: "FOUND" for v in user_request["required_views"]},
    "user": {**record_header, "user_id": "1", "gender_code": 1, "age_code": 4,
             "missing_fields": [], "source_data_row": 1},
    "history": history,
    "dense_query": {**record_header, "user_id": "1",
        "embedding_space_id": versions["embedding_space_id"],
        "history_hash": history["history_hash"], "dimension": 64,
        "vector": [0.125, -0.125] * 32},
    "sparse_query": {**record_header, "user_id": "1",
        "sparse_recipe_id": versions["sparse_recipe_id"],
        "history_hash": history["history_hash"],
        "tokens": [{"token":"video_type_0", "weight":2/9},
                   {"token":"video_type_1", "weight":7/9}],
        "known_history_count": 9, "missing_history_count": 1},
}
```

图法：UML 时序图。

```mermaid
sequenceDiagram
    participant P as PaiRec
    participant F as 特征服务
    participant R as Redis
    P->>F: GetUserContext(user_request)
    F->>R: MGET(user_key,history_key,dense_key,sparse_key)
    R-->>F: [user_json,history_json,dense_json,sparse_json]
    F->>F: decode；检查release、schema、history_hash和表示版本
    F-->>P: user_context，4个view_status均为FOUND
    P->>P: vector_request.query_vector = user_context.dense_query.vector
    P->>P: sparse_request.sparse_tokens = user_context.sparse_query.tokens
```

```python
# 特征服务内部拼键；PaiRec不依赖Redis键格式。
prefix = "rec:qkv:demo_tenrec_v1"
user_key    = prefix + ":user:1"
history_key = prefix + ":history:1"
dense_key   = prefix + ":user_rep:dense:demo_dssm_64_v1:1"
sparse_key  = prefix + ":user_rep:sparse:video_type_binary_v1:1"
```

Redis 的 `MGET` 按键顺序返回四个值，特征服务将其解析成 `user/history/dense_query/sparse_query`。这些是同一在线库中的四类记录；只有记录都为 `FOUND`，版本和历史摘要均匹配，才继续召回。`missing_fields=[]` 表示没有已知的字段缺失。

业务值按数据集编码解释：`age_code=4` 不是 4 岁，`video_type_0/1` 是视频长短类型而非主题。本例历史中 9 项类型已知，其中类型 0 有 2 项、类型 1 有 7 项，所以词项权重为 `2/9`、`7/9`。类型未知的物品仍保留在原始历史中。

## 4. 三路召回：同一上下文怎样形成不同请求

### 4.1 向量与稀疏请求

```python
# call_meta保持本次request_id、release_id不变，每次调用更新剩余时间。
vector_request = {
    **call_meta(5000), "query_vector": user_context["dense_query"]["vector"],
    "embedding_space_id": user_context["dense_query"]["embedding_space_id"],
    "topk": scene["vector_topk"],
}
sparse_request = {
    **call_meta(5000), "sparse_tokens": user_context["sparse_query"]["tokens"],
    "sparse_recipe_id": user_context["sparse_query"]["sparse_recipe_id"],
    "topk": scene["sparse_topk"],
}
```

以下是稀疏召回目标 RPC 的完整 JSON 表示。词项来自用户 1 的 `sparse_query.tokens`，因此无需再传 `user_id`；相同的 `request_id` 将它关联到原请求。此时只剩 900 毫秒，故 `call_meta(5000)` 展开后的超时为 `min(5000, 900)=900`。

PaiRec → 稀疏召回的完整请求：

```json
{
  "request_id": "demo-user-1-001",
  "release_id": "demo_tenrec_v1",
  "schema_version": "feature_v1",
  "remaining_timeout_ms": 900,
  "sparse_tokens": [
    {"token": "video_type_0", "weight": 0.2222222222222222},
    {"token": "video_type_1", "weight": 0.7777777777777778}
  ],
  "sparse_recipe_id": "video_type_binary_v1",
  "topk": 50
}
```

稀疏召回 → PaiRec 的完整响应。`score_semantics="bm25"` 表示 BM25 词项相关性评分，分数不是点击概率；本例只返回三个匹配物品，`topk=50` 不要求补足 50 个：

```json
{
  "request_id": "demo-user-1-001",
  "release_id": "demo_tenrec_v1",
  "schema_version": "feature_v1",
  "sparse_recipe_id": "video_type_binary_v1",
  "score_semantics": "bm25",
  "items": [
    {"item_id": "4", "score": 3.2},
    {"item_id": "9002", "score": 2.7},
    {"item_id": "9003", "score": 2.1}
  ]
}
```

图法：UML 时序图。两路召回并行查询各自索引，再把结果转成统一候选字段。

```mermaid
sequenceDiagram
    participant P as PaiRec
    participant V as 向量召回
    participant M as Milvus
    participant S as 稀疏召回
    participant O as OpenSearch
    par 向量检索
        P->>V: Recall(vector_request)
        V->>M: search(collection,data=[query_vector],limit=50,metric="IP")
        M-->>V: hits[{id,distance}]，例如{id:"1201",distance:0.92}
        V-->>P: vector_response.items[{item_id:"1201",score:0.92},...]
    and 词项检索
        P->>S: Recall(sparse_request)
        S->>O: search(index,body=make_query(sparse_tokens,50))
        O-->>S: hits.hits[{_source:{item_id:"4"},_score:3.2},...]
        S-->>P: sparse_response.items[{item_id:"4",score:3.2},...]
    end
```

上图只展示理解请求所需的数据库参数。`collection` 是 Milvus 中的物品向量集合，`index` 是 OpenSearch 中的物品词项索引；它们都由当前发布清单指定。`IP` 表示内积相似度；本例用户和物品向量都假定已单位化，故内积与余弦相似度一致。

`make_query` 是[稀疏服务](07_other_services.md)中的查询构造函数：为 `video_type_0/1` 各生成一个词项条件，用 `2/9`、`7/9` 作为权重，要求至少命中一个条件，最多返回 50 项。Milvus 的 `id/distance` 和 OpenSearch 的 `_source.item_id/_score` 都由召回服务转换为 `item_id/score`；转换统一字段名，保留各路分数含义。

### 4.2 历史原始 ID → SID → 生成候选原始 ID

`SID`（语义编码）是生成模型使用的物品表示，本例由四个整数组成。该分支先把历史物品 ID 转成 SID 供模型输入，再将新生成 SID 反查成原始物品 ID。`lookup_by` 分别取 `raw_item_id`、`semantic_id`，表示这两个查询方向。

先查历史编码：输入仍是用户 1 的十项历史，结果保持原顺序。

```python
history_ids = user_context["history"]["item_ids"]  # 沿用前面的10项，保留顺序
history_repr_request = {
    **feature_meta(), "lookup_by": "raw_item_id", "item_ids": history_ids,
    "sid_version": versions["sid_version"],
}
# 以下循环只生成教学响应，真实服务从物品编码表查询，不按序号计算SID。
history_results = []
for position, item_id in enumerate(history_ids, start=1):
    semantic_id = [position, 10+position, 20+position, 30+position]
    history_results.append({"item_id": item_id, "status": "FOUND", "value": {
        **record_header, "item_id": item_id, "sid_version": versions["sid_version"],
        "semantic_id": semantic_id}})
history_repr_response = {
    **record_header, "request_id": request_id, "lookup_by": "raw_item_id",
    "sid_version": versions["sid_version"],
    "results": history_results,
}
```

然后构造生成输入：从每项结果中取出编码，包装成协议要求的 `{value: [...]}`。第一项物品 `2` 变成 `{value:[1,11,21,31]}`，最后一项 `1202` 变成 `{value:[10,20,30,40]}`。

```python
generation_history = []
for result in history_repr_response["results"]:
    assert result["status"] == "FOUND"  # 任一历史编码缺失，生成分支失败
    generation_history.append({"value": result["value"]["semantic_id"]})
generation_request = {
    **call_meta(10000), "user_id": "1", "sid_version": versions["sid_version"],
    "history": generation_history,
    "topk": 10,
    "temperature": 1.0,  # 接口中的生成采样温度
    "beam_width": 1,     # 接口中的保留生成路径数
}
```

模型输出两个完整 SID，生成服务再查询它们对应哪些物品。反查接口用 `semantic_ids` 二维数组，不使用生成输入的 `{value: [...]}` 包装。下面是本例的输出与反查结果；具体参数不证明后端采用了哪种生成算法。

```python
generated_sids = [[21,31,41,51], [22,32,42,52]]
generated_sid_lookup_request = {
    **feature_meta(), "lookup_by": "semantic_id", "semantic_ids": generated_sids,
    "sid_version": versions["sid_version"],
}
generated_sid_lookup_response = {
    **record_header, "request_id": request_id, "lookup_by": "semantic_id",
    "sid_version": versions["sid_version"],
    "results": [
        {"semantic_id":[21,31,41,51], "status":"FOUND", "item_ids":["4","1201"]},
        {"semantic_id":[22,32,42,52], "status":"FOUND", "item_ids":["9002"]},
    ],
}
```

正查使用 `item_rep:<sid_version>:<item_id>`，反查使用 `sid_map:<sid_version>:<四位编码>`，二者都带第 3 节的 Redis 公共前缀。一个 SID 可以关联多个物品：本例第一个编码按数值 ID 升序展开为 `4,1201`，第二个得到 `9002`，形成三个候选，最终数量仍受 `topk` 限制。

```python
# 生成服务保留现有protobuf响应形状；省略计时/trace。
generation_wire_response = {"code": 200, "user_id": "1", "recommendations": [
    {"item_id": 4,    "semantic_id": [21,31,41,51], "score": 1.0},
    {"item_id": 1201, "semantic_id": [21,31,41,51], "score": 1.0},
    {"item_id": 9002, "semantic_id": [22,32,42,52], "score": 1.0},
]}
# PaiRec客户端做显式转换，后续召回合并才能统一使用items与字符串ID。
generative_response = {"items": [
    {"item_id": str(x["item_id"]), "score": x["score"]}
    for x in generation_wire_response["recommendations"]]}
```

目标生成服务新增特征反查；现有实现仍是本地映射。既有 protobuf 的 `history` 每项确为 `{value:[...]}`，返回确为 `recommendations`；目标版本头需要另外补齐。[生成协议源码](assets/source_snapshots/pairec4tigerllm/proto/recommend.proto.html#L7)。请求中的 `temperature/beam_width` 是协议字段；当前原生 TRT 后端使用启动配置建立采样参数，没有直接使用这两个请求值，不能以字段已发送证明配置已生效。请求 `topk` 则限制返回候选数，不限制此前枚举的编码组合数。

TensorRT-LLM 的模型缓存由实际块生命周期触发 DataSystem 读写，与上述业务映射查询分开。采样、组合枚举和缓存的执行边界见[生成服务视图](04_generative_recall.md)。

### 4.3 三路候选与召回合并

以下节选各路响应的 `items`，省略第 4.1 节已展示的公共响应头；候选数组完整，次序与各来源返回次序相同。

```python
vector_response = {"items": [
    {"item_id":"1201", "score":0.92}, {"item_id":"9001", "score":0.89},
    {"item_id":"3", "score":0.82},    {"item_id":"4", "score":0.81}]}
sparse_response = {"items": [
    {"item_id":"4", "score":3.2}, {"item_id":"9002", "score":2.7},
    {"item_id":"9003", "score":2.1}]}
# generative_response.items = [{item_id:"4",score:1.0}, {item_id:"1201",score:1.0},
#                              {item_id:"9002",score:1.0}]
candidate_ids = ["4", "1201", "9002", "9001", "9003"]
```

| 召回合并动作 | 本例变化 |
|---|---|
| 先取生成路，最多 10 个 | 得到 `4,1201,9002` |
| 稀疏、向量逐项轮询补充 | 重复 ID 不重复加入；新增 `9001,9003` |
| 按特征服务历史过滤已看 | `3` 已在用户历史中，排除 |
| 总候选最多 50 个 | 本例只有 5 个，不补造不足的候选 |

这里按来源优先级合并名单、去重并过滤已看物品，不使用精排分数。同一物品保留全部来源以便追溯；三路原始分数含义不同，不直接相加，生成路当前的常量 `1.0` 也不是点击概率。

## 5. 候选取特征、OneTrans 打分与返回

### 5.1 候选属性批查：保留缺失位置

```python
item_request = {**feature_meta(), "item_ids": candidate_ids,
    "fields": ["category_code", "metadata_available", "statistics", "missing_fields"]}
# 以下为item_response.results；省略公共响应头。Python True/None对应JSON true/null。
item_results = [
    {"item_id":"4", "status":"FOUND", "value":{"category_code":1,
     "metadata_available":True,"statistics":{"n":36,"ctr":1/6},"missing_fields":[]}},
    {"item_id":"1201", "status":"FOUND", "value":{"category_code":1,
     "metadata_available":True,"statistics":{"n":32,"ctr":0.25},"missing_fields":[]}},
    {"item_id":"9002", "status":"FOUND", "value":{"category_code":0,
     "metadata_available":True,"statistics":{"n":20,"ctr":0.10},"missing_fields":[]}},
    {"item_id":"9001", "status":"FOUND", "value":{"category_code":1,
     "metadata_available":True,"statistics":{"n":40,"ctr":0.20},"missing_fields":[]}},
    {"item_id":"9003", "status":"NOT_FOUND", "value":None},
]
# statistics在正文只显示n/ctr；完整教学对象见JSON附件。
rankable_ids = [r["item_id"] for r in item_results if r["status"] == "FOUND"]
# -> ["4", "1201", "9002", "9001"]；本例人工屏蔽集合为空。
```

特征服务批读 `prefix + ':item:' + item_id`，返回五个对应位置。`category_code` 仍是视频类型码，`metadata_available` 只表示该类型是否已知；`statistics.n` 是聚合曝光行数，`ctr` 是这些行的点击比例。它们是离线统计，不是本次精排分数。

PaiRec 用这些业务属性判断候选是否允许进入打分，必要时也用于重排。本例人工屏蔽名单为空，只排除整条特征缺失的 `9003`，候选从 5 个变为 4 个。这些属性**不传进当前 OneTrans `/rank`**。

### 5.2 OneTrans 历史与候选计算：现有字段，教学输入

OneTrans 的现有历史 Provider 是配套调用方的取历史组件：优先使用消息流缓存的点击历史，缺失时查询本地用户文件。本例另行假定它返回下面十项历史，且 OneTrans 已装载的本地 TSV（制表符分隔文件）覆盖四个候选；特征服务查到了用户 1，并不能证明这两个独立来源也具备相同数据。

```python
provider_history_ids = [2,3,80936,781,111774,1230,26403,991,2362,1202]
ingest_request = {"user_id":"1", "item_ids":provider_history_ids,
                  "timestamps":list(range(10))}  # 调用方生成的0..9，占位序号
rank_request = {"request_id":request_id, "user_id":"1",
                "items":[{"item_id":i} for i in rankable_ids]}
```

`timestamps` 在当前 C++ `/ingest` 主链中是**为满足现有接口要求生成的等长序号数组，不是点击事件时间**。取到十项 `item_ids` 后，调用方生成 `0..9`：第一项物品 `2` 对应 `0`，最后一项 `1202` 对应 `9`。它不来自数据集时间列，也不从特征服务的 `positions` 字段赋值。[调用方构造](assets/source_snapshots/pairec4tigerllm_8506/services/recall/onetrans_s_stage.go.html#L82)。

服务端要求两个数组等长；否则返回 HTTP 400，错误为 `item_ids 与 timestamps 必须等长`，但没有时间递增校验。[接口解析](assets/source_snapshots/OneTrans_HSE_project/cpp/src/serving/json_io.cpp.html#L12)。历史前处理只使用 `item_ids` 构建输入，`timestamps` 未传入后续模型计算，也不参与排序、截断、掩码或位置编码。历史顺序来自 `item_ids` 数组；位置嵌入取补齐后的数组下标，本例十项占 50 位输入的 `40..49`。[历史编码](assets/source_snapshots/OneTrans_HSE_project/cpp/src/engine/frontend.cpp.html#L84)。

历史处理代码仅把末项序号记入 `seq_ts_last`，本例为 `9`。[历史处理实现](assets/source_snapshots/OneTrans_HSE_project/cpp/src/serving/pipeline.cpp.html#L103)。本地内存存储保留该记录字段；DataSystem 实际只存模型结果字节 `payload`，不保存 `seq_ts_last`。[DataSystem 写入](assets/source_snapshots/OneTrans_HSE_project/cpp/src/kv/datasystem_store.cpp.html#L46)。因此不能用 `timestamps` 判断历史新旧或证明读到了本次历史。

下图的模型参数服务 `PS` 按模型表名和 ID 返回模型向量参数；`history_kv` 是历史 Transformer 计算产生的注意力键、值张量，存入 DataSystem 供候选计算复用。它不是原始历史，也不是特征服务的 key/value 记录。图中的计算函数只描述业务动作，不代表对外 RPC 接口。

图法：UML 时序图。

```mermaid
sequenceDiagram
    participant P as PaiRec
    participant H as OneTrans历史
    participant R as OneTrans精排
    participant PS as 模型参数服务
    participant D as DataSystem
    Note over H,R: 启动装载本地TSV与模型；两进程连接同一参数表和存储
    P->>H: POST /ingest(ingest_request)，user_id="1"
    H->>PS: Lookup({table:model_version+"/item",<br/>ids:[0]*40+provider_history_ids})
    PS-->>H: {dim:128,ids:[...],weights:6400个float}
    H->>H: history_kv = encode_and_run_history(weights)
    H->>D: Set(kv_key,serialize(history_kv))
    D-->>H: 写入结果
    H-->>P: {accepted:true,checksum:SHA256字符串,reason:"",shard:0}
    P->>R: POST /rank(rank_request)，items[*].item_id=["4","1201","9002","9001"]
    R->>R: assemble(user_id,items)：从已装载TSV查询与补齐字段
    R->>PS: Lookup({table,ids})；user/item/artist/album四组参数
    PS-->>R: {dim:128,ids,weights}；weights为展平数组
    R->>R: 编码用户和候选输入
    R->>D: Get(kv_key)
    D-->>R: serialize(history_kv)
    R->>R: logits = run_candidates(history_kv)；score = sigmoid(logits[:,0])
    R-->>P: items按输入顺序给score=[0.72,0.86,0.63,0.91]，trace.kv_hit=true
```

```python
# 当前OneTrans装配方式；本例本地用户TSV另行设定gender=1、age=4。
user_dense = [1.0, 4.0] + [0.0] * 13
# 每个候选：artist_ids与album_ids均取物品TSV第2列；dense取第4～18列。
# 字段顺序和缺行补零见02，不使用item_results中的statistics替代。
model_version = "demo_onetrans_v1"
kv_key = "kv:" + base64url_no_padding(model_version) + ":MQ"  # MQ是用户"1"的编码
# 参数服务响应weights为展平数组：len(weights) == len(ids) * dim。
```

历史最多输入 50 项，本例只有 10 项，所以在左侧补 40 个零 ID，再查询 `50 × 128 = 6400` 个参数值；“展平数组”指把 50 行向量依次接成一个数组。模型用有效长度排除补空位。候选计算中的 `artist/album` 是沿用的参数槽名称，当前都使用物品 TSV 的视频类型值，不表示歌手或专辑。

`base64url_no_padding` 表示去掉末尾填充符的 URL 安全 Base64 编码；`kv_key` 因此由模型版本和用户 ID 两部分组成。候选参数查询发生在历史 KV 读取之前。

`/ingest` 成功响应有 `accepted/shard/checksum/reason/ingest_us`；`checksum` 是计算结果字节的 SHA256 字符串，不是原始历史的 `history_hash`；`shard` 是服务分片编号，`ingest_us` 是处理耗时，单位为微秒。现有 `/rank` 成功响应如下，省略耗时与部分 trace 字段：

```json
{
  "code": 200, "msg": "ok", "request_id": "demo-user-1-001",
  "model_version": "demo_onetrans_v1", "model_role": "engineering",
  "items": [
    {"item_id":"4", "score":0.72},
    {"item_id":"1201", "score":0.86},
    {"item_id":"9002", "score":0.63},
    {"item_id":"9001", "score":0.91}
  ],
  "trace": {"kv_hit":true,"n_candidates":4}
}
```

`logit` 是模型输出头的原始数值；`sigmoid(x)=1/(1+exp(-x))` 将它转换到 0～1。`score` 取第一个输出头的转换结果，不自动代表点击概率；精排响应保持输入顺序，**重排由 PaiRec 执行**。`model_role` 是配置值，此处沿用源码默认值。现有 OneTrans 的缓存键只含模型与用户，`accepted=true`、`kv_hit=true` 不能独自证明并发时读到的是本次历史；这个教学成功路径假定用户 1 的请求串行处理。

### 5.3 重排与最终响应

```python
# rank_response为上面的响应；人工屏蔽规则为空，无其他业务加分。
score_by_id = {x["item_id"]: x["score"] for x in rank_response["items"]}
# Python稳定排序：按分数降序；同分时保留rankable_ids中的召回合并次序。
final_ids = sorted(rankable_ids, key=lambda item_id: -score_by_id[item_id])[:scene["size"]]
# -> ["9001", "1201", "4", "9002"]
```

```json
{
  "request_id":"demo-user-1-001", "release_id":"demo_tenrec_v1",
  "items":[
    {"item_id":"9001","score":0.91},
    {"item_id":"1201","score":0.86},
    {"item_id":"4","score":0.72},
    {"item_id":"9002","score":0.63}
  ],
  "returned_size":4, "shortfall":true
}
```

`returned_size` 是实际返回数，`shortfall=true` 表示少于请求的 `size`。客户端请求 10 项，本例实际返回 4 项：三路候选经召回合并得到 5 项，缺失特征的 `9003` 被排除，其余 4 项全部打分后按分数返回。Nginx 转发这一响应，不再次排序。

## 6. 读图时需要检查的四个分支

前文演示成功路径。下表补充缺失与失败的处理；“严格”表示不能把没有实际完成的必需计算记为端到端成功。可选召回路失败后是否继续，由场景策略明确配置，不能由空数组暗示成功。

| 条件 | 此次请求应怎样结束 |
|---|---|
| 必需用户视图缺失、版本不匹配；历史 SID 任一缺失 | 停止相关必需阶段，明确失败；不静默替换用户历史 |
| 生成的 SID 没有物品关联 | 记录该 SID，不产生候选；可返回真实空候选 |
| 某个候选整条物品特征缺失 | 保留结果位置并由 PaiRec 剔除；本例 `9003` 就是此分支 |
| 当前 OneTrans 历史为空、写入失败或 KV 未命中 | 目标严格编排不算完整成功；当前未命中可能不执行候选模型，零 logit 经 sigmoid 后得到统一 0.5，需检查 `kv_hit` 和计算记录 |

接口依据：[特征完整类型与缺失语义](10_feature_catalog.md)、[历史输入构造](assets/source_snapshots/pairec4tigerllm_8506/services/recall/onetrans_s_stage.go.html#L72)、[OneTrans 路由与响应](assets/source_snapshots/OneTrans_HSE_project/cpp/tools/server_main.cpp.html#L234)、[PS 查询协议](assets/source_snapshots/OneTrans_HSE_project/deploy/ps/embedding_service.proto.html#L18)、[历史补齐与参数查询](assets/source_snapshots/OneTrans_HSE_project/cpp/src/engine/frontend.cpp.html#L84)。
