# 向量召回、稀疏召回与 Nginx：各模块 4+1 视图

图中符号与连线遵循[图法与 UML 约定](DIAGRAM_NOTATION.md)，每张图前注明使用的图法。

以下是目标系统的第三优先级模块视图。每个模块独立给出逻辑、开发、进程、物理、场景五个视图；本轮业务取数以独立特征服务为准，覆盖旧版由 PaiRec 直接查询 Redis 的决定。字段字典见[特征数据库与接口](10_feature_catalog.md)。特征读写见[特征与数据视图](05_feature_data.md)，生成 KV 见[生成式召回视图](04_generative_recall.md)。

下文画出“服务桥”的图展开的是可选 HTTP 适配方案：桥将原生 bRPC 请求转交 HTTP 后端。若召回程序内置原生 bRPC 处理器，则省去桥进程，对应[系统物理视图](01_system.md)的目标分组。采用适配方案也须补齐后端的 `query_vector` 或 `sparse_tokens` 契约，不能只加桥而沿用旧输入；桥与后端的容器分组及主机位置尚未确定。

## 1. 向量召回：DSSM 离线向量与 Milvus

### 1.1 逻辑视图

图法：结构或流程示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    A[查询向量校验] --> B[物品相似向量检索]
    B --> C[候选ID与分数还原]
```

```yaml
input:
  request_id: string
  release_id: string
  embedding_space_id: string
  query_vector: 实际DSSM用户塔离线输出的有限浮点数组
  topk: 有上限的正整数
output:
  items: "[{item_id: string, score: finite_number}]"
  source: milvus
  executed: true
  versions: release_id与embedding_space_id
offline_state:
  user_vectors: 特征服务user_rep:dense，PaiRec每请求查询后传入
  item_vectors: Milvus常驻collection与索引
```

DSSM 表示用户塔和物品塔映射到同一向量空间的双塔模型。首期用户状态固定，用户塔移到离线；线上检索服务直接接收 `query_vector`，不再按 uid 读取本地画像或运行用户塔。

### 1.2 开发视图

图法：模块关系示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    A[双塔向量导出器] -->|向量文件| B[Milvus装载器]
    C[向量服务处理器] -->|进程内调用| D[查询向量校验器]
    C -->|进程内调用| E[Milvus检索客户端]
    F[向量bRPC服务桥：可选] -->|HTTP调用| C
```

```python
# 目标离线导出：同一checkpoint、词表、输入配方、输出维度与归一化规则。
user_vectors = dssm.user_tower(user_ids, gender_codes, age_codes, histories)
item_vectors = dssm.item_tower(item_ids, video_type_codes)
user_vectors = normalize_l2(user_vectors)
item_vectors = normalize_l2(item_vectors)
write_user_vectors_to_release(user_raw_ids, user_vectors, embedding_space_id)
write_item_vectors_to_release(item_raw_ids, item_vectors, embedding_space_id)
```

```python
# 目标在线处理；query_vector已经由PaiRec通过GetUserContext查询取得。
def recall(request, manifest, milvus):
    validate_release_and_vector_space(request, manifest)
    validate_finite_dimension_and_norm(request.query_vector, manifest)
    hits = milvus.search(
        collection=manifest.collection,
        vector=request.query_vector,
        limit=request.topk,
        metric=manifest.metric,
    )
    return real_candidates(hits, source="milvus", executed=True)
```

现有[导出器](assets/source_snapshots/pairec4tigerllm/training/dssm/export_embeddings.py.html#L75)只导出物品向量和用户画像；[在线服务](assets/source_snapshots/pairec4tigerllm/inference/dssm_recall_server.py.html#L80)仍读本地画像运行用户塔；[Milvus 装载器](assets/source_snapshots/pairec4tigerllm/scripts/load_item_embeddings_to_milvus.py.html#L43)已有建表、插入、建索引和自查询。因此是复用计算/灌库基础并补用户导出与接口，不是把现有系统认定为已满足目标。

### 1.3 进程视图

图法：UML 时序图。

```mermaid
sequenceDiagram
    participant P as PaiRec
    participant B as 向量服务桥
    participant V as 向量服务
    participant M as Milvus
    P->>B: 原生bRPC：实际用户向量、空间版本、topk=50
    B->>V: HTTP /recall：相同字段
    V->>V: 校验release、维度、有限值、归一化规则
    V->>M: 固定collection的向量search
    M-->>V: 原始item_id、相似度分数
    V-->>B: items、source=milvus、executed、版本
    B-->>P: 类型化向量召回结果
```

```text
Milvus不可用 → 明确错误
不能静默改成本地numpy矩阵检索后仍宣布Milvus已执行
返回原始ID → 统一融合 → 候选产生后通过BatchGetItemFeatures读取属性
```

### 1.4 物理视图

图法：部署映射示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    P[PaiRec进程] -->|原生bRPC| B[向量服务桥进程：可选]
    B -->|HTTP| V[向量服务进程]
    V -->|Milvus SDK| M[(Milvus服务)]
    L[离线向量装载进程] --> M
    M --- D[(索引与数据持久化)]
```

```yaml
deployment:
  query_gpu_required: false
  collection: 按release版本化，启动时固定
  dimension: 从真实checkpoint读取，代码默认64不等于所有模型必须64
  score: 归一化向量内积；只有双方归一化时等价余弦相似度
  reload: 不向同名已发布collection反复insert；新版本装载验证后切换
  readiness: 数据数量、索引已加载、空间匹配、真实search可返回合法ID
```

### 1.5 场景视图

图法：结构或流程示意图（非 UML，连线含义见本节）。

```mermaid
flowchart TD
    A[用户1通过特征服务取得的用户向量] --> B{存在真实用户向量且空间版本匹配}
    B -->|否| C[特征或发布失败]
    B -->|是| D[以topk 50查询Milvus]
    D --> E[返回实际数量的候选ID和相似度]
    E --> F[交给统一候选融合，不与其他召回原分直接相加]
```

第 08 篇的完整向量是教学值；本地尚未核实样例用户的真实向量产物，[当前接口附件](assets/README.md)需要从发布资产补齐后才能发送。验收应证明离线与参考用户塔在同输入下数值一致、真实 Milvus 被调用、ID 可连接到特征服务的物品目录；训练准确率不阻塞工程贯通。

## 2. 稀疏召回：真实视频类型词项与 OpenSearch BM25

### 2.1 逻辑视图

图法：结构或流程示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    A[用户类型词项校验] --> B[词项匹配与BM25计分]
    B --> C[候选ID与分数还原]
```

```yaml
input:
  request_id: string
  release_id: string
  sparse_recipe_id: video_type_binary_v1
  sparse_tokens: "[{token: video_type_0或video_type_1, weight: 非负有限值}]"
  topk: 有上限的正整数
output:
  items: "[{item_id: string, score: BM25分数}]"
  source: opensearch
  executed: true
  versions: release_id与sparse_recipe_id
offline_state:
  user_tokens: 特征服务user_rep:sparse，绑定同一历史内容校验值
  item_document: 原始item_id和实际类型词项
  index: 版本化mapping与倒排索引
```

BM25 是按词项在文档中的出现情况及文档分布计算匹配分数的方法。本场景只有 `video_type_0/1` 两个词项，语义为长短视频类型；它不是主题兴趣分类或自然语言搜索。类型未知的物品不生成假词项。

### 2.2 开发视图

图法：模块关系示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    A[真实物品文档构建器] -->|物品文档文件| B[索引mapping与bulk装载器]
    C[稀疏服务处理器] -->|进程内调用| D[查询构造器]
    D -->|查询请求对象| E[OpenSearch客户端]
    F[稀疏bRPC服务桥：可选] -->|HTTP调用| C
```

```json
{
  "mappings": {
    "properties": {
      "item_id": {"type": "keyword"},
      "tokens": {"type": "text", "analyzer": "whitespace", "similarity": "BM25"}
    }
  }
}
```

目标 mapping 使用 text 的词项分析与 BM25，不靠 keyword 默认评分猜测行为；部署时锁定 OpenSearch 版本与 mapping hash，并对固定样本执行评分解释。[OpenSearch text 官方说明](https://docs.opensearch.org/latest/mappings/supported-field-types/text/)。

```python
def make_document(item):
    if item.category_code is None:
        return None  # 保留缺失覆盖报告，不变成video_type_0
    assert item.category_code in (0, 1)
    return {"item_id": item.raw_id, "tokens": f"video_type_{item.category_code}"}

def make_query(tokens, topk):
    terms = merge_duplicates_and_validate(tokens)
    if not terms:
        return EmptyResult(reason="NO_KNOWN_VIDEO_TYPE")
    return {
        "size": topk,
        "query": {"bool": {
            "should": [{"term": {"tokens": {"value": t.token, "boost": t.weight}}}
                       for t in terms],
            "minimum_should_match": 1,
        }},
        "sort": [{"_score": "desc"}, {"item_id": "asc"}],
    }
```

补充 `_score` 相同后的 `item_id` 次序用于结果稳定性；这个 keyword 次序是字符串词典序，不当作数值大小比较。词项条数/长度和 topk 有界，索引从服务启动 manifest 固定，不允许请求自行选索引。返回只需候选字段，不把完整物品属性塞进 `_source` 替代候选产生后的特征查询。

**当前差距与旧审阅不同：** 当前[Go 实现](assets/source_snapshots/pairec_sh/pairec-demo/src/recall/brpc_sparse_recall.go.html#L65)取 `video_category` 字符串，[C++ 客户端](assets/source_snapshots/pairec_sh/pairec-demo/src/cpp/brpcClients/opensearch_client.cpp.html#L86)拼成 `match.content`；源码注释中的加权 sparse_tokens 已不能描述这条当前执行路径。目标仍需独立稀疏服务、结构化查询、真实文档与 mapping/bulk 装载，不能照抄旧“客户端已等于完整服务”的结论。

### 2.3 进程视图

图法：UML 时序图。

```mermaid
sequenceDiagram
    participant P as PaiRec
    participant B as 稀疏服务桥
    participant S as 稀疏服务
    participant O as OpenSearch
    P->>B: 原生bRPC：类型词项和权重、release、topk=50
    B->>S: HTTP /recall：相同字段
    S->>S: 校验词项、合并重复、绑定固定index
    S->>O: HTTP search：term加权查询和BM25排序
    O-->>S: hits、score、分片执行状态
    S->>S: 检查超时与分片失败，恢复原始ID
    S-->>B: items、source=opensearch、executed、版本
    B-->>P: 类型化稀疏召回结果
```

真实空词项可明确返回 `empty`，并标记此次未查询索引；不能同时声称 `executed=true` 代表完成了 OpenSearch 查询。真实查询后零命中与后端错误是不同结果。

### 2.4 物理视图

图法：部署映射示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    P[PaiRec进程] -->|原生bRPC| B[稀疏服务桥进程：可选]
    B -->|HTTP| S[稀疏服务进程]
    S -->|OpenSearch HTTP| O[(OpenSearch服务)]
    L[离线文档装载进程] -->|mapping与bulk| O
    O --- D[(索引持久化卷)]
```

```yaml
readiness:
  mapping: text + whitespace + BM25，与manifest一致
  bulk: 检查每条action的错误，不能只看HTTP 200
  refresh: 装载后完成refresh并抽样search
  coverage: 已知类型文档数、未知类型排除数、raw ID关联率
  query_validation: 固定样本explain与实际词项分布
```

### 2.5 场景视图

图法：结构或流程示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    A[用户1的10项固定历史] --> B[其中9项类型在样例目录可查]
    B --> C[类型0权重2除9，类型1权重7除9]
    C --> D[真实OpenSearch查询]
    D --> E[返回真实候选，保留大量同分的可能]
```

```json
{
  "sparse_tokens": [
    {"token": "video_type_0", "weight": 0.2222222222222222},
    {"token": "video_type_1", "weight": 0.7777777777777778}
  ],
  "topk": 50
}
```

完整请求见[特征服务真实数据样例](10_feature_catalog.md)。本配方只有两种词项且每文档一个词项，倒排长、同分多，是数据本身决定的负载特征。验收记录词表大小、每文档长度、每查询词数、命中数和同分率，不用虚构标题改善分布。

## 3. Nginx 统一推荐入口

### 3.1 逻辑视图

图法：结构或流程示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    A[推荐请求接入] --> B[入口格式与大小限制]
    B --> C[转发到可用PaiRec实例]
    C --> D[返回推荐响应与访问记录]
```

```yaml
public_business_route: POST /api/recommend
request_body:
  uid: 十进制字符串
  scene_id: home_feed
  size: 目标返回数量
  request_id: 可选外部关联ID；不直接作为模型临时KV唯一键
response_body: PaiRec实际响应，包含最终物品列表与实际返回数
request_identity:
  gateway_request_id: Nginx生成，关联入口日志
  internal_request_id: PaiRec生成并贯穿下游，关联特征和模型调用；当前OneTrans KV仍按模型与用户标识
gateway_responsibility: HTTP接入、转发、超时边界、访问记录
business_responsibility: PaiRec负责读特征、召回、排序和精排
```

### 3.2 开发视图

图法：模块关系示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    A[Nginx路由配置] --> B[PaiRec upstream配置]
    A --> C[请求头与超时配置]
    A --> D[访问日志配置]
    E[PaiRec自有控制器] --> F[请求校验与内部ID生成]
```

```nginx
# 目标配置片段；放在http块中，DNS、端口和容量值由部署清单替换。
upstream recommendation_api {
    server pairec:18080;
    keepalive 16;
}

server {
    listen 8080;
    client_max_body_size 8k;

    location = /api/recommend {
        proxy_pass http://recommendation_api;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Gateway-Request-ID $request_id;
        proxy_connect_timeout 1s;
        proxy_read_timeout 30s;
        proxy_send_timeout 30s;
        proxy_next_upstream off;
        proxy_cache off;
    }

    location / { return 404; }
}
```

入口片段不执行推荐策略、不启用结果缓存或自动上游重试。`proxy_read_timeout` 约束两次读取之间的等待，并不是整个推荐的总时限；实际业务总预算仍由 PaiRec 的 25 秒 deadline 控制。[Nginx 官方代理模块](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_read_timeout)。生产域名、TLS 证书与地址应在实际部署清单中配置；上面 `8080/pairec:18080` 是与场景模板一致的建议角色地址，不是已盘点地址。

### 3.3 进程视图

图法：UML 时序图。

```mermaid
sequenceDiagram
    participant U as 请求方
    participant N as Nginx进程
    participant P as PaiRec进程
    U->>N: POST /api/recommend：uid、scene_id、size
    N->>N: 生成gateway_request_id，检查请求边界
    N->>P: 原请求体和网关关联头
    P->>P: 校验业务字段，生成内部request_id和总deadline
    P->>P: 完整推荐编排
    P-->>N: 真实推荐列表或明确失败
    N-->>U: 原样返回业务响应和HTTP状态
```

```yaml
logging:
  nginx: gateway_request_id、HTTP状态、request_time、upstream_response_time、上游地址
  pairec: gateway_request_id与internal_request_id关联、业务阶段、返回数、失败阶段
failure_handling:
  unavailable_upstream: 返回可辨认的网关错误，不伪造推荐列表
  business_error: 保留实际业务错误语义
  client_disconnect: 向PaiRec传播取消信号；远端模型资源仍按生命周期收尾
```

### 3.4 物理视图

图法：部署映射示意图（非 UML，连线含义见本节）。

```mermaid
flowchart LR
    U[外部请求方] --> N[Nginx网关部署单元]
    N --> P1[PaiRec实例1]
    N --> P2[PaiRec实例2，可选]
```

```yaml
deployment:
  public_entry: Nginx统一对外
  internal_entry: PaiRec仅作为网关上游
  replica_policy: 首条链路一个PaiRec实例即可；容量需要时增加
  publish_order: 数据READY → 模型和存储ready → PaiRecready → 网关上游接流量
  rollback: 切回已验证PaiRec配置或release，排空旧请求
  health: 网关存活与推荐依赖就绪分开检查
```

Nginx 与下游 bRPC 服务同处系统物理架构，但不在这张入口图中展开模型内部依赖，以保持单一层级。

### 3.5 场景视图

图法：结构或流程示意图（非 UML，连线含义见本节）。

```mermaid
flowchart TD
    A[用户1请求10项home_feed] --> B[Nginx唯一业务入口]
    B --> C{PaiRec可用且请求合法}
    C -->|是| D[PaiRec执行完整推荐]
    D --> E[返回实际Top-K，可能不足10项]
    C -->|否| F[返回错误，不内部复制请求]
```

```json
{"uid":"1","scene_id":"home_feed","size":10}
```

验收最少包含：由网关发入的一条真实推荐请求、上游不可用、请求超限、总预算耗尽、客户端取消；每次能关联到真实 PaiRec 请求，正常路径只有一次业务转发。当前工作区没有已验证的业务 Nginx 部署交付，本节补齐目标角色与配置要求，不声称已上线。
