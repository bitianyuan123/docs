# 特征数据库与接口字典

本文件回答三个具体问题：**要保存什么数据；用几类存储、怎样设计键和值；PaiRec 和子服务何时查询。** 服务的五个视图见 [特征服务文档](05_feature_data.md)。以下是目标接口与数据设计，不能视为当前数据库已经装载完毕。

## 1. 需要几类数据库

```yaml
最小配置:
  在线特征数据库:
    产品: Redis
    部署: 一套实例或集群，按容量决定
    数据: 用户、物品、历史、派生表示、发布元数据
  离线持久存储:
    产品: 文件卷或对象存储
    数据: 原始数据、清洗产物、特征快照、可重放装载文件、发布清单
  单独关系数据库: 首期不需要
  单独时序数据库: 首期不需要
  每种特征独立数据库: 不需要
```

这是 **两类存储、一类在线特征数据库**。四类核心业务数据是逻辑划分；它们共用 Redis，按键前缀区分。发布清单既保存在离线产物中，也可以在同一 Redis 保存可查询副本，不必因此部署新库。

| 系统中还存在的存储 | 保存什么 | 是否计入这里的在线特征库 |
|---|---|---|
| Milvus | 物品向量、原始 ID、检索所需字段 | 否，是向量检索索引 |
| OpenSearch | 物品 ID、用于 BM25 的词项和检索字段 | 否，是稀疏检索索引 |
| 模型参数服务 | 模型 user/item/type embedding 参数等 | 否，是模型参数 |
| DataSystem | 模型执行产生的注意力键、值张量 | 否，是计算状态 |
| OneTrans 本地 TSV | 当前代码装载的用户和物品业务特征 | 是本轮保留的独立数据路径；不是新特征服务的数据库 |

已有的向量、稀疏索引仍由离线流程全量装载。在线只有查询向量、兴趣词项和必要的候选信息经过特征服务，不逐请求返回整张物品表。

## 2. 键和值的总表

`<release>` 代表一次不可变数据发布；`<uid>`、`<iid>` 是原始十进制字符串 ID。`<embedding_space_id>` 绑定双塔模型及向量空间，`<sparse_recipe_id>` 绑定词项配方，`<sid_version>` 绑定物品语义编码版本。尖括号是格式占位符，不是可以直接写入生产库的内容。前缀 `rec:qkv` 是本项目为这份 Tenrec 视频数据固定的推荐数据命名空间；它是普通字符串，不表示 Transformer 的查询、键、值张量。

| 逻辑集合与键示例 | value 的业务内容 | 生产者 | 直接读取者 / 最终使用方 |
|---|---|---|---|
| 用户画像：`rec:qkv:<release>:user:1` | `user_id, gender_code, age_code, missing_fields` | 原始数据清洗，按固定规则选择用户源行 | 特征服务 / PaiRec 及需要用户属性的计算服务 |
| 用户历史：`rec:qkv:<release>:history:1` | `item_ids[], positions[], valid_length, history_hash, time_semantics` | 与用户画像相同的源行规则 | 特征服务 / PaiRec、生成召回；需要时转交其他服务 |
| 物品画像与统计：`rec:qkv:<release>:item:4` | `item_id, category_code, statistics, valid_counts, missing_counts, metadata_available` | 物品目录构建与交互聚合 | 特征服务 / PaiRec 候选检查、人工规则、后排序 |
| 用户向量：`rec:qkv:<release>:user_rep:dense:<embedding_space_id>:1` | `user_id, vector[], dimension, embedding_space_id, history_hash` | 固定 DSSM 用户塔处理同版本用户与历史 | 特征服务 / PaiRec 转交向量召回 |
| 用户兴趣词项：`rec:qkv:<release>:user_rep:sparse:<sparse_recipe_id>:1` | `user_id, tokens[], known_history_count, missing_history_count, history_hash` | 历史关联物品类型，按已冻结配方聚合 | 特征服务 / PaiRec 转交稀疏召回 |
| 物品语义编码：`rec:qkv:<release>:item_rep:<sid_version>:2` | `item_id, semantic_id[], sid_version` | 固定模型编码流程 | 特征服务 / PaiRec 转交生成召回 |
| SID 反向关联：`rec:qkv:<release>:sid_map:<sid_version>:<canonical_sid>` | `semantic_id[], item_ids[], sid_version` | 从同一物品编码表反向分组产生 | 特征服务 / 生成召回把输出编码转换为原始 ID |
| 发布清单：`rec:qkv:<release>:release` | 状态、字段版本、源数据范围、各表示版本、索引名、数量与覆盖信息 | 离线发布进程 | 特征服务、PaiRec 启动与发布管理 |

上述两种 `user_rep` 同属用户派生表示，`item_rep` 与 `sid_map` 是同一派生关系的两个查询方向。因此核心业务集合仍为用户、物品、历史、派生表示四类，而不是表里有几行就部署几个数据库。

```typescript
// 首期统一用Redis String保存UTF-8 JSON；这里表达字段类型，不是模型输入张量。
type RecordHeader = {
  release_id: string;
  schema_version: "feature_v1";
};
type RawId = string;                    // 无损十进制字符串，不通过浮点数转换
type MissingFields = string[];         // 例如["gender_code"]
type Counts = Record<string, number>;  // 非负整数

// 每组业务value包含RecordHeader，并包含下文对应的业务字段。
// keys不设逐键TTL；release停用且旧请求结束后统一回收。
// 数据体出现非法数值、错误ID、错误版本：读取失败，不能作为“缺键”返回。
```

### 2.1 哪些数据首期不入库

| 信息 | 理想推荐系统如何处理 | 当前 Tenrec 构筑方式 |
|---|---|---|
| 请求场景、请求数量、实时设备信息 | 请求携带，PaiRec 解析后向需要的服务传递 | 场景和数量来自请求；不存在的设备信息不补造 |
| 用户长期地域、会员等级、物品库存/价格 | 有真实上游系统时作为画像或业务状态纳入特征服务 | 当前源数据没有这些字段，不增加虚构值 |
| 用户与物品交叉计数 | 需要时增加 `user_item:<uid>:<iid>` 或按用户分组存储 | 未纳入首期，避免为所有用户物品组合生成空记录 |
| 实时窗口统计 | 有真实事件流时按窗口聚合，保留事件时间和更新时间 | Tenrec 现有源文件没有事件时间，不声称具备小时级统计 |
| 模型权重、tokenizer、codebook | 模型发布和装载流程管理 | 不放进业务特征查询接口 |

## 3. 用户画像与行为历史

### 3.1 用户画像 `user`

```typescript
type UserProfile = RecordHeader & {
  user_id: RawId;
  gender_code: number | null;   // 原始编码，不为编码赋未经核实的文字标签
  age_code: number | null;      // 原始年龄分组编码，不是实际年龄
  missing_fields: MissingFields;
  source_data_row: number;      // 在本release声明源文件中的数据行号
};
```

用户 `1` 的真实业务字段如下；省略通用发布头，因为这里只展示前 20 万行证据中的值，尚未形成完整可发布的 release。

```json
{
  "user_id": "1",
  "gender_code": 1,
  "age_code": 4,
  "missing_fields": [],
  "source_data_row": 1
}
```

生产规则是按声明的数据范围、文件顺序选择该用户最早源行，同时固定历史。源数据的 `\N` 转成 `null` 并登记缺失；合法编码 `0` 仍是有效值。某个属性未知不等于用户不存在。

### 3.2 用户历史 `history`

```typescript
type UserHistory = RecordHeader & {
  user_id: RawId;
  item_ids: RawId[];
  positions: number[];           // 从1开始的序列位置，与item_ids一一对应
  valid_length: number;
  time_semantics: "ordinal";    // 序列位置，不是Unix时间
  history_hash: string;          // 固定编码规则产生的SHA256
  source_data_row: number;
};
```

```json
{
  "user_id": "1",
  "item_ids": ["2","3","80936","781","111774","1230","26403","991","2362","1202"],
  "positions": [1,2,3,4,5,6,7,8,9,10],
  "valid_length": 10,
  "time_semantics": "ordinal",
  "source_data_row": 1
}
```

上例只省略按下面规则计算的 `history_hash` 与发布头，不使用编造的哈希值。

```python
def normalize_history(hist_columns):
    # 保留原列顺序和重复物品；只去掉padding 0。
    ids = [checked_raw_id(x) for x in hist_columns if x != "0"]
    return {
        "item_ids": ids,
        "positions": list(range(1, len(ids) + 1)),
        "time_semantics": "ordinal",
    }

history_hash = sha256(json.dumps(
    normalized_history, sort_keys=True, separators=(",", ":")
).encode("utf-8")).hexdigest()
```

如果源列包含无法解释的值，应报告数据错误；不能把它当作 padding 静默删除。也不能仅因为物品类型未知就删掉历史物品。顺序表示数据集给定的历史次序，不据此虚构真实时间戳或时间间隔。

| 查询情况 | 返回方式 | 调用方处理 |
|---|---|---|
| 用户存在、历史存在且非空 | `FOUND` 与完整视图 | 正常召回 |
| 用户存在、历史存在且真实为空 | `FOUND`，`valid_length=0` | 进入已声明冷启动场景；不伪装成有历史模型计算 |
| 用户或历史整键缺失 | 对应视图 `NOT_FOUND` | 首期完整数据场景失败；有冷启动策略才使用该策略 |
| 用户派生表示绑定另一份历史 | `VERSION_MISMATCH` 调用错误 | 不发送不一致输入给模型 |

## 4. 物品画像与统计

### 4.1 物品记录 `item`

```typescript
type ItemFeatures = RecordHeader & {
  item_id: RawId;
  category_code: 0 | 1 | null;     // 兼容字段名；实际语义只有视频长短类型
  metadata_available: boolean;   // 此字段只说明视频类型是否已知
  seen_as_target: boolean;
  seen_in_history: boolean;
  statistics: ItemStatistics;
  valid_counts: Counts;
  missing_counts: Counts;
  missing_fields: MissingFields;
};

type ItemStatistics = {
  n: number;                     // 在声明数据范围内作为目标物品出现的行数
  click_sum: number | null;
  like_sum: number | null;
  follow_sum: number | null;
  share_sum: number | null;
  ctr: number | null;
  like_rate: number | null;
  follow_rate: number | null;
  share_rate: number | null;
  watch_mean: number | null;
  watch_max: number | null;
  watch_sum: number | null;
  gender1_share: number | null;
  avg_age_code: number | null;
  log1p_watch_sum: number | null;
};
```

`category_code` 不表示主题、品类或标签；仅是 Tenrec 视频长短类型。`0/1` 的长短对应方向没有在当前材料中披露，因此直接保留代码。`watch_*` 聚合源字段 `watching_times`，不擅自赋予秒等单位；`avg_age_code` 是年龄编码平均值，不是平均年龄。

以下为真实物品 `4` 在前 20 万行中的部分业务字段，**不是模型召回结果**：

```json
{
  "item_id": "4",
  "category_code": 1,
  "metadata_available": true,
  "statistics": {
    "n": 36,
    "click_sum": 6,
    "like_sum": 0,
    "follow_sum": 0,
    "share_sum": 0,
    "ctr": 0.16666666666666666,
    "like_rate": 0.0,
    "follow_rate": 0.0,
    "share_rate": 0.0,
    "watch_mean": 0.7777777777777778,
    "watch_max": 2,
    "watch_sum": 28,
    "gender1_share": 0.75,
    "avg_age_code": 2.5555555555555554,
    "log1p_watch_sum": 3.367295829986474
  },
  "valid_counts": {"click":36,"like":36,"follow":36,"share":36,"watching_times":36,"gender":36,"age":36,"video_category":36},
  "missing_counts": {"click":0,"like":0,"follow":0,"share":0,"watching_times":0,"gender":0,"age":0,"video_category":0}
}
```

### 4.2 统计的分母与缺失

```python
def rate_or_null(total, valid_count):
    return total / valid_count if valid_count > 0 else None

ctr = rate_or_null(click_sum, valid_counts["click"])
like_rate = rate_or_null(like_sum, valid_counts["like"])
follow_rate = rate_or_null(follow_sum, valid_counts["follow"])
share_rate = rate_or_null(share_sum, valid_counts["share"])
watch_mean = rate_or_null(watch_sum, valid_counts["watching_times"])
gender1_share = rate_or_null(gender1_count, valid_counts["gender"])
avg_age_code = rate_or_null(age_code_sum, valid_counts["age"])
```

每一字段使用自己的有效行数。没有有效观测时，业务值保存 `null` 并保留计数；不能把“没有统计”解释成“真实统计等于零”。若将来某模型必须接收数值张量，由明确的模型输入适配配方填默认值并附缺失标志，而不是修改业务事实。

目标和历史出现的全部 ID 构成物品目录。仅出现在历史中的物品也有 `item` 记录；`n=0`、无有效观测的统计为 `null`，类型只有真实来源能关联时才填写。候选整键缺失与物品记录中个别字段未知要区分处理。

现有附件中的 15 维 `candidate_dense` 是上一版接口配方，不是本轮强制采用的特征库存储格式，更不能当作当前 OneTrans `/rank` 的 TSV 列顺序。特征库优先保留有名称的业务字段；OneTrans 当前如何装配，见 [源码口径](02_onetrans.md)。

## 5. 用户表示、兴趣词项与物品语义编码

### 5.1 用户向量与稀疏兴趣 `user_rep`

```typescript
type DenseUserRepresentation = RecordHeader & {
  user_id: RawId;
  embedding_space_id: string;
  history_hash: string;
  vector: number[];           // 有限实数；长度严格等于dimension
  dimension: number;          // 来自实际checkpoint，不能由客户端任意指定
};

type SparseUserRepresentation = RecordHeader & {
  user_id: RawId;
  sparse_recipe_id: "video_type_binary_v1";
  history_hash: string;
  tokens: { token: string; weight: number }[];
  known_history_count: number;
  missing_history_count: number;
};
```

```python
# 来自同一release的物品类型；按每次历史出现计数，保留重复行为的作用。
known_types = [item.category_code for item in history_items
               if item.category_code is not None]
tokens = [
    {"token": "video_type_" + str(code),
     "weight": known_types.count(code) / len(known_types)}
    for code in (0, 1) if code in known_types
]
# 没有已知类型时tokens=[]，不是伪造video_type_0。
```

用户 `1` 在样例中有 9 个已知历史类型和 1 个未知类型，得到 `video_type_0: 2/9`、`video_type_1: 7/9`。DSSM 用户向量必须由实际用户塔计算；生产装载没有产物就返回缺失并阻止要求该资产的发布，不能以演示向量替代模型产物。第 08 篇另有明确标记的教学值，只用于接口说明。

用户画像或历史改变后必须重算相关表示。当前采用不可变 release，因此读到的用户、历史和表示不会在请求中途被覆盖。未来使用实时更新时，应保持明确的历史版本关联，不能只给各键设置 TTL 就声称一致。

### 5.2 物品编码 `item_rep` 与反向关联 `sid_map`

SID 是生成模型使用的一组离散编码；它不是 Tenrec 原始物品 ID。编码长度、每一位合法范围、与生成 token 的转换规则来自同版本模型清单。

```typescript
type ItemSemanticRepresentation = RecordHeader & {
  item_id: RawId;
  sid_version: string;
  semantic_id: number[];
};
type SemanticIdToItems = RecordHeader & {
  sid_version: string;
  semantic_id: number[];
  item_ids: RawId[];       // 去重，按数值ID稳定升序；允许一对多
};
```

```python
def canonical_sid(codes, declared_length, allowed_ranges):
    assert len(codes) == declared_length
    for code, legal_range in zip(codes, allowed_ranges):
        assert isinstance(code, int) and code in legal_range
    return ".".join(str(code) for code in codes)

forward_key = f"rec:qkv:{release}:item_rep:{sid_version}:{raw_item_id}"
reverse_key = f"rec:qkv:{release}:sid_map:{sid_version}:{canonical_sid_value}"
# 以上只有键格式，不提供未经模型实际生产的SID数值。
```

离线从同一份物品编码结果生成正反向表，并检查关联一致性。SID 碰撞时，一个反向键保存全部合法原始 ID；不能后写覆盖前写。反向表仅收录当前发布允许返回的原始 ID，生成服务按已声明规则展开、去重、限量。

```python
for generated_sid in generated_sids_in_model_order:
    raw_ids = feature_result[generated_sid].item_ids
    for item_id in raw_ids:               # 相同SID下按稳定ID顺序
        if item_id not in emitted:
            emit(item_id)
        if output_count == topk:
            return output
```

无反向关联的 SID 报告缺失并不产生候选；不能通过任意取模或随机 ID 补足数量。模型的 tokenizer、codebook、engine 仍在模型服务装载；**它们描述如何计算/解析编码，特征服务的表描述哪些真实物品对应哪些编码。**

## 6. 发布、更新与缺失：统一规则

| 数据组 | 更新方式与版本 | 缺失时 | 批量读取 |
|---|---|---|---|
| `user`、`history` | 同源行规则、同 release 一起生产；不原地修改 | 整键缺失是 `NOT_FOUND`；空历史用存在的记录表达 | `GetUserContext` 内部合并查询 |
| `item` | 同 release 目录与聚合结果 | 缺整键按候选策略丢弃；缺字段保留 `null` 和标志 | `BatchGetItemFeatures`，响应与候选输入逐位置对应 |
| `user_rep:dense` | release + 向量空间 + history_hash | 必需向量缺失则发布/请求失败；不填随机值 | 与用户上下文同次业务 RPC 返回 |
| `user_rep:sparse` | release + 词项配方 + history_hash | 无已知类型可返回真实空词项；缺记录与空词项不同 | 与用户上下文同次业务 RPC 返回 |
| `item_rep` | release + sid_version | 逐 ID 报告缺失；首期任一历史 SID 缺失即使生成分支失败，不静默删历史 | `BatchGetItemRepresentations(raw_item_id)` |
| `sid_map` | 从同版本正向表构造 | 逐 SID 返回缺失；无候选就是真实空结果 | `BatchGetItemRepresentations(semantic_id)` |
| `release` | 构建、校验、发布；只有 READY 可供新请求 | 未知或非 READY 版本拒绝 | 可在进程装载受控缓存，按明确发布事件刷新 |

```json
{
  "schema_version": "feature_v1",
  "release_id": "example_not_published",
  "state": "BUILDING",
  "source": {
    "dataset": "Tenrec",
    "row_scope": "first_200000_data_rows_in_file_order"
  },
  "required_business_key_groups": ["user","history","item","user_rep","item_rep","sid_map"],
  "publication_note": "仅展示清单结构；完整发布还需要实际hash、版本、数量和覆盖校验结果"
}
```

首期不得把生产中缺少的字段用默认配置掩盖。READY 检查至少包括：源范围可追溯、键可读回、ID 与 key 一致、用户表示对应同一历史、向量维度与 Milvus 一致、SID 正反表一致、索引返回 ID 可关联物品目录。各存储不是一个原子事务：先在新版本名下完整装载，再发布 READY，切换新请求，最后回收旧版本。

## 7. PaiRec 与子服务如何交互

### 7.1 三个业务方法

下面先给出响应骨架，再展示调用方式。用户上下文的四种数据分别有固定状态字段；既不把缺失记录伪装成空对象，也不要求调用方猜测“响应里没有这个字段”是什么意思。

```typescript
type ResponseMeta = {
  request_id: string;
  release_id: string;
  schema_version: "feature_v1";
};
type ViewStatus = "FOUND" | "NOT_FOUND" | "NOT_REQUESTED";
type UserContextResult = ResponseMeta & {
  view_status: {
    USER: ViewStatus;
    HISTORY: ViewStatus;
    DENSE_QUERY: ViewStatus;
    SPARSE_QUERY: ViewStatus;
  };
  user: UserProfile | null;
  history: UserHistory | null;
  dense_query: DenseUserRepresentation | null;
  sparse_query: SparseUserRepresentation | null;
};

type ItemResult<T> = { item_id: RawId; status: "FOUND" | "NOT_FOUND"; value: T | null };
type ItemFeatureBatch = ResponseMeta & {
  results: ItemResult<Partial<ItemFeatures>>[]; // value只含请求选择的业务字段
};
type ItemRepresentationBatch = ResponseMeta & { sid_version: string } & (
  | { lookup_by: "raw_item_id"; results: ItemResult<ItemSemanticRepresentation>[] }
  | { lookup_by: "semantic_id"; results: {
      semantic_id: number[];
      status: "FOUND" | "NOT_FOUND";
      item_ids: RawId[];                     // NOT_FOUND时为空数组
    }[] }
);
```

| 用户上下文状态 | 对应数据字段 | 调用方判断 |
|---|---|---|
| `FOUND` | 返回对应的业务记录 | 可使用；记录内属性缺失查看 `missing_fields` |
| `NOT_FOUND` | `null` | 请求了该视图，但数据库没有该记录 |
| `NOT_REQUESTED` | `null` | 本次没有要求这个视图；服务无需返回它的数据 |

四个状态字段和四个数据字段始终返回。请求了不支持的字段/视图、版本不匹配、数据库不可用或解析失败，均是**请求级错误**，不以 `NOT_FOUND` 表达。原始 ID 批量结果中，`FOUND` 的 `value` 有值，`NOT_FOUND` 的 `value` 为 `null`；SID 反查的缺失结果用 `status=NOT_FOUND, item_ids=[]` 表达。

```python
required_views = ["USER", "HISTORY", "DENSE_QUERY", "SPARSE_QUERY"]
user_context = FeatureService.GetUserContext(
    request_id=request_id,
    release_id=release_id,
    schema_version="feature_v1",
    user_id="1",
    required_views=required_views,
    representation_versions={
        "embedding_space_id": manifest.embedding_space_id,
        "sparse_recipe_id": manifest.sparse_recipe_id,
    },
    remaining_timeout_ms=remaining_budget(),
)
for view in required_views:
    if user_context.view_status[view] != "FOUND":
        raise RequiredFeatureMissing(view)
# 本例要求四种输入齐全；检查通过后才读取下面的history.item_ids。

history_codes = FeatureService.BatchGetItemRepresentations(
    request_id=request_id, release_id=release_id, schema_version="feature_v1",
    lookup_by="raw_item_id",
    item_ids=user_context.history.item_ids,
    sid_version=manifest.sid_version,
    remaining_timeout_ms=remaining_budget(),
)

candidate_features = FeatureService.BatchGetItemFeatures(
    request_id=request_id, release_id=release_id, schema_version="feature_v1",
    item_ids=candidate_ids,
    fields=["category_code", "metadata_available", "statistics", "missing_fields"],
    remaining_timeout_ms=remaining_budget(),
)
```

响应均回传请求和发布身份。两个批量方法的 `results` 与输入等长、同顺序，缺失仍占位置；缺失列表由这些结果的 `status` 提取，不额外维护一份可能失配的 `missing_ids`。表示方法采用二选一查询字段：`lookup_by=raw_item_id` 时必须给 `item_ids`；`lookup_by=semantic_id` 时必须给 `semantic_ids`，不能两组都传。

```python
# 生成服务完成模型推理之后调用，版本从PaiRec原请求传入，禁止自行选择最新版。
resolved = FeatureService.BatchGetItemRepresentations(
    request_id=request_id, release_id=release_id, schema_version="feature_v1",
    lookup_by="semantic_id", semantic_ids=generated_semantic_ids,
    sid_version=sid_version,
    remaining_timeout_ms=remaining_budget(),
)
# resolved.results[i] = {semantic_id, status, item_ids:[...]}，一对多不丢失。
```

### 7.2 查询责任只分配一次

| 阶段 / 服务 | 谁调用特征服务 | 实际输入与输出 | 是否再查询同一份数据 |
|---|---|---|---|
| 用户准备 | PaiRec | 用户 ID → 画像、原历史、用户向量、稀疏词项 | 各召回服务使用传入结果 |
| 向量召回 | 默认不直接调用 | PaiRec 传用户向量和空间版本 → 向量召回访问 Milvus | 不按用户 ID 再查同一向量 |
| 稀疏召回 | 默认不直接调用 | PaiRec 传词项/权重 → 稀疏召回访问 OpenSearch | 不按用户 ID 再查同一兴趣 |
| 生成输入准备 | PaiRec | 原历史物品 ID → 同版本历史 SID | 生成服务直接使用传入历史 SID |
| 生成输出转换 | 生成服务 | 模型新生成 SID → 当前目录中的原始物品 ID 列表 | 此时数据才产生，属于该阶段的专属查询 |
| 候选准备与后排序 | PaiRec | 融合后的候选 ID → 物品属性和统计 | 在本请求内复用同一份结果 |
| OneTrans 当前 `/ingest` | 按当前历史提供代码 | 外部历史 ID 与位置进入模型；内部查参数 | 不把当前路径标成 FeatureService 已接入 |
| OneTrans 当前 `/rank` | 不调用新特征服务 | 用户与候选 ID → 本地已装载的用户/物品特征 + 参数服务 | 本轮按实际源码保留 |
| 将来的其他精排模型 | 默认 PaiRec 传完整必要字段 | 用户/历史/候选业务值 → 模型自行构造张量与打分 | 仅专属或大量中间 ID 查询时允许服务自行批查 |

默认规则是 PaiRec 查公共输入并分发，子服务只在 **输入到达后才产生的新 ID** 或 **阶段内部专属的大批量 ID** 查询上直接调用特征服务。上述生成输出转换就是具体例子。这样没有必要把所有内部查询挤入 PaiRec，也避免每个服务再次查询同一份用户画像。

```text
一次正常三路召回请求的特征RPC：
1. PaiRec → GetUserContext
2. PaiRec → BatchGetItemRepresentations，原始历史ID查SID
3. 生成服务 → BatchGetItemRepresentations，输出SID查原始ID
4. PaiRec → BatchGetItemFeatures，融合候选查物品

这是两个业务取数阶段、三个API方法、四次业务RPC。
Redis实际读取批次由记录大小、条数和分片决定，另行计数。
```

每次调用携带同一 `release_id`、正确的表示版本、请求身份及剩余时间。特征服务错误、超时、字段不合法不得转换成正常空结果；子服务不得改用另一版本或任意本地 CSV 回源。OneTrans 本轮保留的本地装载路径是已明确列出的源码例外，不是其他模块的通用降级规则。

## 8. 实施时优先完成什么

```yaml
priority_1:
  - 冻结上述字段、键和三个接口，连同SID查询方向一起定义
  - 从真实源数据生产用户、历史、物品和有来源的派生表示
  - 独立FeatureService实现有界批查、结果对齐、版本与缺失检查
priority_2:
  - PaiRec两阶段读取和三个召回输入接通
  - 生成服务用SID反向查询得到真实原始物品ID
  - 验证所有检索结果可关联物品特征，完成READY发布与回滚
later_when_required:
  - 有真实事件流后增加实时特征与窗口统计
  - 有其他排序模型后增加对应输入适配
  - 容量证据需要时扩成Redis集群或引入离线分析库
```

当前 OneTrans 的本地数据来源可直接核对 [用户/物品 TSV 装载与装配](assets/source_snapshots/OneTrans_HSE_project/cpp/src/serving/rank_assembler.cpp.html#L18)及 [历史构造代码](assets/source_snapshots/pairec4tigerllm_8506/services/recall/onetrans_s_stage.go.html#L63)。本文件不修改这些代码，也不将旧配方的 15 维数组声明为已接入的在线特征。
