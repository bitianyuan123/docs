"""本版逻辑数据合同：目标FeatureService + 当前OneTrans HTTP业务字段投影。

仅用于工程评审，不是已实现的客户端、protobuf或生产验证器。
批量项保持输入顺序及重复位置；缺失与调用失败分开表达。
"""
from typing import Generic, Literal, NotRequired, Protocol, TypeVar, TypedDict

RawID = str
RecordStatus = Literal["FOUND", "NOT_FOUND"]
ViewStatus = Literal["FOUND", "NOT_FOUND", "NOT_REQUESTED"]
ViewName = Literal["USER", "HISTORY", "DENSE_QUERY", "SPARSE_QUERY"]
T = TypeVar("T")


class RecordHeader(TypedDict):
    release_id: str
    schema_version: Literal["feature_v1"]


class RequestMeta(RecordHeader):
    request_id: str
    remaining_timeout_ms: int


class ResponseMeta(RecordHeader):
    request_id: str


class UserProfile(RecordHeader):
    user_id: RawID
    gender_code: int | None
    age_code: int | None
    missing_fields: list[str]
    source_data_row: int


class UserHistory(RecordHeader):
    user_id: RawID
    item_ids: list[RawID]
    positions: list[int]             # 1起；序列位置，不是时间戳
    valid_length: int
    time_semantics: Literal["ordinal"]
    history_hash: str
    source_data_row: int


class SparseToken(TypedDict):
    token: str
    weight: float


class DenseUserRepresentation(RecordHeader):
    user_id: RawID
    embedding_space_id: str
    history_hash: str
    vector: list[float]              # 真实离线模型输出，不可随机补齐
    dimension: int


class SparseUserRepresentation(RecordHeader):
    user_id: RawID
    sparse_recipe_id: str
    history_hash: str
    tokens: list[SparseToken]
    known_history_count: int
    missing_history_count: int


class ItemFeatures(RecordHeader):
    item_id: RawID
    category_code: Literal[0, 1] | None
    metadata_available: bool
    seen_as_target: bool
    seen_in_history: bool
    statistics: dict[str, int | float | None]  # 字段名由10_feature_catalog锁定
    valid_counts: dict[str, int]
    missing_counts: dict[str, int]
    missing_fields: list[str]


class ItemSemanticRepresentation(RecordHeader):
    item_id: RawID
    sid_version: str
    semantic_id: list[int]


class RepresentationVersions(TypedDict):
    embedding_space_id: NotRequired[str]
    sparse_recipe_id: NotRequired[str]


class UserQuery(RequestMeta):
    user_id: RawID
    required_views: list[ViewName]
    representation_versions: RepresentationVersions


class UserContextResult(ResponseMeta):
    # 总有四个键；仅请求的视图可为FOUND/NOT_FOUND，其余NOT_REQUESTED。
    view_status: dict[ViewName, ViewStatus]
    user: UserProfile | None
    history: UserHistory | None
    dense_query: DenseUserRepresentation | None
    sparse_query: SparseUserRepresentation | None


class RawItemRepresentationQuery(RequestMeta):
    lookup_by: Literal["raw_item_id"]
    item_ids: list[RawID]             # 不允许同时传semantic_ids
    sid_version: str


class SemanticRepresentationQuery(RequestMeta):
    lookup_by: Literal["semantic_id"]
    semantic_ids: list[list[int]]    # 不允许同时传item_ids
    sid_version: str


class ItemQuery(RequestMeta):
    item_ids: list[RawID]
    fields: list[str]


class ItemResult(TypedDict, Generic[T]):
    item_id: RawID
    status: RecordStatus
    value: T | None


class SemanticIdResult(TypedDict):
    semantic_id: list[int]
    status: RecordStatus
    item_ids: list[RawID]            # 同SID按数值ID升序；NOT_FOUND时空


class ItemFeatureBatch(ResponseMeta):
    results: list[ItemResult[dict[str, object]]]  # value仅含fields所选字段；整键状态独立


class RawRepresentationBatch(ResponseMeta):
    lookup_by: Literal["raw_item_id"]
    sid_version: str
    results: list[ItemResult[ItemSemanticRepresentation]]


class SemanticRepresentationBatch(ResponseMeta):
    lookup_by: Literal["semantic_id"]
    sid_version: str
    results: list[SemanticIdResult]


class FeatureService(Protocol):
    def GetUserContext(self, request: UserQuery) -> UserContextResult: ...
    def BatchGetItemFeatures(self, request: ItemQuery) -> ItemFeatureBatch: ...
    def BatchGetItemRepresentations(
        self, request: RawItemRepresentationQuery | SemanticRepresentationQuery
    ) -> RawRepresentationBatch | SemanticRepresentationBatch: ...


# 当前OneTrans HTTP字段投影，与上方FeatureService类型没有继承关系。
# /ingest timestamps由当前调用方生成0..n-1；不改称真实时间。
class OneTransIngestRequest(TypedDict):
    user_id: str
    item_ids: list[int]
    timestamps: list[int]


class OneTransRankItem(TypedDict):
    item_id: str


class OneTransRankRequest(TypedDict):
    request_id: str
    user_id: str
    items: list[OneTransRankItem]


class OneTransRankResultItem(TypedDict):
    item_id: str
    score: float                    # sigmoid(first logit)，未必是点击概率


class OneTransRankResponse(TypedDict):
    code: int
    msg: str
    request_id: str
    model_version: str
    model_role: str
    items: list[OneTransRankResultItem]
    trace: dict[str, object]         # 已有kv_hit/候选数和耗时字段，非新ready句柄
