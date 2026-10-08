"""目标架构的逻辑类型契约；不是已实现的网络客户端或生成式现有protobuf。

ID 是 Tenrec 原始十进制字符串。现有HTTP的数值ID由显式适配转换；
生成式服务沿用既有protobuf及其int32范围校验，不能直接序列化本文件代替。
"""

from dataclasses import dataclass
from typing import Literal, Protocol, Sequence

RawID = str
Dense15 = tuple[float, ...]  # 运行时必须 len == 15，且所有值有限
RecallSource = Literal["vector", "sparse", "generative"]


@dataclass(frozen=True)
class RequestContext:
    request_id: str             # 内部生成且全局唯一，不照搬外部ID
    release_id: str
    remaining_timeout_ms: int   # 每次调用时重新计算，使用单调时钟


@dataclass(frozen=True)
class SparseToken:
    token: str
    weight: float


@dataclass(frozen=True)
class History:
    item_ids: tuple[RawID, ...]
    positions: tuple[int, ...]
    valid_length: int
    history_hash: str
    time_semantics: Literal["ordinal"] = "ordinal"


@dataclass(frozen=True)
class UserFeatures:
    user_id: RawID
    release_id: str
    schema_version: str
    user_dense: Dense15
    dense_vector: tuple[float, ...]     # 真实DSSM输出；维度与索引空间匹配
    embedding_space_id: str
    sparse_tokens: tuple[SparseToken, ...]
    missing_fields: tuple[str, ...]


@dataclass(frozen=True)
class ItemFeatures:
    item_id: RawID
    release_id: str
    category_code: int | None          # 视频长短类型0/1/null
    candidate_dense: Dense15
    missing_fields: tuple[str, ...]


@dataclass(frozen=True)
class SourceEvidence:
    source: RecallSource
    source_rank: int
    raw_score: float | None            # 无概率语义时不用它跨源排序
    score_semantics: str


@dataclass(frozen=True)
class Candidate:
    item_id: RawID
    sources: tuple[SourceEvidence, ...]
    fusion_order: int


@dataclass(frozen=True)
class RecallResult:
    request_id: str
    items: tuple[Candidate, ...]
    executed: bool
    backend: str
    status: Literal["ok", "empty"]
    # 错误通过异常/Result.error传播，不能用empty伪装失败。


@dataclass(frozen=True)
class KVIdentity:
    request_id: str
    user_id: RawID
    release_id: str
    model_version: str
    history_hash: str
    kv_key: str  # fine:<release_id>:<model_version>:<request_id>


@dataclass(frozen=True)
class PreparedHistory:
    identity: KVIdentity
    accepted: bool
    ready: bool
    executed: bool
    kv_bytes: int
    kv_backend: Literal["datasystem"]
    backend: Literal["cpp_cpu"]


@dataclass(frozen=True)
class CandidateScoreResult:
    identity: KVIdentity
    item_ids: tuple[RawID, ...]
    logits: tuple[tuple[float, float], ...]  # 与输入逐ID逐位置一一对应
    executed: bool
    kv_hit: bool
    kv_backend: Literal["datasystem"]
    backend: Literal["cpp_cpu"]
    device: Literal["cpu"]


class FeatureRepository(Protocol):
    async def load_user_history(
        self, ctx: RequestContext, uid: RawID
    ) -> tuple[UserFeatures, History]: ...

    async def load_items(
        self, ctx: RequestContext, ids: Sequence[RawID]
    ) -> Sequence[ItemFeatures | None]:
        """按输入原位置返回；None是缺键，不是网络或解析失败。"""
        ...


class VectorRecall(Protocol):
    async def recall(
        self, ctx: RequestContext, query_vector: Sequence[float],
        embedding_space_id: str, topk: int
    ) -> RecallResult: ...


class SparseRecall(Protocol):
    async def recall(
        self, ctx: RequestContext, tokens: Sequence[SparseToken], topk: int
    ) -> RecallResult: ...


class GenerativeRecall(Protocol):
    async def recall(
        self, ctx: RequestContext, history: History, topk: int
    ) -> RecallResult:
        """适配层先以同版本SID表转换，再发送现有Recommend protobuf。"""
        ...


class OneTrans(Protocol):
    async def prepare(
        self, ctx: RequestContext, identity: KVIdentity, history: History
    ) -> PreparedHistory: ...

    async def score(
        self, ctx: RequestContext, ready: PreparedHistory,
        user: UserFeatures, candidates: Sequence[ItemFeatures]
    ) -> CandidateScoreResult: ...

    async def release(
        self, identity: KVIdentity,
        score_state: Literal["not_started", "terminal", "unknown"],
        cleanup_timeout_ms: int,
    ) -> None:
        """幂等控制操作；身份全量校验；使用独立、有界的清理期限。"""
        ...


USER_DENSE_FIELDS = ("gender_code_or_zero", "age_code_or_zero") + tuple(
    f"reserved_zero_{i}" for i in range(13)
)
CANDIDATE_DENSE_FIELDS = (
    "n", "click_sum", "like_sum", "follow_sum", "share_sum",
    "like_rate", "follow_rate", "share_rate", "ctr",
    "watch_mean", "watch_max", "watch_sum", "gender1_share",
    "avg_age_code", "log1p_watch_sum",
)
