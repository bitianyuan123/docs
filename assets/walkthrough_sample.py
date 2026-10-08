"""构造并检查第08章的教学数据；只读写本目录文件，不调用任何服务。

运行：python3 walkthrough_sample.py
输出：walkthrough_sample.json、walkthrough_checks.json。
此文件说明字段传递与候选顺序，不模拟模型，也不证明任何服务已部署。
"""
import hashlib
import json
import math
from pathlib import Path


REQUEST_ID = "demo-user-1-001"
RELEASE_ID = "demo_tenrec_v1"
SCHEMA_VERSION = "feature_v1"
USER_ID = "1"
EMBEDDING_SPACE = "demo_dssm_64_v1"
SID_VERSION = "demo_sid_v1"
SPARSE_RECIPE = "video_type_binary_v1"
MODEL_VERSION = "demo_onetrans_v1"
HISTORY = ["2", "3", "80936", "781", "111774", "1230", "26403", "991", "2362", "1202"]
STATISTICS_FIELDS = {
    "n", "click_sum", "like_sum", "follow_sum", "share_sum", "ctr", "like_rate",
    "follow_rate", "share_rate", "watch_mean", "watch_max", "watch_sum",
    "gender1_share", "avg_age_code", "log1p_watch_sum",
}


def header():
    return {"release_id": RELEASE_ID, "schema_version": SCHEMA_VERSION}


def response_meta():
    return {"request_id": REQUEST_ID, **header()}


def request_meta(remaining_ms):
    return {**response_meta(), "remaining_timeout_ms": remaining_ms}


def ordered_items(pairs):
    return [{"item_id": item_id, "score": score} for item_id, score in pairs]


def build_sample():
    evidence = json.loads((Path(__file__).resolve().parent / "tenrec_sample_evidence.json").read_text(encoding="utf-8"))
    actual_statistics = {
        item["item_id"]: dict(zip(evidence["candidate_dense_fields"], item["candidate_dense"]))
        for item in evidence["candidate_examples"]
    }

    def illustrative_statistics(n, click_sum):
        return {"n": n, "click_sum": click_sum, "like_sum": 0, "follow_sum": 0,
                "share_sum": 0, "ctr": click_sum / n, "like_rate": 0.0,
                "follow_rate": 0.0, "share_rate": 0.0, "watch_mean": 0.0,
                "watch_max": 0, "watch_sum": 0, "gender1_share": .5,
                "avg_age_code": 4.0, "log1p_watch_sum": 0.0}

    normalized_history = {
        "item_ids": HISTORY.copy(),
        "positions": list(range(1, 11)),
        "time_semantics": "ordinal",
    }
    history_hash = hashlib.sha256(json.dumps(
        normalized_history, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()
    user_context = {
        **response_meta(),
        "view_status": {name: "FOUND" for name in ["USER", "HISTORY", "DENSE_QUERY", "SPARSE_QUERY"]},
        "user": {**header(), "user_id": USER_ID, "gender_code": 1, "age_code": 4,
                 "missing_fields": [], "source_data_row": 1},
        "history": {**header(), "user_id": USER_ID, **normalized_history,
                    "valid_length": 10, "history_hash": history_hash, "source_data_row": 1},
        "dense_query": {**header(), "user_id": USER_ID,
                        "embedding_space_id": EMBEDDING_SPACE, "history_hash": history_hash,
                        "vector": [0.125, -0.125] * 32, "dimension": 64},
        "sparse_query": {**header(), "user_id": USER_ID,
                         "sparse_recipe_id": SPARSE_RECIPE, "history_hash": history_hash,
                         "tokens": [{"token": "video_type_0", "weight": 2 / 9},
                                    {"token": "video_type_1", "weight": 7 / 9}],
                         "known_history_count": 9, "missing_history_count": 1},
    }
    history_results = [
        {"item_id": item_id, "status": "FOUND", "value": {
            **header(), "item_id": item_id, "sid_version": SID_VERSION,
            "semantic_id": [i, i + 10, i + 20, i + 30],
        }} for i, item_id in enumerate(HISTORY, start=1)
    ]
    vector_response = {
        **response_meta(), "embedding_space_id": EMBEDDING_SPACE,
        "score_semantics": "cosine_similarity",
        "items": ordered_items([("1201", .92), ("9001", .89), ("3", .82), ("4", .81)]),
    }
    sparse_response = {
        **response_meta(), "sparse_recipe_id": SPARSE_RECIPE,
        "score_semantics": "bm25",
        "items": ordered_items([("4", 3.2), ("9002", 2.7), ("9003", 2.1)]),
    }
    generated_sids = [[21, 31, 41, 51], [22, 32, 42, 52]]
    resolved_results = [
        {"semantic_id": generated_sids[0], "status": "FOUND", "item_ids": ["4", "1201"]},
        {"semantic_id": generated_sids[1], "status": "FOUND", "item_ids": ["9002"]},
    ]
    generation_wire_response = {
        "code": 200, "user_id": USER_ID, "inference_time_ms": 0,
        "recommendations": [
            {"item_id": int(item_id), "semantic_id": entry["semantic_id"], "score": 1.0}
            for entry in resolved_results for item_id in entry["item_ids"]
        ],
    }
    generative_response = {
        **response_meta(), "sid_version": SID_VERSION,
        "score_semantics": "constant_placeholder",
        "items": [{"item_id": str(item["item_id"]), "score": item["score"]}
                  for item in generation_wire_response["recommendations"]],
    }
    # 本例融合规则：先生成路，再对稀疏/向量路按各自次序轮询。
    # 跨路分数不能比较；先过滤已看、再保留首次出现的物品。
    candidate_ids = []
    seen = set(HISTORY)

    def take(item):
        item_id = item["item_id"]
        if item_id not in seen and len(candidate_ids) < 50:
            candidate_ids.append(item_id)
            seen.add(item_id)

    for item in generative_response["items"]:
        take(item)
    for i in range(max(len(sparse_response["items"]), len(vector_response["items"]))):
        for branch in [sparse_response, vector_response]:
            if i < len(branch["items"]):
                take(branch["items"][i])

    item_values = {
        "4": {"category_code": 1, "metadata_available": True,
              "statistics": actual_statistics["4"], "missing_fields": []},
        "1201": {"category_code": 1, "metadata_available": True,
                 "statistics": actual_statistics["1201"], "missing_fields": []},
        "9002": {"category_code": 0, "metadata_available": True,
                 "statistics": illustrative_statistics(20, 2), "missing_fields": []},
        "9001": {"category_code": 1, "metadata_available": True,
                 "statistics": illustrative_statistics(40, 8), "missing_fields": []},
    }
    item_results = [
        {"item_id": item_id, "status": "FOUND" if item_id in item_values else "NOT_FOUND",
         "value": item_values.get(item_id)} for item_id in candidate_ids
    ]
    rankable_ids = [entry["item_id"] for entry in item_results if entry["status"] == "FOUND"]
    rank_items = ordered_items(list(zip(rankable_ids, [.72, .86, .63, .91])))
    rank_trace = {"kv_hit": True, "score_unique_count": 4,
                  "feature_us": 0, "compute_us": 0, "backend_rpc_us": 0,
                  "lookup_us": 0, "encode_us": 0, "kv_us": 0,
                  "batch_wait_us": 0, "batch_compute_us": 0,
                  "batch_size": 1, "n_candidates": 4}
    ranked = sorted(rank_items, key=lambda item: -item["score"])
    return {
        "metadata": {
            "kind": "illustrative_not_model_run",
            "purpose": "第08章逐阶段字段、来源、顺序的教学例；不是在线请求记录。",
            "real_source_fields": {
                "user_id_gender_age_history": "Tenrec首条数据行；历史保留10项原始ID与顺序。",
                "sparse_tokens": "已有前20万行证据中的9个已知历史类型，权重2/9、7/9。",
                "item_4_and_1201_statistics": "从tenrec_sample_evidence.json的字段名与数组对应关系读取已有前20万行聚合证据；不能据此推断它们被真实召回。",
                "history_hash": "按特征字典规范，从本例历史重新计算。",
            },
            "illustrative_fields": [
                "release与模型版本、DSSM查询向量、全部SID及映射、三路召回候选和分数",
                "9001与9002物品字段、9003缺失状态、精排分数、最终推荐",
                "OneTrans独立历史提供器恰好返回相同10项的假设",
                "accepted、kv_hit、shard、全部耗时、checksum及其他运行诊断值",
            ],
            "vector_note": "64维单位向量只说明格式，不是DSSM模型产物，不能装载为真实发布数据。",
            "item_projection_note": "本附件statistics包含10_feature_catalog定义的全部15个具名统计字段；第08章正文只节选n和ctr。",
            "protocol_note": "特征服务及召回封装是目标接口；generation_request新增版本元数据，history保留旧proto形状。generation_wire_response仅展示旧响应字段形状。",
            "onetrans_data_boundary": "ingest_request来自独立历史提供器假设；rank_request仅传ID，用户/物品TSV和参数服务取数仍由当前OneTrans负责。",
            "checksum_note": "64个0是格式占位符，不是任何模型KV载荷的SHA256。",
            "timing_note": "所有时间值0只是字段格式示意，没有执行或测量服务。",
            "model_role_note": "engineering是当前程序默认配置值，不表示已部署模型角色。",
        },
        "external_request": {"uid": USER_ID, "scene_id": "home_feed", "size": 10},
        "user_request": {
            **request_meta(1000), "user_id": USER_ID,
            "required_views": ["USER", "HISTORY", "DENSE_QUERY", "SPARSE_QUERY"],
            "representation_versions": {"embedding_space_id": EMBEDDING_SPACE,
                                        "sparse_recipe_id": SPARSE_RECIPE},
        },
        "user_context": user_context,
        "history_repr_request": {**request_meta(900), "lookup_by": "raw_item_id",
                                 "item_ids": HISTORY.copy(), "sid_version": SID_VERSION},
        "history_repr_response": {**response_meta(), "lookup_by": "raw_item_id",
                                  "sid_version": SID_VERSION, "results": history_results},
        "vector_request": {
            **request_meta(900), "query_vector": user_context["dense_query"]["vector"].copy(),
            "embedding_space_id": user_context["dense_query"]["embedding_space_id"], "topk": 50,
        },
        "vector_response": vector_response,
        "sparse_request": {
            **request_meta(900), "sparse_tokens": user_context["sparse_query"]["tokens"],
            "sparse_recipe_id": user_context["sparse_query"]["sparse_recipe_id"], "topk": 50,
        },
        "sparse_response": sparse_response,
        "generation_request": {
            **request_meta(800), "user_id": USER_ID, "sid_version": SID_VERSION,
            "history": [{"value": item["value"]["semantic_id"]} for item in history_results],
            "topk": 10, "temperature": 1.0, "beam_width": 1,
        },
        "generated_sid_lookup_request": {
            **request_meta(700), "lookup_by": "semantic_id", "semantic_ids": generated_sids,
            "sid_version": SID_VERSION,
        },
        "generated_sid_lookup_response": {**response_meta(), "lookup_by": "semantic_id",
                                          "sid_version": SID_VERSION, "results": resolved_results},
        "generation_wire_response": generation_wire_response,
        "generative_response": generative_response,
        "candidate_ids": candidate_ids,
        "item_request": {**request_meta(650), "item_ids": candidate_ids,
                         "fields": ["category_code", "metadata_available", "statistics", "missing_fields"]},
        "item_response": {**response_meta(), "results": item_results},
        "rankable_ids": rankable_ids,
        "onetrans_history_provider": {
            "source": "independent_provider_assumed_for_this_example",
            "user_id": USER_ID, "item_ids": HISTORY.copy(),
        },
        "ingest_request": {"user_id": USER_ID, "item_ids": [int(x) for x in HISTORY],
                           "timestamps": list(range(10))},
        "ingest_response": {"accepted": True, "shard": 0, "checksum": "0" * 64,
                            "reason": "", "ingest_us": 0},
        "rank_request": {"request_id": REQUEST_ID, "user_id": USER_ID,
                         "items": [{"item_id": x} for x in rankable_ids]},
        "rank_response": {"code": 200, "msg": "ok", "request_id": REQUEST_ID,
                          "model_version": MODEL_VERSION, "model_role": "engineering",
                          "items": rank_items, "trace": rank_trace},
        "final_response": {"request_id": REQUEST_ID, "release_id": RELEASE_ID,
                           "items": ranked[:10], "returned_size": len(ranked[:10]),
                           "shortfall": len(ranked) < 10},
    }


def validate(sample):
    """验证样例自身的传递关系，不验证服务、模型或数据库实现。"""
    checks = []

    def check(name, condition):
        if not condition:
            raise ValueError("样例关系不成立：" + name)
        checks.append(name)

    context = sample["user_context"]
    check("样例显式标记为未执行模型", sample["metadata"]["kind"] == "illustrative_not_model_run")
    check("用户ID从外部入口传入用户查询", sample["external_request"]["uid"] == sample["user_request"]["user_id"] == "1")
    normalized = {key: context["history"][key] for key in ["item_ids", "positions", "time_semantics"]}
    history_hash = hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    check("历史哈希重算及两种派生表示绑定一致", history_hash == context["history"]["history_hash"] == context["dense_query"]["history_hash"] == context["sparse_query"]["history_hash"])
    check("用户上下文四视图均存在", all(status == "FOUND" for status in context["view_status"].values()))
    vector = sample["vector_request"]["query_vector"]
    check("向量调用取自上下文且维度为64", vector == context["dense_query"]["vector"] and len(vector) == context["dense_query"]["dimension"] == 64)
    check("演示向量单位范数", math.isclose(sum(x * x for x in vector), 1.0))
    check("向量空间版本贯穿查询与结果", sample["user_request"]["representation_versions"]["embedding_space_id"] == context["dense_query"]["embedding_space_id"] == sample["vector_request"]["embedding_space_id"] == sample["vector_response"]["embedding_space_id"])
    check("稀疏下游使用sparse_tokens而不是内部tokens字段", "sparse_tokens" in sample["sparse_request"] and "tokens" not in sample["sparse_request"])
    check("稀疏词项完整转交且权重和为1", sample["sparse_request"]["sparse_tokens"] == context["sparse_query"]["tokens"] and math.isclose(sum(x["weight"] for x in sample["sparse_request"]["sparse_tokens"]), 1.0))
    check("历史SID查询保持历史次序", sample["history_repr_request"]["item_ids"] == context["history"]["item_ids"] == [x["item_id"] for x in sample["history_repr_response"]["results"]])
    check("生成请求history保持proto对象形状与SID来源", sample["generation_request"]["history"] == [{"value": x["value"]["semantic_id"]} for x in sample["history_repr_response"]["results"]])
    check("生成proto用户ID为字符串而物品ID为整数", sample["generation_request"]["user_id"] == sample["generation_wire_response"]["user_id"] == USER_ID and all(isinstance(x["item_id"], int) for x in sample["generation_wire_response"]["recommendations"]))
    check("生成调用显式保留proto默认推理参数", sample["generation_request"]["temperature"] == 1.0 and sample["generation_request"]["beam_width"] == 1)
    check("SID查询方向互斥", "semantic_ids" not in sample["history_repr_request"] and "item_ids" not in sample["generated_sid_lookup_request"])
    reverse = sample["generated_sid_lookup_response"]["results"]
    check("反向结果保留SID次序及一对多", [x["semantic_id"] for x in reverse] == sample["generated_sid_lookup_request"]["semantic_ids"] and reverse[0]["item_ids"] == ["4", "1201"])
    check("生成服务展开反查结果", [x["item_id"] for x in sample["generative_response"]["items"]] == [item_id for x in reverse for item_id in x["item_ids"]])
    check("旧proto响应经适配保留原始ID", [str(x["item_id"]) for x in sample["generation_wire_response"]["recommendations"]] == [x["item_id"] for x in sample["generative_response"]["items"]])
    check("融合次序与样例策略一致", sample["candidate_ids"] == ["4", "1201", "9002", "9001", "9003"])
    check("候选已去重且已看物品3被过滤", len(set(sample["candidate_ids"])) == len(sample["candidate_ids"]) and not set(sample["candidate_ids"]) & set(HISTORY))
    check("候选特征逐位置返回且缺失不移位", sample["item_request"]["item_ids"] == sample["candidate_ids"] == [x["item_id"] for x in sample["item_response"]["results"]] and sample["item_response"]["results"][-1] == {"item_id": "9003", "status": "NOT_FOUND", "value": None})
    present_items = [x["value"] for x in sample["item_response"]["results"] if x["status"] == "FOUND"]
    check("物品投影仅含请求字段且statistics字段完整", all(set(x) == set(sample["item_request"]["fields"]) and set(x["statistics"]) == STATISTICS_FIELDS for x in present_items))
    check("物品统计中点击率与计数一致", all(math.isclose(x["statistics"]["ctr"], x["statistics"]["click_sum"] / x["statistics"]["n"]) for x in present_items))
    check("仅有特征的候选进入精排", sample["rankable_ids"] == [x["item_id"] for x in sample["item_response"]["results"] if x["status"] == "FOUND"] == [x["item_id"] for x in sample["rank_request"]["items"]])
    check("OneTrans历史来自独立提供器且整数转换不丢ID", [str(x) for x in sample["ingest_request"]["item_ids"]] == sample["onetrans_history_provider"]["item_ids"] and sample["onetrans_history_provider"]["source"] == "independent_provider_assumed_for_this_example")
    check("两种位置约定不同且都不是事件时间", context["history"]["positions"] == list(range(1, 11)) and sample["ingest_request"]["timestamps"] == list(range(10)))
    check("当前OneTrans入参只含源码接受的业务字段", set(sample["rank_request"]) == {"request_id", "user_id", "items"} and all(set(x) == {"item_id"} for x in sample["rank_request"]["items"]) and set(sample["ingest_request"]) == {"user_id", "item_ids", "timestamps"})
    check("精排响应按输入顺序归属分数", [x["item_id"] for x in sample["rank_response"]["items"]] == sample["rankable_ids"] and [x["score"] for x in sample["rank_response"]["items"]] == [.72, .86, .63, .91])
    check("后排序不混用三路召回分数", sample["final_response"]["items"] == sorted(sample["rank_response"]["items"], key=lambda x: -x["score"]))
    check("最终顺序与不足10项标志一致", [x["item_id"] for x in sample["final_response"]["items"]] == ["9001", "1201", "4", "9002"] and sample["final_response"]["returned_size"] == 4 and sample["final_response"]["shortfall"] is True)
    feature_keys = ["user_request", "user_context", "history_repr_request", "history_repr_response", "generated_sid_lookup_request", "generated_sid_lookup_response", "item_request", "item_response"]
    check("四次特征RPC共享请求与发布身份", all(sample[key]["request_id"] == REQUEST_ID and sample[key]["release_id"] == RELEASE_ID and sample[key]["schema_version"] == SCHEMA_VERSION for key in feature_keys))
    sid_keys = ["history_repr_request", "history_repr_response", "generation_request", "generated_sid_lookup_request", "generated_sid_lookup_response", "generative_response"]
    check("SID版本贯穿正查生成反查", all(sample[key]["sid_version"] == SID_VERSION for key in sid_keys))
    return {"kind": "illustrative_data_consistency_only", "model_or_service_executed": False,
            "passed": True, "check_count": len(checks), "checks": checks}


if __name__ == "__main__":
    sample = build_sample()
    report = validate(sample)
    folder = Path(__file__).resolve().parent
    for filename, value in [("walkthrough_sample.json", sample), ("walkthrough_checks.json", report)]:
        (folder / filename).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"sample": "walkthrough_sample.json", "checks": report["check_count"],
                      "model_or_service_executed": False}, ensure_ascii=False))
