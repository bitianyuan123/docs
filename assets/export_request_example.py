"""Export concrete request/response JSON from the existing teaching fixture.

Run from any directory: python3 assets/export_request_example.py
This exports data only; it does not send requests or execute models.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CALLS = [
    ("01_recommend", "客户端经Nginx", "PaiRec", "POST /api/recommend", "external_request", "final_response"),
    ("02_user_context", "PaiRec", "特征服务", "GetUserContext", "user_request", "user_context"),
    ("03_history_sid", "PaiRec", "特征服务", "BatchGetItemRepresentations(raw_item_id)", "history_repr_request", "history_repr_response"),
    ("04_vector", "PaiRec", "向量召回", "Recall", "vector_request", "vector_response"),
    ("05_sparse", "PaiRec", "稀疏召回", "Recall", "sparse_request", "sparse_response"),
    ("06_generate", "PaiRec", "生成召回", "Recommend", "generation_request", "generation_wire_response"),
    ("07_generated_sid", "生成召回", "特征服务", "BatchGetItemRepresentations(semantic_id)", "generated_sid_lookup_request", "generated_sid_lookup_response"),
    ("08_item_features", "PaiRec", "特征服务", "BatchGetItemFeatures", "item_request", "item_response"),
    ("09_ingest", "PaiRec历史任务", "OneTrans历史", "POST /ingest", "ingest_request", "ingest_response"),
    ("10_rank", "PaiRec", "OneTrans候选", "POST /rank", "rank_request", "rank_response"),
]


def export():
    sample = json.loads((ROOT / "walkthrough_sample.json").read_text(encoding="utf-8"))
    output = ROOT / "request_example"
    output.mkdir(exist_ok=True)
    calls = []
    for call_id, caller, callee, method, request_key, response_key in CALLS:
        entry = {"id": call_id, "caller": caller, "callee": callee, "method": method}
        for kind, key in (("request", request_key), ("response", response_key)):
            filename = f"{call_id}.{kind}.json"
            (output / filename).write_text(json.dumps(sample[key], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            entry[f"{kind}_file"] = filename
            entry[f"{kind}_sample_key"] = key
        calls.append(entry)
    manifest = {"kind": "illustrative_not_model_run", "source": "../walkthrough_sample.json",
                "request_id": "demo-user-1-001", "calls": calls}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Exported {len(calls)} complete request/response pairs to {output.name}/")


if __name__ == "__main__":
    export()
