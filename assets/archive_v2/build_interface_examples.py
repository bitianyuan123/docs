#!/usr/bin/env python3
"""Build documentation requests from audited Tenrec rows; never invent vectors or SIDs.

Default: write clearly labelled unbound templates. With --binding, --user-vectors,
--sid-map: require real exporter artifacts and produce concrete wire-body examples.
This is a documentation helper, not a production ETL, deployment or RPC runner.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binding", type=Path)
    p.add_argument("--user-vectors", type=Path)
    p.add_argument("--sid-map", type=Path)
    p.add_argument("--out", type=Path, default=ROOT / "api_examples")
    args = p.parse_args()
    if any((args.binding, args.user_vectors, args.sid_map)) and not all((args.binding, args.user_vectors, args.sid_map)):
        p.error("bound examples require ALL of --binding, --user-vectors and --sid-map; no random fallback")
    e = read(ROOT / "tenrec_sample_evidence.json")
    uid = e["user_id"]
    # Hash canonical JSON including semantic marker; do not call positions real times.
    hist = {"item_ids": e["history_item_ids"], "positions": e["history_positions"], "time_semantics": "ordinal"}
    history_hash = hashlib.sha256(json.dumps(hist, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    release = "qkv_doc200k_" + e["scope"]["sample_bytes_sha256"][:12]
    model = "${ONETRANS_MODEL_VERSION}"
    embedding = "${EMBEDDING_SPACE_ID}"
    query_vector = {"$artifact": "actual DSSM user-vector export", "user_id": uid, "dimension": "checkpoint.config.out_dim"}
    sid_history = {"$artifact": "deployed raw-item-to-SID map", "raw_item_ids": e["history_item_ids"], "width": 4}
    provenance = {"bound": False, "note": "Templates are NOT wire-valid until actual artifacts are bound; no RPC was made."}
    if args.binding:
        b, uv, sm = read(args.binding), read(args.user_vectors), read(args.sid_map)
        assert b["release_id"] == release, "fixture release mismatch: regenerate evidence for other scopes"
        assert b["sample_bytes_sha256"] == e["scope"]["sample_bytes_sha256"], "source scope mismatch"
        assert b["data_semantics_revision"] == e["data_semantics_revision"] == 1
        assert b["sparse_recipe"] == e["sparse_recipe"] == "video_type_binary_v1"
        assert b["user_vectors_sha256"] == sha(args.user_vectors), "user-vector artifact checksum mismatch"
        assert b["sid_map_sha256"] == sha(args.sid_map), "SID artifact checksum mismatch"
        model, embedding = b["onetrans_model_version"], b["embedding_space_id"]
        assert model and embedding and "${" not in model + embedding
        assert uv["release_id"] == sm["release_id"] == release
        assert uv["embedding_space_id"] == embedding
        assert uv["checkpoint_sha256"] == b["dssm_checkpoint_sha256"]
        assert sm["sid_version"] == b["sid_version"]
        assert uv["dimension"] == b["dssm_dimension"] > 0
        query_vector = uv["vectors"][uid]
        assert len(query_vector) == b["dssm_dimension"]
        assert all(type(v) in (float, int) and math.isfinite(v) for v in query_vector)
        assert abs(sum(v * v for v in query_vector) - 1.0) < 1e-3, "require normalized DSSM vector"
        assert b["sid_width"] == 4 and b["sid_codebook_size"] > 0
        missing, sid_history = [], []
        for item in e["history_item_ids"]:
            if item not in sm["raw_item_to_sid"]:
                missing.append(item)
                continue
            sid = sm["raw_item_to_sid"][item]
            assert len(sid) == 4 and all(type(x) is int and 0 <= x < b["sid_codebook_size"] for x in sid)
            sid_history.append({"value": sid})
        max_items = min(10, int(b["gen_max_history_items"]))
        assert max_items > 0
        sid_history = sid_history[-max_items:]
        assert sid_history, "no valid SID history; must not invent fallback"
        provenance = {"bound": True, "binding_sha256": sha(args.binding), "user_vectors_sha256": sha(args.user_vectors),
                      "sid_map_sha256": sha(args.sid_map), "sid_missing_history_ids": missing,
                      "note": "Checks artifact claims and shape only; still not proof of online execution or model provenance."}
    reqid = "doc_uid1_0001"  # docs only; real ingress issues globally unique IDs.
    common = {"request_id": reqid, "release_id": release}
    fine = {**common, "model_version": model, "history_hash": history_hash,
            "kv_key": f"fine:{release}:{model}:{reqid}"}
    bodies = {
        "01_dense": {**common, "embedding_space_id": embedding, "query_vector": query_vector, "topk": 50},
        "02_sparse": {**common, "sparse_tokens": e["sparse_tokens"], "topk": 50},
        "03_generative": {"user_id": uid, "history": sid_history, "topk": 10, "temperature": 1.0, "beam_width": 1, "request_id": reqid},
        "04_prepare": {**fine, "user_id": uid, "item_ids": list(map(int, e["history_item_ids"])),
                       "timestamps": e["history_positions"], "time_semantics": "ordinal"},
        "05_rank": {**fine, "user_id": uid, "uid_sparse": int(uid), "user_dense": e["user_dense"],
                    "candidates": [{"item_id": int(c["item_id"]), "artist_ids": [] if c["category_code"] is None else [c["category_code"] + 1],
                                    "album_ids": [], "dense": c["candidate_dense"]} for c in e["candidate_examples"]]}
    }
    for name, body in bodies.items():
        write(args.out / (name + ".request.json"), body)
    write(args.out / "example_provenance.json", {**provenance, "release_id": release, "history_hash": history_hash,
          "input_evidence": "../tenrec_sample_evidence.json", "candidate_note": e["candidate_examples_note"],
          "design_revision": 2,
          "data_semantics_revision": e["data_semantics_revision"],
          "category_semantics": e["category_semantics"],
          "code_label_mapping": e["code_label_mapping"],
          "sparse_recipe": e["sparse_recipe"],
          "transport": {"01_dense": "native adapter DenseRecall -> new loadmodel.v1.DenseRecallService.Recall -> local HTTP POST /recall",
                        "02_sparse": "native adapter SparseRecall -> new loadmodel.v1.SparseRecallService.Recall -> local HTTP POST /recall",
                        "03_generative": "native adapter GenerativeRecall -> existing pairec.inference.RecommendService.Recommend; business JSON is not a REST endpoint",
                        "04_prepare": "native adapter PrepareHistory -> new loadmodel.v1.HistoryPrepareService.Prepare -> local HTTP POST /ingest",
                        "05_rank": "native adapter ScoreCandidates -> new loadmodel.v1.CandidateScoreService.Score -> local HTTP POST /score"}})
    print(json.dumps({"out": str(args.out), "bound": provenance["bound"], "requests": 5}))


if __name__ == "__main__":
    main()
