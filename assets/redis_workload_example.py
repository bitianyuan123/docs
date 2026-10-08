"""Compute the chapter 11 teaching example; no sockets, model calls or load generation.

Run without arguments to write redis_workload_example.json.
Run with --check to compare the saved result without modifying files.
Candidate values are projections, not complete Redis ItemFeatures records.
"""
import argparse
import json
from pathlib import Path


def bulk(data):
    return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"


def array(elements):
    return b"*" + str(len(elements)).encode() + b"\r\n" + b"".join(elements)


def calculate(sample):
    u = sample["user_context"]
    header = {name: u[name] for name in ("release_id", "schema_version")}
    p = "rec:qkv:" + header["release_id"]
    uid = sample["user_request"]["user_id"]
    sid = sample["history_repr_request"]["sid_version"]
    user_keys = [p + ":user:" + uid, p + ":history:" + uid,
                 p + ":user_rep:dense:" + u["dense_query"]["embedding_space_id"] + ":" + uid,
                 p + ":user_rep:sparse:" + u["sparse_query"]["sparse_recipe_id"] + ":" + uid]
    batches = [("user", user_keys,
                [u[name] for name in ("user", "history", "dense_query", "sparse_query")])]
    forward = sample["history_repr_response"]["results"]
    assert all(r["status"] == "FOUND" for r in forward)
    batches.append(("history_sid",
                    [p + ":item_rep:" + sid + ":" + r["item_id"] for r in forward],
                    [r["value"] for r in forward]))
    reverse = sample["generated_sid_lookup_response"]["results"]
    assert all(r["status"] == "FOUND" for r in reverse)
    batches.append(("generated_sid",
                    [p + ":sid_map:" + sid + ":" + ".".join(map(str, r["semantic_id"])) for r in reverse],
                    [dict(header, sid_version=sid, semantic_id=r["semantic_id"], item_ids=r["item_ids"])
                     for r in reverse]))
    items = sample["item_response"]["results"]
    batches.append(("item_projection",
                    [p + ":item:" + r["item_id"] for r in items],
                    [dict(header, item_id=r["item_id"], **r["value"]) if r["status"] == "FOUND" else None
                     for r in items]))
    rows = []
    for name, keys, records in batches:
        assert len(keys) == len(records)
        values = [None if x is None else json.dumps(
            x, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8") for x in records]
        request = array([bulk(b"MGET")] + [bulk(k.encode("utf-8")) for k in keys])
        reply = array([b"$-1\r\n" if v is None else bulk(v) for v in values])
        rows.append({"name": name, "keys": keys, "key_count": len(keys),
                     "value_bytes_by_position": [None if v is None else len(v) for v in values],
                     "found_count": sum(v is not None for v in values),
                     "missing_count": sum(v is None for v in values),
                     "json_value_bytes": sum(len(v) for v in values if v is not None),
                     "key_bytes": sum(len(k.encode("utf-8")) for k in keys),
                     "resp2_request_bytes": len(request), "resp2_reply_bytes": len(reply),
                     "candidate_projection_lower_bound": name == "item_projection"})
    totals = {field: sum(row[field] for row in rows) for field in (
        "key_count", "found_count", "missing_count", "json_value_bytes", "key_bytes",
        "resp2_request_bytes", "resp2_reply_bytes")}
    totals["mget_commands_single_instance_without_splitting"] = len(rows)
    totals["resp2_both_directions_bytes"] = totals["resp2_request_bytes"] + totals["resp2_reply_bytes"]
    return {"source": "walkthrough_sample.json", "kind": "illustrative_byte_accounting",
            "service_executed": False, "protocol": "RESP2", "json_encoding": "compact UTF-8, no compression",
            "note": "Candidate values contain only header, item_id and response projections; full stored records are larger. No TCP/TLS/IP or Redis object-memory overhead is counted.",
            "batches": rows, "totals": totals}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    folder = Path(__file__).resolve().parent
    result = calculate(json.loads((folder / "walkthrough_sample.json").read_text(encoding="utf-8")))
    target = folder / "redis_workload_example.json"
    if args.check:
        assert result == json.loads(target.read_text(encoding="utf-8")), "Saved calculation differs from current sample"
    else:
        target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"check": args.check, **result["totals"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
