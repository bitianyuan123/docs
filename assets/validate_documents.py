#!/usr/bin/env python3
"""默认仅校验仓库内文档与证据；显式指定源工作区时重新检查真实输入。"""
import argparse
import ast
import csv
import hashlib
import json
import math
import re
import runpy
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parent.parent
ASSETS = ROOT / "assets"
errors = []
checks = {}


def require(condition, message):
    if not condition:
        errors.append(message)


def close(a, b):
    return math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)


def verify_real_input(e, source_workspace):
    previous_errors = len(errors)
    source = source_workspace / "Tenrec" / Path(e["source"]).name
    require(source.is_file(), f"真实数据文件不存在: {source}")
    if not source.is_file():
        checks["real_input_reverified"] = {"status": "failed", "reason": "source_file_missing"}
        return
    hash_state = hashlib.sha256()
    target_ids = {c["item_id"] for c in e["candidate_examples"]}
    wanted = set(e["history_item_ids"]) | target_ids
    rows_by_target = {i: [] for i in target_ids}
    types = {i: set() for i in wanted}
    users, targets = set(), set()
    first = None
    with source.open("rb") as stream:
        header_bytes = stream.readline()
        hash_state.update(header_bytes)
        header = next(csv.reader([header_bytes.decode().strip()]))
        for index in range(e["scope"]["data_rows"]):
            raw = stream.readline()
            require(bool(raw), "真实样例文件提前结束")
            if not raw:
                checks["real_input_reverified"] = {"status": "failed", "reason": "source_file_truncated"}
                return
            hash_state.update(raw)
            row = dict(zip(header, next(csv.reader([raw.decode().strip()]))))
            if first is None:
                first = row
            users.add(row["user_id"])
            targets.add(row["item_id"])
            item_id = row["item_id"]
            if item_id in types and row["video_category"] in ("0", "1"):
                types[item_id].add(int(row["video_category"]))
            if item_id in rows_by_target:
                rows_by_target[item_id].append(row)
    require(hash_state.hexdigest() == e["scope"]["sample_bytes_sha256"], "前20万行字节SHA不符")
    require(first == e["user_source"]["first_raw_row"], "第一行数据不符")
    require(len(users) == e["sample_counts"]["users"], "样例用户数不符")
    require(len(targets) == e["sample_counts"]["target_items"], "样例物品数不符")
    observed = []
    for meta in e["history_metadata"]:
        actual = types[meta["item_id"]]
        require(len(actual) <= 1, f"样例类型冲突: {meta['item_id']}")
        code = next(iter(actual)) if actual else None
        require(code == meta["category_code"], f"历史类型不符: {meta['item_id']}")
        if code is not None:
            observed.append(code)
    for token in e["sparse_tokens"]:
        code = int(token["token"].removeprefix("video_type_"))
        require(close(token["weight"], observed.count(code)/len(observed)), "词项权重不符")

    for c in e["candidate_examples"]:
        rows = rows_by_target[c["item_id"]]
        feedback = {field: [int(r[field]) for r in rows if r[field] in ("0", "1")]
                    for field in ("click", "like", "follow", "share")}
        watch = [float(r["watching_times"]) for r in rows if r["watching_times"] != "\\N"]
        genders = [int(r["gender"]) for r in rows if r["gender"] != "\\N"]
        ages = [int(r["age"]) for r in rows if r["age"] != "\\N"]
        avg = lambda xs: sum(xs)/len(xs) if xs else 0.0
        derived = [len(rows)] + [sum(feedback[f]) for f in ("click", "like", "follow", "share")]
        derived += [avg(feedback[f]) for f in ("like", "follow", "share", "click")]
        derived += [avg(watch), max(watch, default=0), sum(watch),
                    genders.count(1)/len(genders) if genders else 0.0,
                    avg(ages), math.log1p(sum(watch))]
        require(len(derived) == 15 and all(close(a,b) for a,b in zip(derived,c["candidate_dense"])),
                f"候选15维重新聚合不符: {c['item_id']}")
    checks["real_input_reverified"] = {"status": "passed" if len(errors) == previous_errors else "failed", "source": str(source),
        "rows": e["scope"]["data_rows"], "sha256": hash_state.hexdigest(),
        "users": len(users), "target_items": len(targets),
        "candidate_aggregates_recomputed": sorted(target_ids)}


def markdown_targets(text):
    # Fenced code is illustrative data, not a set of actual document links.
    prose = re.sub(r"^```[^\n]*\n.*?^```[ \t]*$", "", text, flags=re.M | re.S)
    for match in re.finditer(r'!?\[[^\]\n]*\]\(\s*(<[^>]+>|[^\s)]+)(?:\s+"[^"]*")?\s*\)', prose):
        yield match[1].strip("<>")


def validate_local_link(doc, target):
    parts = urlsplit(target)
    if parts.scheme or parts.netloc:
        require(parts.scheme != "file", f"本机file链接不可移植: {doc.name}: {target}")
        return False
    raw = unquote(parts.path)
    # #L123 is the portable source-reference convention. Old :123 paths are
    # rejected by existence checks after links have been migrated.
    require(not Path(raw).is_absolute(), f"本地链接应使用相对路径: {doc.name}: {target}")
    path = (doc.parent / raw).resolve() if raw else doc.resolve()
    try:
        path.relative_to(ROOT)
    except ValueError:
        require(False, f"引用越出文档仓库: {doc.name}: {target}")
        return True
    require(path.exists(), f"引用不存在: {doc.name}: {target}")
    line = re.fullmatch(r"L(\d+)(?:-L?(\d+))?", parts.fragment)
    if line and path.is_file():
        first, last = int(line[1]), int(line[2] or line[1])
        if path.suffix.lower() in (".html", ".htm"):
            snapshot = path.read_text(errors="replace")
            for number in (first, last):
                require(bool(re.search(r'id=[\'"]L' + str(number) + r'[\'"]', snapshot)),
                        f"源码HTML没有行锚点L{number}: {doc.name}: {target}")
            require(first <= last, f"行号范围反转: {doc.name}: {target}")
        else:
            with path.open(errors="replace") as stream:
                line_count = sum(1 for _ in stream)
            require(1 <= first <= last <= line_count, f"行号越界: {doc.name}: {target}")
    return True


def verify_file_manifest(manifest_path):
    manifest = json.loads(manifest_path.read_text())
    files = manifest["files"]
    for item in files:
        path = manifest_path.parent / item["file"]
        require(path.is_file(), f"清单文件缺失: {item['file']}")
        if path.is_file():
            require(hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"],
                    f"清单文件hash不符: {item['file']}")
    return len(files)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-workspace", type=Path,
                        help="含Tenrec与四个源码仓库的目录；显式开启真实数据重新聚合和源码状态检查")
    parser.add_argument("--report", default=str(ASSETS / "validation.json"),
                        help="检查报告路径；设为-时只输出，不写文件")
    args = parser.parse_args()
    docs = sorted(ROOT.glob("*.md")) + sorted(ASSETS.rglob("*.md"))
    links, diagrams = 0, 0
    for doc in docs:
        text = doc.read_text()
        require(len(re.findall(r"^```", text, flags=re.M)) % 2 == 0, f"围栏不成对: {doc}")
        diagrams += len(re.findall(r"^```mermaid\s*$", text, flags=re.M))
        for target in markdown_targets(text):
            if validate_local_link(doc, target):
                links += 1
        for i, payload in enumerate(re.findall(r"```json\n(.*?)\n```", text, flags=re.S)):
            try:
                json.loads(payload)
            except Exception as exc:
                errors.append(f"JSON代码块: {doc.name}:{i}: {exc}")
    checks.update(documents=len(docs), local_links=links, mermaid_blocks=diagrams)

    for number in range(1, 8):
        file = next(ROOT.glob(f"{number:02d}_*.md"))
        text = file.read_text()
        for view in ["逻辑视图", "开发视图", "进程视图", "物理视图", "场景视图"]:
            require(view in text, f"缺少{view}: {file.name}")
    checks["five_views_present_in_all_module_documents"] = True

    json_count = 0
    for path in ASSETS.rglob("*.json"):
        if path.name in ("validation.json", "render_validation.json"):
            continue
        try:
            json.loads(path.read_text())
            json_count += 1
        except Exception as exc:
            errors.append(f"JSON文件: {path}: {exc}")
    checks["json_files"] = json_count
    for path in ASSETS.glob("*.py"):
        try:
            ast.parse(path.read_text(), filename=str(path))
        except SyntaxError as exc:
            errors.append(f"Python语法: {path}: {exc}")

    checks["copied_assets_verified"] = verify_file_manifest(ASSETS / "source_manifest.json")
    checks["vendor_assets_verified"] = verify_file_manifest(ASSETS / "vendor" / "dependencies.json")
    snapshot_manifest = ASSETS / "source_snapshots" / "manifest.json"
    if snapshot_manifest.is_file():
        checks["source_snapshot_files_verified"] = verify_file_manifest(snapshot_manifest)

    e = json.loads((ASSETS / "tenrec_sample_evidence.json").read_text())
    history = {"item_ids":e["history_item_ids"], "positions":e["history_positions"], "time_semantics":"ordinal"}
    history_hash = hashlib.sha256(json.dumps(history,sort_keys=True,separators=(",", ":")).encode()).hexdigest()
    prepare = json.loads((ASSETS/"archive_v2/api_examples/04_prepare.request.json").read_text())
    require(history_hash == prepare["history_hash"], "历史指纹不符")
    require(len(e["user_dense"]) == len(e["candidate_dense_fields"]) == 15, "15维字段长度不符")
    checks["stored_history_evidence"] = {"history_hash": history_hash,
        "compared_with_archived_request": True}
    if args.source_workspace:
        verify_real_input(e, args.source_workspace.resolve())
    else:
        checks["real_input_reverified"] = {"status": "not_run",
            "reason": "默认仅检查随仓库保存的证据；未重新读取Tenrec源文件。使用--source-workspace启用。"}


    # 本轮示例不伪造模型资产，并与实际数据证据分开。
    examples = ASSETS / "feature_examples"
    context_query = json.loads((examples/"02_get_user_context.request.json").read_text())
    context_result = json.loads((examples/"02_get_user_context.response.example.json").read_text())
    for view, field in [("USER", "user"), ("HISTORY", "history"),
                        ("DENSE_QUERY", "dense_query"), ("SPARSE_QUERY", "sparse_query")]:
        state = context_result["view_status"][view]
        require(state == ("FOUND" if view in context_query["required_views"] else "NOT_REQUESTED"),
                f"上下文状态不对应请求: {view}")
        require((context_result[field] is None) == (state != "FOUND"), f"上下文值与状态不符: {view}")
    require(context_result["history"]["history_hash"] == history_hash, "本轮历史hash不一致")
    require(context_result["sparse_query"]["history_hash"] == history_hash, "词项与历史hash不一致")
    require(context_result["user"]["gender_code"] == 1 and context_result["user"]["age_code"] == 4, "用户值错误")
    require(context_result["sparse_query"]["tokens"] == e["sparse_tokens"], "当前词项示例不符")
    candidate_query = json.loads((examples/"05_get_item_features.request.json").read_text())
    candidate_result = json.loads((examples/"05_get_item_features.response.example.json").read_text())
    require([x["item_id"] for x in candidate_result["results"]] == candidate_query["item_ids"], "候选响应顺序错误")
    for result, original in zip(candidate_result["results"], e["candidate_examples"]):
        require(set(result["value"]) == set(candidate_query["fields"]), "候选投影字段不符")
        stats = result["value"]["statistics"]
        require(all(close(stats[k], v) for k,v in zip(e["candidate_dense_fields"], original["candidate_dense"])),
                "新命名统计值与保存的数据证据不符")
    ingest = json.loads((examples/"06_onetrans_ingest.request.example.json").read_text())
    rank = json.loads((examples/"07_onetrans_rank.request.example.json").read_text())
    require(set(ingest) == {"user_id", "item_ids", "timestamps"}, "OneTrans ingest形状越界")
    require(ingest["timestamps"] == list(range(len(ingest["item_ids"]))), "当前调用方生成的ingest占位序号应从0开始")
    require(set(rank) == {"request_id", "user_id", "items"}, "OneTrans rank形状越界")
    require(all(set(x) == {"item_id"} for x in rank["items"]), "当前rank不能混入目标特征字段")
    require("FeatureRepository" not in (ASSETS/"contracts.py").read_text(), "当前类型仍引用旧特征直连模块")
    checks["current_feature_examples"] = {"files":len(list(examples.glob("*.json"))),
        "values_match_stored_evidence":True, "onetrans_id_only_contract":True,
        "no_model_outputs_or_deployment_claim":True}

    # 教学样例可使用示意值，但正文、附件与逐阶段字段必须自洽。
    namespace = runpy.run_path(str(ASSETS / "walkthrough_sample.py"))
    walkthrough = json.loads((ASSETS / "walkthrough_sample.json").read_text())
    workload_tools = runpy.run_path(str(ASSETS / "redis_workload_example.py"))
    workload = workload_tools["calculate"](walkthrough)
    saved_workload = json.loads((ASSETS / "redis_workload_example.json").read_text())
    require(workload == saved_workload, "Redis字节测算与当前教学样例不一致")
    require(workload["service_executed"] is False, "Redis测算不能标为实际服务执行")
    checks["redis_workload_accounting"] = {
        "matches_current_sample": workload == saved_workload,
        "scope": "RESP2教学样例；候选仅字段投影；未执行Redis",
        "totals": workload["totals"],
    }
    require(walkthrough == namespace["build_sample"](), "教学JSON未与构造文件同步")
    sample_report = namespace["validate"](walkthrough)
    require(sample_report["passed"], "教学样例关系检查失败")
    checks["illustrative_walkthrough"] = sample_report
    concrete = ASSETS / "request_example"
    concrete_manifest = json.loads((concrete / "manifest.json").read_text())
    require(concrete_manifest["kind"] == "illustrative_not_model_run", "具体接口样例缺少教学标记")
    for call in concrete_manifest["calls"]:
        for kind in ("request", "response"):
            payload = json.loads((concrete / call[f"{kind}_file"]).read_text())
            require(payload == walkthrough[call[f"{kind}_sample_key"]],
                    f"具体接口样例与主样例不一致: {call['id']} {kind}")
    checks["concrete_request_response_pairs"] = len(concrete_manifest["calls"])
    python_blocks = 0
    for filename in ("01_system.md", "08_request_walkthrough.md"):
        document = (ROOT / filename).read_text()
        for block in re.findall(r"```python\n(.*?)\n```", document, flags=re.S):
            ast.parse(block, filename=filename)
            python_blocks += 1
    walkthrough_text = (ROOT / "08_request_walkthrough.md").read_text()
    public_examples = [json.loads(block) for block in re.findall(r"```json\n(.*?)\n```", walkthrough_text, flags=re.S)]
    final_example = next(x for x in public_examples if "returned_size" in x)
    rank_example = next(x for x in public_examples if "trace" in x)
    sparse_input = next(x for x in public_examples if "sparse_tokens" in x)
    sparse_output = next(x for x in public_examples if x.get("score_semantics") == "bm25")
    require(sparse_input == walkthrough["sparse_request"], "正文具体稀疏请求与附件不一致")
    require(sparse_output == walkthrough["sparse_response"], "正文具体稀疏响应与附件不一致")
    require(final_example == walkthrough["final_response"], "正文最终响应与附件不一致")
    for key, value in rank_example.items():
        if key == "trace":
            require(all(walkthrough["rank_response"][key][k] == v for k,v in value.items()), "正文精排trace不符")
        else:
            require(value == walkthrough["rank_response"][key], "正文精排响应字段不符: " + key)
    checks["main_document_examples"] = {"python_blocks_parsed": python_blocks,
        "rank_and_final_json_match_attachment": True}

    if args.source_workspace:
        git = {}
        for name in ("pairec4tigerllm", "pairec4tigerllm_8506", "pairec_sh", "OneTrans_HSE_project"):
            path = args.source_workspace.resolve() / name
            if not (path / ".git").exists():
                require(False, f"源仓库不存在或不是git checkout: {path}")
                continue
            git[name] = {"head": subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip(),
                         "status": subprocess.check_output(["git", "-C", str(path), "status", "--short"], text=True).strip()}
            require(not git[name]["status"], f"原工程非干净状态（需人工辨别来源）: {name}")
        checks["source_repositories"] = {"status": "checked", "repositories": git}
    else:
        checks["source_repositories"] = {"status": "not_run",
            "reason": "未指定--source-workspace；不要求本机存在四个源工程。"}

    report = {"scope": "仓库文档与证据静态校验；不执行模型或在线服务",
              "mode": "with_source_workspace" if args.source_workspace else "repository_only",
              "real_data_reverification_requested": bool(args.source_workspace), "ok":not errors,
              "checks":checks, "errors":errors}
    if args.report != "-":
        Path(args.report).write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps(report,ensure_ascii=False,indent=2))
    raise SystemExit(bool(errors))


if __name__ == "__main__":
    main()
