#!/usr/bin/env python3
"""Render local architecture Markdown and Mermaid without remote document APIs.

Use the active Python environment and Playwright Chromium by default. Optional
ARCHITECTURE_RENDER_PYTHONPATH and browser overrides support custom installations.
Mermaid and fonts are pinned local assets bundled with these documents.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import html
import importlib.metadata
import json
import os
from pathlib import Path
import re
import sys
import threading
import time
import tempfile
from datetime import datetime, timezone
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, unquote, urlsplit, urlunsplit

EXTRA = os.environ.get("ARCHITECTURE_RENDER_PYTHONPATH")
if EXTRA:
    for entry in reversed(EXTRA.split(os.pathsep)):
        if entry:
            sys.path.insert(0, entry)

import markdown
from fontTools import subset
from fontTools.ttLib import TTFont
from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parent.parent
VENDOR = ROOT / "assets" / "vendor"
MERMAID = re.compile(r"^```mermaid[^\n]*\n(.*?)^```[ \t]*$", re.M | re.S)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def slug(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name)


def read_documents() -> list[dict]:
    paths = sorted(ROOT.glob("*.md"), key=lambda p: (p.name != "README.md", p.name))
    return [{"path": p, "name": p.name, "stem": p.stem,
             "text": p.read_text(encoding="utf-8"), "sha256": sha(p.read_bytes())}
            for p in paths]


def verify_dependencies() -> dict:
    manifest = json.loads((VENDOR / "dependencies.json").read_text())
    for entry in manifest["files"]:
        path = VENDOR / entry["file"]
        if not path.is_file() or sha(path.read_bytes()) != entry["sha256"]:
            raise RuntimeError(f"Pinned dependency missing or changed: {path}")
    return manifest


def build_subset_font(documents: list[dict]) -> dict:
    text = "".join(d["text"] for d in documents)
    text += "真实推荐系统架构视图文档目录查看源码适合宽度实际尺寸导出矢量图已验证离线版"
    text += "".join(chr(i) for i in range(32, 127))
    options = subset.Options()
    options.flavor = "woff2"
    options.desubroutinize = True
    font = TTFont(VENDOR / "NotoSansCJKsc-Regular.otf")
    font.recalcTimestamp = False
    subsetter = subset.Subsetter(options=options)
    subsetter.populate(text=text)
    subsetter.subset(font)
    font.flavor = "woff2"
    output = VENDOR / "NotoSansCJKsc-architecture.woff2"
    font.save(output)
    return {"file": str(output.relative_to(ROOT)), "sha256": sha(output.read_bytes()),
            "bytes": output.stat().st_size, "unique_input_characters": len(set(text))}


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass


def graph_container(key: str, svg: str, source: str) -> str:
    return (f'<figure class="diagram" id="figure-{key}">'
            f'<div class="diagram-tools"><span>{html.escape(key)}</span>'
            '<button type="button" data-size="fit">适合宽度</button>'
            '<button type="button" data-size="actual">实际尺寸</button>'
            f'<a href="rendered/{key}.svg" target="_blank">导出 SVG</a></div>'
            f'<div class="diagram-canvas">{svg}</div>'
            '<details class="mermaid-source"><summary>查看 Mermaid 源码</summary>'
            f'<pre><code>{html.escape(source)}</code></pre></details></figure>')


CSS = """
@font-face {font-family:ArchitectureCJK;src:url('assets/vendor/NotoSansCJKsc-architecture.woff2') format('woff2');font-weight:100 900;font-display:swap}
:root{color-scheme:light;--ink:#183247;--muted:#607183;--line:#dce5ed;--bg:#f5f8fb;--accent:#215d8d}
*{box-sizing:border-box}html{scroll-padding-top:30px}body{margin:0;color:var(--ink);background:var(--bg);font:15px/1.8 ArchitectureCJK,system-ui,sans-serif}
a{color:var(--accent);text-underline-offset:3px}button{font:inherit;cursor:pointer;border:1px solid #b8cbdc;border-radius:5px;background:white;color:var(--accent);padding:2px 9px}
.sidebar{position:fixed;inset:0 auto 0 0;width:276px;padding:25px 21px;overflow:auto;background:#102d43;color:#e6f0f7}
.sidebar .brand{font-size:19px;font-weight:bold;line-height:1.5;margin:0 0 8px}.sidebar .meta{font-size:12px;color:#a9c1d4;margin-bottom:22px}
.sidebar a{display:block;color:#dcecf7;text-decoration:none;padding:6px 8px;border-left:2px solid transparent;line-height:1.6}.sidebar a:hover,.sidebar a.active{background:#1f435e;border-color:#67b3e7}.sidebar .nav-sub{font-size:12px;padding-left:18px;color:#b8cfdf}
main{margin-left:276px;padding:25px 40px 90px;max-width:1800px}.intro{border:1px solid var(--line);border-radius:8px;padding:18px 23px;background:white;margin-bottom:28px}
.intro strong{font-size:20px}.intro p{margin:7px 0;color:var(--muted)}.badge{display:inline-block;background:#e4f3ee;color:#28604c;padding:0 8px;border-radius:4px;font-size:12px;margin-left:10px}
.document{background:white;border:1px solid var(--line);border-radius:8px;padding:25px 30px;margin:0 0 32px;overflow:hidden;min-width:0}.document h1{font-size:27px;line-height:1.5;border-bottom:2px solid #dde9f2;padding-bottom:17px;margin:5px 0 22px}.document h2{font-size:22px;margin:32px 0 15px}.document h3{font-size:18px;margin:26px 0 12px}.document h4{font-size:16px;margin:22px 0 10px}
.document p{margin:12px 0}.document li{margin:5px 0}.document pre{background:#f2f6fa;border:1px solid #dce5ed;border-radius:5px;padding:14px 16px;overflow:auto;line-height:1.55;font-size:13px}.document code{font-family:ui-monospace,SFMono-Regular,Consolas,ArchitectureCJK,monospace}.document :not(pre)>code{background:#eef3f8;padding:1px 4px;border-radius:3px;font-size:.93em}
.table-wrap{overflow:auto;margin:18px 0}table{border-collapse:collapse;width:100%;font-size:13px;line-height:1.65}th,td{border:1px solid var(--line);padding:9px 11px;vertical-align:top;text-align:left}th{background:#eaf1f7}tr:nth-child(even) td{background:#fafcfe}.document blockquote{border-left:4px solid #94b6cf;margin:18px 0;padding:4px 16px;color:var(--muted);background:#f7fafd}
.diagram{margin:20px 0;border:1px solid #cbdce9;border-radius:7px;background:#fbfdff;overflow:hidden}.diagram-tools{display:flex;gap:8px;align-items:center;flex-wrap:wrap;border-bottom:1px solid #dce8f1;padding:8px 12px;background:#edf4fa;font-size:12px}.diagram-tools span{margin-right:auto;color:#5e7890;font-family:ui-monospace,monospace}.diagram-tools a{text-decoration:none;padding:2px 6px}
.diagram-canvas{overflow:auto;padding:20px;text-align:center}.diagram-canvas svg{display:block;max-width:none;height:auto;margin:0 auto}.diagram.fit .diagram-canvas svg{max-width:100%}.diagram-canvas text{font-family:ArchitectureCJK,sans-serif!important}.mermaid-source{border-top:1px solid #dce8f1;padding:8px 12px;font-size:13px}.mermaid-source summary{cursor:pointer;color:#446a88}.mermaid-source pre{margin:8px 0 0}
.build-note{font-size:12px;color:#637b8e}.doc-origin{font-size:12px;color:#647f95;text-align:right;margin-bottom:12px}.doc-origin a{text-decoration:none}
@media(max-width:1000px){.sidebar{width:222px;padding:20px 12px}main{margin-left:222px;padding:20px 18px}.document{padding:20px}}
@media(max-width:700px){.sidebar{position:relative;width:100%;max-height:310px}main{margin:0;padding:15px 10px}.document{padding:17px 13px}.document h1{font-size:23px}}
@media print{.sidebar,.diagram-tools,.mermaid-source{display:none}main{margin:0;padding:0}.document{border:0;padding:0;break-before:page}.diagram-canvas{padding:8px;overflow:visible}.diagram-canvas svg{max-width:100%}}
"""


def rewrite_document_link(target: str, document: Path, document_ids: dict[str, str]) -> str:
    """Resolve links from their Markdown file into the combined reader's location."""
    parts = urlsplit(target)
    if parts.scheme == "file":
        raise ValueError("Use a repository-relative link instead of file: " + target)
    if parts.scheme or parts.netloc:
        return target
    if not parts.path:
        if parts.fragment:
            return "#" + document_ids[document.name] + "-" + unquote(parts.fragment)
        return target
    source_path = Path(unquote(parts.path))
    # Accept an absolute path inside this checkout for compatibility, but never
    # expose an external host path or turn it into a file: URL in the reader.
    destination = (source_path if source_path.is_absolute() else document.parent / source_path).resolve()
    try:
        relative = destination.relative_to(ROOT)
    except ValueError as error:
        raise ValueError("Local link points outside this documentation checkout: " + target) from error
    if destination.parent == ROOT and destination.name in document_ids:
        fragment = document_ids[destination.name]
        if parts.fragment:
            fragment += "-" + unquote(parts.fragment)
        return "#" + fragment
    return urlunsplit(("", "", quote(relative.as_posix(), safe="/"), parts.query, parts.fragment))


def build_index(documents: list[dict], rendered: dict[str, str], records: list[dict]) -> str:
    nav, sections = [], []
    by_file = {d["name"]: f'doc-{slug(d["stem"])}' for d in documents}
    for d in documents:
        docid = by_file[d["name"]]
        counter = 0

        def substitute(match):
            nonlocal counter
            counter += 1
            key = f'{d["stem"]}_{counter:02d}'
            return "\n\n" + graph_container(key, rendered[key], match.group(1).strip()) + "\n\n"

        text = MERMAID.sub(substitute, d["text"])
        md = markdown.Markdown(extensions=["fenced_code", "tables", "toc", "sane_lists"])
        body = md.convert(text)
        body = re.sub(r'(<h[1-6] id=")([^"]+)(")', lambda m: m[1]+docid+"-"+m[2]+m[3], body)
        body = body.replace("<table>", '<div class="table-wrap"><table>').replace("</table>", "</table></div>")

        def local_link(match):
            target = rewrite_document_link(html.unescape(match[2]), d["path"], by_file)
            return match[1] + '="' + html.escape(target, quote=True) + '"'

        body = re.sub(r'\b(href|src)="([^"]+)"', local_link, body)
        title_match = re.search(r"^#\s+(.+)$", d["text"], re.M)
        title = title_match[1] if title_match else d["stem"]
        nav.append(f'<a href="#{docid}">{html.escape(title)}</a>')
        for tag in md.toc_tokens:
            for child in tag.get("children", []):
                nav.append(f'<a class="nav-sub" href="#{docid}-{html.escape(child["id"],quote=True)}">{html.escape(child["name"])}</a>')
        sections.append(f'<section class="document" id="{docid}"><div class="doc-origin">'
                        f'<a href="{html.escape(d["name"], quote=True)}">原始 Markdown · {html.escape(d["name"])}</a></div>{body}</section>')
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return ("<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">"
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
            "<title>真实推荐系统 · 4+1 架构视图</title><style>"+CSS+"</style></head><body>"
            '<nav class="sidebar" aria-label="文档目录"><div class="brand">真实推荐系统<br>4+1 架构视图</div>'
            f'<div class="meta">离线阅读版 · {len(documents)} 份文档<br>{len(records)} 张已渲染架构图</div>'
            + "".join(nav) + '</nav><main><header class="intro"><strong>系统与模块架构</strong>'
            '<span class="badge">本机渲染验证</span><p>从左侧选择文档。图以原始尺寸展示，可切换适合宽度；源码可展开，SVG 可单独打开。</p>'
            f'<div class="build-note">Mermaid 11.4.1 · 本地中文字体 · 无运行时 CDN · 构建于 {timestamp}</div></header>'
            + "".join(sections) + """</main><script>
document.addEventListener('click',function(e){const b=e.target.closest('[data-size]');if(!b)return;b.closest('.diagram').classList.toggle('fit',b.dataset.size==='fit');});
const navLinks=[...document.querySelectorAll('.sidebar a')];
const observer=new IntersectionObserver(function(entries){for(const x of entries){if(!x.isIntersecting)continue;navLinks.forEach(a=>a.classList.toggle('active',a.hash==='#'+x.target.id));}}, {rootMargin:'0px 0px -70% 0px'});
document.querySelectorAll('.document').forEach(x=>observer.observe(x));
</script></body></html>""")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--screenshot", action="store_true", help="Save an overview screenshot after HTML verification")
    args = parser.parse_args()
    started = time.time()
    dependencies = verify_dependencies()
    documents = read_documents()
    font = build_subset_font(documents)
    diagrams = ROOT / "diagrams"
    output = ROOT / "rendered"
    diagrams.mkdir(exist_ok=True)
    output.mkdir(exist_ok=True)
    jobs = []
    for d in documents:
        for index, match in enumerate(MERMAID.finditer(d["text"]), 1):
            key = f'{d["stem"]}_{index:02d}'
            source = match.group(1).strip()+"\n"
            path = diagrams / (key+".mmd")
            path.write_text(source, encoding="utf-8")
            jobs.append({"key": key, "source": source, "document": d["name"],
                         "source_line": d["text"][:match.start()].count("\n")+1,
                         "source_sha256": sha(source.encode())})
    # 本目录只保存由Markdown生成的图，删除旧修订遗留的序号文件。
    active_keys = {job["key"] for job in jobs}
    for directory, suffix in ((diagrams, ".mmd"), (output, ".svg")):
        for stale in directory.glob("*" + suffix):
            if stale.stem not in active_keys:
                stale.unlink()
    font_data = base64.b64encode((ROOT/font["file"]).read_bytes()).decode()
    standalone_font = ('<style data-architecture-font="embedded">@font-face{font-family:ArchitectureCJK;'
                       f'src:url(data:font/woff2;base64,{font_data}) format("woff2")}}'
                       '</style>')
    handler = partial(QuietHandler, directory=str(ROOT))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_port}"
    records, errors, rendered, console_errors, blocked_network = [], [], {}, [], []
    browser_version = ""
    html_checks = {}
    render_temp = tempfile.TemporaryDirectory(prefix="architecture-render-")
    try:
        with sync_playwright() as p:
            browser_env = dict(os.environ)
            local_lib = os.environ.get("ARCHITECTURE_BROWSER_LIBRARY_PATH")
            if local_lib:
                browser_env["LD_LIBRARY_PATH"] = local_lib + (os.pathsep+browser_env["LD_LIBRARY_PATH"] if browser_env.get("LD_LIBRARY_PATH") else "")
            # Minimal images may contain no system fonts. Mermaid also measures a
            # generic sans-serif fallback, so a CSS webfont alone is insufficient.
            font_config = Path(render_temp.name) / "fonts.conf"
            font_cache = Path(render_temp.name) / "font-cache"
            font_cache.mkdir()
            font_config.write_text('<?xml version="1.0"?><!DOCTYPE fontconfig SYSTEM "fonts.dtd">'
                                   '<fontconfig><dir>'+html.escape(str(VENDOR))+'</dir>'
                                   '<cachedir>'+html.escape(str(font_cache))+'</cachedir>'
                                   '<alias><family>sans-serif</family><prefer><family>Noto Sans CJK SC</family></prefer></alias>'
                                   '<alias><family>serif</family><prefer><family>Noto Sans CJK SC</family></prefer></alias>'
                                   '<alias><family>monospace</family><prefer><family>Noto Sans CJK SC</family></prefer></alias>'
                                   '</fontconfig>')
            browser_env["FONTCONFIG_FILE"] = str(font_config)
            launch_options = {"headless": True, "env": browser_env,
                              "args": ["--no-sandbox", "--disable-dev-shm-usage"]}
            if os.environ.get("ARCHITECTURE_CHROMIUM_EXECUTABLE"):
                launch_options["executable_path"] = os.environ["ARCHITECTURE_CHROMIUM_EXECUTABLE"]
            browser = p.chromium.launch(**launch_options)
            browser_version = browser.version
            context = browser.new_context(viewport={"width": 1600, "height": 1100}, device_scale_factor=1)

            def allow_local(route):
                url = route.request.url
                if url.startswith(origin+"/") or url.startswith("data:") or url == "about:blank":
                    route.continue_()
                else:
                    blocked_network.append(url)
                    route.abort()

            context.route("**/*", allow_local)
            page = context.new_page()
            page.on("pageerror", lambda error: console_errors.append(str(error)))
            page.goto(origin+"/", wait_until="domcontentloaded")
            page.set_content('<html><head><style>@font-face{font-family:ArchitectureCJK;src:url("'
                             +origin+"/"+font["file"]+'") format("woff2")}</style></head>'
                             '<body><div id="work"></div></body></html>')
            page.evaluate('async () => {await document.fonts.load(\'16px ArchitectureCJK\'); await document.fonts.ready;}')
            page.add_script_tag(url=origin+"/assets/vendor/mermaid-11.4.1.min.js")
            page.evaluate("""() => mermaid.initialize({startOnLoad:false,securityLevel:'strict',theme:'base',
                fontFamily:'ArchitectureCJK, sans-serif',fontSize:15,
                flowchart:{htmlLabels:false,useMaxWidth:false,curve:'linear'},
                sequence:{useMaxWidth:false,mirrorActors:false,actorMargin:24,diagramMarginX:18},
                themeVariables:{fontFamily:'ArchitectureCJK, sans-serif',fontSize:'15px',
                  primaryColor:'#e8f2fc',primaryTextColor:'#173348',primaryBorderColor:'#86a8c4',
                  lineColor:'#46667d',secondaryColor:'#eef6f1',tertiaryColor:'#f9f3e9'}})""")
            for job in jobs:
                t0 = time.time()
                try:
                    result = page.evaluate("""async ({id,source}) => {
                        await mermaid.parse(source);
                        const result = await mermaid.render(id, source);
                        const host = document.getElementById('work'); host.innerHTML=result.svg;
                        await document.fonts.ready;
                        const svg=host.querySelector('svg');
                        const box=svg.getBoundingClientRect();
                        const texts=[...svg.querySelectorAll('text')].map(e=>e.textContent);
                        const out={svg:result.svg,width:box.width,height:box.height,
                          text_count:texts.length, has_cjk:texts.some(s=>/[\\u3400-\\u9fff]/.test(s)),
                          font_loaded:document.fonts.check('16px ArchitectureCJK')};
                        host.innerHTML='';return out;
                    }""", {"id": "diagram_"+slug(job["key"]), "source": job["source"]})
                    if result["width"] <= 0 or result["height"] <= 0 or not result["font_loaded"]:
                        raise RuntimeError(f"Invalid rendered geometry or font: {result}")
                    raw_svg = result.pop("svg")
                    rendered[job["key"]] = raw_svg
                    standalone = re.sub(r"(<svg\b[^>]*>)", lambda m: m[1]+standalone_font, raw_svg, count=1)
                    path = output/(job["key"]+".svg")
                    path.write_text(standalone, encoding="utf-8")
                    record = {k:v for k,v in job.items() if k != "source"}
                    record.update(result)
                    record.update(status="passed",svg_sha256=sha(standalone.encode()),
                                  render_seconds=round(time.time()-t0,3))
                    records.append(record)
                    print(f'PASS {job["key"]} {result["width"]:.0f}x{result["height"]:.0f}', flush=True)
                except Exception as error:
                    entry = {k:v for k,v in job.items() if k != "source"}
                    entry.update(status="failed",error=str(error))
                    records.append(entry);errors.append(entry)
                    print(f'FAIL {job["key"]}: {error}', flush=True)
            if not errors:
                index = build_index(documents, rendered, records)
                if re.search(r'(?:href|src)=["\']file:', index, re.I):
                    raise RuntimeError("Generated reader contains a host-specific file: URL")
                (ROOT/"index.html").write_text(index, encoding="utf-8")
                page.goto(origin+"/index.html",wait_until="networkidle")
                page.evaluate("() => document.fonts.ready")
                html_checks = page.evaluate("""() => ({
                    documents:document.querySelectorAll('.document').length,
                    inline_svg:document.querySelectorAll('.diagram-canvas svg').length,
                    source_details:document.querySelectorAll('.mermaid-source').length,
                    font_loaded:document.fonts.check('16px ArchitectureCJK'),
                    broken_internal_anchors:[...document.querySelectorAll('a[href^="#"]')]
                      .filter(a=>!document.getElementById(decodeURIComponent(a.getAttribute('href').slice(1))))
                      .map(a=>a.getAttribute('href')),
                    external_runtime_resources:performance.getEntriesByType('resource')
                      .filter(x=>!x.name.startsWith(location.origin) && !x.name.startsWith('data:')).map(x=>x.name)
                })""")
                if args.screenshot:
                    page.screenshot(path=str(ROOT/"assets"/"render_preview.png"), full_page=False)
                    for screenshot_key in ("01_system_01", "08_request_walkthrough_01"):
                        diagram_record = next(x for x in records if x["key"] == screenshot_key)
                        diagram_page = context.new_page()
                        diagram_page.set_viewport_size({
                            "width": int(diagram_record["width"])+32,
                            "height": int(diagram_record["height"])+32})
                        diagram_page.set_content('<!doctype html><html><head>'+standalone_font+'</head>'
                                                 '<body style="margin:16px;background:white">'
                                                 +rendered[screenshot_key]+'</body></html>')
                        diagram_page.evaluate("() => document.fonts.ready")
                        diagram_page.screenshot(path=str(ROOT/"assets"/(screenshot_key+"_preview.png")),
                                                full_page=False, timeout=15000)
                        diagram_page.close()
                first_figure = page.locator('.diagram').first
                first_figure.locator('[data-size="fit"]').click()
                fit_worked = first_figure.evaluate("e => e.classList.contains('fit')")
                first_figure.locator('[data-size="actual"]').click()
                actual_worked = first_figure.evaluate("e => !e.classList.contains('fit')")
                first_figure.locator('summary').click()
                source_worked = first_figure.locator('details').evaluate("e => e.open")
                html_checks["ui_controls"] = {"fit":fit_worked,"actual":actual_worked,"source_toggle":source_worked}
                # Verify local file browsing after build; no Mermaid runtime is loaded.
                file_context = browser.new_context(viewport={"width": 1600,"height": 1100})
                file_context.route("http://**/*", lambda route: route.abort())
                file_context.route("https://**/*", lambda route: route.abort())
                file_page = file_context.new_page()
                file_page.goto((ROOT/"index.html").as_uri(),wait_until="load")
                file_page.evaluate("() => document.fonts.ready")
                html_checks["offline_file"] = file_page.evaluate("""() => ({
                    inline_svg:document.querySelectorAll('.diagram-canvas svg').length,
                    font_loaded:document.fonts.check('16px ArchitectureCJK'),
                    runtime_script_sources:[...document.scripts].map(x=>x.src).filter(Boolean)
                })""")
                file_context.close()
            browser.close()
    except Exception as fatal:
        errors.append({"status":"failed", "stage":"browser_validation", "error":str(fatal)})
        print("BROWSER VALIDATION FAILED: "+str(fatal), flush=True)
    finally:
        server.shutdown();server.server_close()
        render_temp.cleanup()
    changed = [d["name"] for d in documents if sha(d["path"].read_bytes()) != d["sha256"]]
    ok = not errors and not console_errors and not blocked_network and not changed
    if html_checks:
        ok = ok and not html_checks["broken_internal_anchors"] and html_checks["font_loaded"]
        ok = ok and html_checks["inline_svg"] == len(jobs) and html_checks.get("offline_file", {}).get("font_loaded", False)
        ok = ok and bool(html_checks.get("ui_controls")) and all(html_checks.get("ui_controls", {}).values())
    validation = {"status":"passed" if ok else "failed", "generated_at_utc":datetime.now(timezone.utc).isoformat(),
        "duration_seconds":round(time.time()-started,3),"render_method":"local Chromium Mermaid parse+render",
        "browser_version":browser_version,"python_dependencies":{
            x:importlib.metadata.version(x) for x in ("playwright","Markdown","fonttools")},
        "pinned_dependencies":dependencies,"subset_font":font,
        "documents":[{"file":d["name"],"sha256":d["sha256"]} for d in documents],
        "diagram_count":len(jobs),"passed_count":sum(r["status"] == "passed" for r in records),"diagrams":records,
        "errors":errors,"browser_errors":console_errors,"blocked_external_requests":blocked_network,
        "documents_changed_during_render":changed,"html_checks":html_checks,
        "model_or_deployment_validation":False}
    (ROOT/"assets"/"render_validation.json").write_text(json.dumps(validation,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps({"status":validation["status"],"diagrams":len(jobs),"errors":len(errors),
                      "changed_documents":changed,"html_checks":html_checks},ensure_ascii=False,indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
