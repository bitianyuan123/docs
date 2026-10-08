# 离线阅读、文档校验与重新渲染

克隆整个文档仓库后，直接用浏览器打开 [index.html](../index.html) 即可阅读。已提交的 HTML 包含全部 SVG，字体也在仓库内；不需要 Python、Mermaid、推荐系统源码或数据库。也可以单独打开 `rendered/*.svg`，每张图均内嵌中文字体。源码引用指向随仓库保存的只读 HTML 快照，并保留行号锚点。

## 1. 只检查文档与附件

在文档仓库根目录执行，要求 Python 3.11 或更新版本，不需要额外 Python 包：

```bash
python3 assets/validate_documents.py
```

默认仅检查仓库内的相对链接、源码行锚点、代码围栏、JSON、附件校验值和教学样例关系，报告写入 `assets/validation.json`。报告会明确注明：**没有重新读取原始 Tenrec 数据，也没有检查本机源工程**。历史证据文件保留原来源信息，这不要求克隆者拥有原作者的目录。

若本机另有完整源工作区，可显式追加检查：

```bash
python3 assets/validate_documents.py --source-workspace /path/to/source-workspace
```

这个目录应包含 `Tenrec/ctr_data_1M.csv`，以及 `pairec4tigerllm`、`pairec4tigerllm_8506`、`pairec_sh`、`OneTrans_HSE_project` 四个 Git 检出目录。此模式重新聚合样例前 20 万行，核对已有数据证据，并记录各源工程提交与工作区状态。`--report -` 可只输出报告而不写文件。

## 2. 修改图文后重新生成阅读版

Markdown 是编辑源；`diagrams/*.mmd`、`rendered/*.svg` 与 `index.html` 均由 [渲染脚本](render_documents.py) 生成。以下命令在仓库根目录执行，推荐使用独立虚拟环境：

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r assets/requirements-render.txt
python3 -m playwright install chromium
python3 assets/render_documents.py --screenshot
python3 assets/validate_documents.py
```

Windows 可改用 `.venv\Scripts\Activate.ps1` 激活。Linux 若缺少 Chromium 系统动态库，可按 Playwright 的安装提示运行 `python3 -m playwright install-deps chromium`；该步骤可能需要系统包安装权限。已有浏览器运行依赖的机器不需要再次安装。

脚本默认使用当前 Python 环境和 Playwright 安装的 Chromium。渲染只启动临时本机 HTTP 服务，禁止浏览器向外部地址取资源，不向外部服务发送文档或图片。临时字体配置和缓存由系统临时目录分配，结束时清理，不依赖固定路径。

## 3. 可选的已有环境配置

通常无需设置以下变量；已有工具目录或浏览器时才使用：

| 环境变量 | 含义 |
|---|---|
| `ARCHITECTURE_RENDER_PYTHONPATH` | 额外 Python 包目录；多个目录使用操作系统路径分隔符 |
| `ARCHITECTURE_CHROMIUM_EXECUTABLE` | 指定 Chromium 可执行文件；应与当前 Playwright 兼容 |
| `ARCHITECTURE_BROWSER_LIBRARY_PATH` | Linux 浏览器所需的额外动态库搜索路径，仅传给浏览器子进程 |

固定版本的 Mermaid **11.4.1**、Noto Sans CJK SC 原始字体与许可证保存在 `assets/vendor`。来源和 SHA256 见 [依赖清单](vendor/dependencies.json)；脚本先验证文件，再生成当前文档所需的 WOFF2 字体子集。 [browser_dependencies.json](browser_dependencies.json) 仅记录原构建机器使用过的系统包，不是其他机器的安装要求。

## 4. 验证结果的范围

[渲染记录](render_validation.json) 包括文档与图源码校验值、逐图渲染尺寸、字体加载、浏览器错误、页面内链、离线打开和控件检查。带 `--screenshot` 时还会更新阅读页、系统概览和主时序图预览。

这些检查验证文档可以阅读、引用可以定位、样例自洽，不证明推荐模型已训练、服务已部署或系统已完成在线联调。新增源码证据快照后还需更新其 [校验清单](source_snapshots/manifest.json)。
