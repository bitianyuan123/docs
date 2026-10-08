# 源码与前期设计证据快照

此目录仅保存正文已经引用的文件，方便单独 clone 文档仓库后核对依据。每个 HTML 保留原始行号，可直接定位 `#L234`，并以纯文本方式显示源内容。它们不是服务源码的完整分发，不能用于构建服务。

清单中的 `source_sha256` 是原文件摘要，`sha256` 是当前 HTML 快照摘要；原工程文件未修改。少量连接口令已遮盖并在对应快照中标明。历史文档内容按当时版本保存，不代表本版设计。

| 文件 | 来源提交 |
|---|---|
| [prior_design/01_recommendation_detailed_design.md](prior_design/01_recommendation_detailed_design.md.html) | `前期资料` |
| [prior_design/02_recommendation_development_guide.md](prior_design/02_recommendation_development_guide.md.html) | `前期资料` |
| [prior_design/03_recommendation_4plus1_views.md](prior_design/03_recommendation_4plus1_views.md.html) | `前期资料` |
| [prior_design/recommendation_design_assets/contract_decisions.md](prior_design/recommendation_design_assets/contract_decisions.md.html) | `前期资料` |
| [prior_design/recommendation_design_assets/source_pairec_upstream_v2.6.2/route.go](prior_design/recommendation_design_assets/source_pairec_upstream_v2.6.2/route.go.html) | `前期资料` |
| [prior_design/recommendation_design_assets/tenrec_audit/official_sources/issue_11_comments.json](prior_design/recommendation_design_assets/tenrec_audit/official_sources/issue_11_comments.json.html) | `前期资料` |
| [OneTrans_HSE_project/cpp/artifacts/weights/manifest.json](OneTrans_HSE_project/cpp/artifacts/weights/manifest.json.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/src/engine/frontend.cpp](OneTrans_HSE_project/cpp/src/engine/frontend.cpp.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/src/kv/datasystem_store.cpp](OneTrans_HSE_project/cpp/src/kv/datasystem_store.cpp.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/src/kv/serialize.h](OneTrans_HSE_project/cpp/src/kv/serialize.h.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/src/kv/store.cpp](OneTrans_HSE_project/cpp/src/kv/store.cpp.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/src/serving/flow.cpp](OneTrans_HSE_project/cpp/src/serving/flow.cpp.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/src/serving/json_io.cpp](OneTrans_HSE_project/cpp/src/serving/json_io.cpp.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/src/serving/pipeline.cpp](OneTrans_HSE_project/cpp/src/serving/pipeline.cpp.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/src/serving/ps_client.cpp](OneTrans_HSE_project/cpp/src/serving/ps_client.cpp.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/src/serving/rank_assembler.cpp](OneTrans_HSE_project/cpp/src/serving/rank_assembler.cpp.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/cpp/tools/server_main.cpp](OneTrans_HSE_project/cpp/tools/server_main.cpp.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/deploy/ps/embedding_server.cc](OneTrans_HSE_project/deploy/ps/embedding_server.cc.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/deploy/ps/embedding_service.proto](OneTrans_HSE_project/deploy/ps/embedding_service.proto.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/docs/单机多节点部署.md](OneTrans_HSE_project/docs/%E5%8D%95%E6%9C%BA%E5%A4%9A%E8%8A%82%E7%82%B9%E9%83%A8%E7%BD%B2.md.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/onetrans/tools/build_domain_features.awk](OneTrans_HSE_project/onetrans/tools/build_domain_features.awk.html) | `16aecd6f7bd1` |
| [OneTrans_HSE_project/onetrans/tools/tenrec_adapter.py](OneTrans_HSE_project/onetrans/tools/tenrec_adapter.py.html) | `16aecd6f7bd1` |
| [pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp](pairec4tigerllm/cpp/brpc_gateway/brpc_inference_server.cpp.html) | `cadc1855404b` |
| [pairec4tigerllm/docs/F19_NATIVE_DATASYSTEM_ATTRIBUTION.md](pairec4tigerllm/docs/F19_NATIVE_DATASYSTEM_ATTRIBUTION.md.html) | `cadc1855404b` |
| [pairec4tigerllm/docs/PERSISTENT_NATIVE_KEY_LIFECYCLE.md](pairec4tigerllm/docs/PERSISTENT_NATIVE_KEY_LIFECYCLE.md.html) | `cadc1855404b` |
| [pairec4tigerllm/inference/dssm_recall_server.py](pairec4tigerllm/inference/dssm_recall_server.py.html) | `cadc1855404b` |
| [pairec4tigerllm/proto/recommend.proto](pairec4tigerllm/proto/recommend.proto.html) | `cadc1855404b` |
| [pairec4tigerllm/scripts/load_item_embeddings_to_milvus.py](pairec4tigerllm/scripts/load_item_embeddings_to_milvus.py.html) | `cadc1855404b` |
| [pairec4tigerllm/training/dssm/export_embeddings.py](pairec4tigerllm/training/dssm/export_embeddings.py.html) | `cadc1855404b` |
| [pairec4tigerllm_8506/services/brpcwire/client.go](pairec4tigerllm_8506/services/brpcwire/client.go.html) | `530f77327f0d` |
| [pairec4tigerllm_8506/services/feature/provider.go](pairec4tigerllm_8506/services/feature/provider.go.html) | `530f77327f0d` |
| [pairec4tigerllm_8506/services/recall/onetrans_s_stage.go](pairec4tigerllm_8506/services/recall/onetrans_s_stage.go.html) | `530f77327f0d` |
| [pairec4tigerllm_8506/services/sort/onetrans_rank_sort.go](pairec4tigerllm_8506/services/sort/onetrans_rank_sort.go.html) | `530f77327f0d` |
| [pairec_sh/pairec-demo/src/cpp/brpcClients/opensearch_client.cpp](pairec_sh/pairec-demo/src/cpp/brpcClients/opensearch_client.cpp.html) | `f7c4c49cbea7` |
| [pairec_sh/pairec-demo/src/cpp/brpcClients/redis_client.cpp](pairec_sh/pairec-demo/src/cpp/brpcClients/redis_client.cpp.html) | `f7c4c49cbea7` |
| [pairec_sh/pairec-demo/src/dao/feature_brpc_redis_dao.go](pairec_sh/pairec-demo/src/dao/feature_brpc_redis_dao.go.html) | `f7c4c49cbea7` |
| [pairec_sh/pairec-demo/src/recall/brpc_sparse_recall.go](pairec_sh/pairec-demo/src/recall/brpc_sparse_recall.go.html) | `f7c4c49cbea7` |
| [pairec4tigerllm/k8s/deployment-datasystem-pool-hostnetwork.yaml](pairec4tigerllm/k8s/deployment-datasystem-pool-hostnetwork.yaml.html) | `cadc1855404b` |
| [pairec4tigerllm/k8s/deployment-milvus-standalone.yaml](pairec4tigerllm/k8s/deployment-milvus-standalone.yaml.html) | `cadc1855404b` |
| [pairec4tigerllm/k8s/deployment-inference-brpc-trtllm.yaml](pairec4tigerllm/k8s/deployment-inference-brpc-trtllm.yaml.html) | `cadc1855404b` |

完整来源与校验信息见 [manifest.json](manifest.json)。
