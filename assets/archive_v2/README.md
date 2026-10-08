# 历史附件：本轮已替代的接口设计

本目录保留上一版场景配置、请求响应模板、构造脚本和Python类型，只用于追溯。**不作为当前系统的接口合同，也不应直接运行其中脚本生成本轮附件。**

旧版假定PaiRec直读Redis、OneTrans接收完整15维特征、按请求管理KV并使用Release。最新要求改为独立特征服务，而OneTrans保留当前`/ingest`和`/rank`本地装配路径。当前文件入口为[附件说明](../README.md)。

其中旧物品统计仍可作为真实数据证据；旧15维数组不是当前OneTrans TSV的列顺序。源文件SHA保存在父目录source_manifest.json。本目录contracts.py为上一版文档类型定义，不是原工程实现。
