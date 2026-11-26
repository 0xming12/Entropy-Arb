# EC2 NVDA 采集源码

本目录保存从已部署采集进程的源码目录取回的 15 个 Python 文件。用下面的命令可以逐文件核对 SHA-256。

```sh
cd deployment/collector_runtime
sha256sum -c SHA256SUMS
```

运行方式为 `--record-only --symbol NVDA --hedge lighter --env-file /dev/null --no-dashboard`。示例 drop-in 启用 100 ms 五档深度采样、Entropy 公开成交流水和 BBO 变化采集。按本地安装位置提供 Python 解释器及采集配置。交易引擎、策略开发和正常构建使用仓库顶层源码。
