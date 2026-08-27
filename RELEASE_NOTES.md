# Traffic Monitor v2.4.0

## Features

- 新增「规则」维度：直接读取 Mihomo 每个连接命中的 Clash 规则（如 `geosite:telegram`、`geoip:CN`），把裸 IP 归到规则分组下，不再只看到 `91.108.56.108` 这类难以识别的地址。
- 「规则」维度支持细节 / 汇总两种模式，以及规则 → 主机 → 连接明细的下钻。
- 真实命中兜底 `MATCH` 规则的流量会展示为 `match`；升级前没有记录规则的老数据统一归入隐藏的 `unknown` 分组，不展示。

## Fixes

- 修复老库升级时报 `no such column: rule_group`：规则索引改为在列迁移之后创建，旧数据库可平滑升级。
- 老数据（未记录规则）不再以 `match` 出现在规则维度中，避免和真实兜底流量混淆。
