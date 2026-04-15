# Traffic Monitor

`Traffic Monitor` 是一个用于 Mihomo 的轻量流量监控镜像。

容器启动后会持续采集 Mihomo 的连接流量，并在 `8080` 端口提供 Web 页面，方便查看设备、主机、代理维度的流量数据。

## 怎么用

```bash
docker run -d \
  --name traffic-monitor \
  --restart unless-stopped \
  -p 8080:8080 \
  -e MIHOMO_URL=http://host.docker.internal:9090 \
  -e MIHOMO_SECRET=your-secret \
  -e TRAFFIC_MONITOR_DB=/data/traffic_monitor.db \
  -v "$(pwd)/data:/data" \
  zhf883680/clash-traffic-monitor:latest
```

启动后访问：

```text
http://localhost:8080/
```

## 常用环境变量

| 变量名 | 默认值 | 说明 |
| --- | --- | --- |
| `MIHOMO_URL` | `http://127.0.0.1:9090` | Mihomo Controller 地址 |
| `MIHOMO_SECRET` | 空 | Mihomo Bearer Token |
| `TRAFFIC_MONITOR_LISTEN` | `:8080` | 服务监听地址 |
| `TRAFFIC_MONITOR_DB` | `./traffic_monitor.db` | 数据库路径 |
| `TRAFFIC_MONITOR_POLL_INTERVAL_MS` | `2000` | 采集间隔，单位毫秒 |
| `TRAFFIC_MONITOR_RETENTION_DAYS` | `30` | 数据保留天数 |

## 页面预览

![Traffic Monitor 页面预览](https://raw.githubusercontent.com/zhf883680/clash-traffic-monitor/main/readmeImg/image.png)
