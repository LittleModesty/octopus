# 近期提交记录与使用说明（2026-01-18）

## 提交列表

- db3d473 `relay: add debug dumps and handle SSE responses`
  - 新增 relay debug dump（入站/出站/上游原文采样、错误捕获）。
  - 增强 SSE 处理：即便上游返回 `text/event-stream`，也能在客户端非流式时聚合为 JSON 返回。
  - 增加 Docker 相关文件，便于容器化调试。

- 819be6f `revert: responses outbound accept/stream defaults`
  - 回滚 `/responses` 的 `Accept` 与 `stream` 默认策略（不再自动强制匹配）。
  - 影响：部分网关可能在 `stream` 未显式指定时默认走 SSE。

- 46f5f16 `fix: improve responses bridge compatibility`
  - `/responses` 的 `input` 统一用数组格式（避免网关只接受 list 而 400）。
  - `stream_options` 仅透传（不再默认注入），减少不兼容网关的 400。
  - Anthropic → OpenAI 桥接时丢弃 `temperature`（避免 xychatai 拒绝该参数）。

## 使用方式与注意事项

### 1) Debug dump（db3d473）
启用后会在 `data/debug-dumps/`（容器内为 `/app/data/debug-dumps`）生成 JSON。

常用环境变量：
- `OCTOPUS_RELAY_DUMP_ENABLED=true` 开启
- `OCTOPUS_RELAY_DUMP_MODE=all|error`（默认 `error`）
- `OCTOPUS_RELAY_DUMP_DIR=/path/to/dir` 指定目录
- `OCTOPUS_RELAY_DUMP_MAX_BODY_BYTES=65536` 限制采样大小
- `OCTOPUS_RELAY_DUMP_MAX_STREAM_BYTES=65536` 限制 SSE 采样大小
- `OCTOPUS_RELAY_DUMP_REDACT=true` 开启敏感字段脱敏

Docker 示例（覆盖文件）：
```
services:
  octopus:
    environment:
      - OCTOPUS_RELAY_DUMP_ENABLED=true
```

### 2) `/responses` 流式 usage（46f5f16 之后）
`stream_options` 不再默认注入，只有客户端显式传入时才会透传。
如果你确实需要流式 usage，请由客户端发送：
```
{
  "stream": true,
  "stream_options": { "include_usage": true }
}
```
注意：部分上游网关（如 RightCode）可能拒绝 `stream_options`，此时不要传。

### 3) Anthropic → OpenAI 桥接的 `temperature` 行为（46f5f16）
为兼容 xychatai，桥接路径会丢弃 `temperature`：
- 影响：调用方显式设置的 temperature 不再生效（上游走默认值）。
- 建议：需要温度控制时改用支持该参数的上游或使用 OpenAI/RightCode。

### 4) `/responses` 入参格式（46f5f16）
`input` 始终使用数组格式，不再发送字符串形式；客户端侧无感知。

