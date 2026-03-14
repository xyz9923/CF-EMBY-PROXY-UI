# `测试/worker.js` 设置绑定词典

## 目的

这个词典只描述设置面板里的“字段绑定层”，对应 `worker.js` 中的以下结构：

- `CONFIG_FORM_BINDINGS`
- `CONFIG_SECTION_FIELDS`
- `getConfigFormBindings()`
- `applyConfigSectionToForm()`
- `collectConfigSectionFromForm()`

它的职责不是解释全部业务，而是回答三个问题：

1. 某个配置字段属于哪个设置分区。
2. 这个字段绑定到哪个 DOM 节点。
3. 这个字段在“加载表单”和“保存表单”时分别采用什么回退规则。

## 分区字典

| 分区 | 说明 |
|-----|------|
| `ui` | 控制台刷新策略 |
| `proxy` | 协议、预热、重试、源站代理策略 |
| `security` | 地区/IP/CORS/图片缓存 |
| `logs` | 日志刷盘、定时任务租约、Telegram 告警 |
| `account` | JWT、Cloudflare 账号参数 |

## 字段绑定规则

### `ui`

| 字段 | DOM ID | 保存模式 | 加载模式 | 默认值 |
|-----|------|------|------|------|
| `dashboardAutoRefreshEnabled` | `cfg-dashboard-auto-refresh` | `checkbox` | `checkbox` | `false` |
| `dashboardAutoRefreshSeconds` | `cfg-dashboard-auto-refresh-seconds` | `int-finite` | `int-finite` | `30` |

### `proxy`

| 字段 | DOM ID | 保存模式 | 加载模式 | 默认值 |
|-----|------|------|------|------|
| `enableH2` | `cfg-enable-h2` | `checkbox` | `checkbox` | `false` |
| `enableH3` | `cfg-enable-h3` | `checkbox` | `checkbox` | `false` |
| `peakDowngrade` | `cfg-peak-downgrade` | `checkbox` | `checkbox(defaultTrue)` | `true` |
| `protocolFallback` | `cfg-protocol-fallback` | `checkbox` | `checkbox(defaultTrue)` | `true` |
| `enablePrewarm` | `cfg-enable-prewarm` | `checkbox` | `checkbox(defaultTrue)` | `true` |
| `prewarmCacheTtl` | `cfg-prewarm-ttl` | `int-or-default` | `number-finite` | `180` |
| `prewarmPrefetchBytes` | `cfg-prewarm-prefetch-bytes` | `int-finite` | `int-finite` | `4194304` |
| `sourceSameOriginProxy` | `cfg-source-same-origin-proxy` | `checkbox` | `checkbox(defaultTrue)` | `true` |
| `forceExternalProxy` | `cfg-force-external-proxy` | `checkbox` | `checkbox(defaultTrue)` | `true` |
| `wangpandirect` | `cfg-wangpandirect` | `trim` | `or-default` | 内置默认关键词串 |
| `pingTimeout` | `cfg-ping-timeout` | `int-or-default` | `number-finite` | `5000` |
| `upstreamTimeoutMs` | `cfg-upstream-timeout-ms` | `int-finite` | `int-finite` | `0` |
| `upstreamRetryAttempts` | `cfg-upstream-retry-attempts` | `int-finite` | `int-finite` | `0` |
| `sourceDirectNodes` | 无独立单个 DOM 输入 | 特殊处理：由 `settingsSourceDirectNodes` 汇总 | 特殊处理 | 空数组 |

### `security`

| 字段 | DOM ID | 保存模式 | 加载模式 | 默认值 |
|-----|------|------|------|------|
| `geoAllowlist` | `cfg-geo-allow` | `text` | `text` | 空串 |
| `geoBlocklist` | `cfg-geo-block` | `text` | `text` | 空串 |
| `ipBlacklist` | `cfg-ip-black` | `text` | `text` | 空串 |
| `rateLimitRpm` | `cfg-rate-limit` | `int-or-default` | `or-default` | `0`，加载空值展示为空串 |
| `cacheTtlImages` | `cfg-cache-ttl` | `int-or-default` | `int-or-default` | `30` |
| `corsOrigins` | `cfg-cors` | `text` | `text` | 空串 |

### `logs`

| 字段 | DOM ID | 保存模式 | 加载模式 | 默认值 |
|-----|------|------|------|------|
| `logRetentionDays` | `cfg-log-days` | `int-finite` | `int-finite` | `7` |
| `logWriteDelayMinutes` | `cfg-log-delay` | `float-finite` | `float-finite` | `20` |
| `logFlushCountThreshold` | `cfg-log-flush-count` | `int-finite` | `int-finite` | `50` |
| `logBatchChunkSize` | `cfg-log-batch-size` | `int-finite` | `int-finite` | `50` |
| `logBatchRetryCount` | `cfg-log-retry-count` | `int-finite` | `int-finite` | `2` |
| `logBatchRetryBackoffMs` | `cfg-log-retry-backoff` | `int-finite` | `int-finite` | `75` |
| `scheduledLeaseMs` | `cfg-scheduled-lease-ms` | `int-finite` | `int-finite` | `300000` |
| `tgBotToken` | `cfg-tg-token` | `trim` | `trim` | 空串 |
| `tgChatId` | `cfg-tg-chatid` | `trim` | `trim` | 空串 |
| `tgAlertDroppedBatchThreshold` | `cfg-tg-alert-drop-threshold` | `int-finite` | `int-finite` | `0` |
| `tgAlertFlushRetryThreshold` | `cfg-tg-alert-retry-threshold` | `int-finite` | `int-finite` | `0` |
| `tgAlertOnScheduledFailure` | `cfg-tg-alert-scheduled-failure` | `checkbox` | `checkbox` | `false` |
| `tgAlertCooldownMinutes` | `cfg-tg-alert-cooldown-minutes` | `int-finite` | `int-finite` | `30` |

### `account`

| 字段 | DOM ID | 保存模式 | 加载模式 | 默认值 |
|-----|------|------|------|------|
| `jwtExpiryDays` | `cfg-jwt-days` | `int-or-default` | `int-or-default` | `30` |
| `cfAccountId` | `cfg-cf-account` | `trim` | `trim` | 空串 |
| `cfZoneId` | `cfg-cf-zone` | `trim` | `trim` | 空串 |
| `cfApiToken` | `cfg-cf-token` | `trim` | `trim` | 空串 |

## 边界说明

- `sourceDirectNodes` 不走通用单输入绑定，它依赖节点列表、搜索框和多选状态汇总。
- `loadMode` 与 `saveMode` 不完全对称是有意设计，不是遗漏。
- 典型例子：
  - `wangpandirect` 加载时允许回退到内置默认关键词，保存时只保留管理员当前输入。
  - `rateLimitRpm` 加载时允许展示空串，保存时空输入会落成 `0`。
  - `prewarmCacheTtl` 与 `pingTimeout` 加载时保留合法 `0/数值` 展示，保存时仍沿用历史 `parseInt(...) || 默认值` 语义。

## 维护约定

- 新增设置项时，先补这个词典，再补 `CONFIG_FORM_BINDINGS`。
- 如果某个字段的加载/保存行为不对称，必须在这里显式记录原因。
- 不要把文案、布局、风险提示也塞进这份词典；这份文件只维护“字段绑定关系”。
