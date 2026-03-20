// EMBY-PROXY-UI V18.5 (SaaS UI Optimized - Ultimate Fix + Emby Auth Patch)

/**
 * @typedef {{
 *   get(key: string, options?: { type?: string }): Promise<any>,
 *   put(key: string, value: string): Promise<void>,
 *   delete(key: string): Promise<void>,
 *   list(options?: { prefix?: string, cursor?: string }): Promise<{ keys: Array<{ name: string }>, cursor?: string, list_complete?: boolean }>
 * }} KVNamespaceLike
 *
 * @typedef {{ waitUntil(promise: Promise<any>): void }} ExecutionContextLike
 *
 * @typedef {{
 *   success?: boolean,
 *   ok?: boolean,
 *   description?: string,
 *   errors?: Array<{ message?: string }>,
 *   result?: any,
 *   result_info?: { total_pages?: number, totalPages?: number },
 *   data?: {
 *     viewer?: {
 *       zones?: any[],
 *       accounts?: any[]
 *     }
 *   }
 * }} JsonApiEnvelope
 *
 * @typedef {{
 *   reason?: string,
 *   section?: string,
 *   actor?: string,
 *   source?: string,
 *   note?: string
 * }} ConfigSnapshotMeta
 *
 * @typedef {{
 *   id?: string,
 *   name?: string,
 *   type?: string,
 *   content?: string,
 *   savedAt?: string,
 *   updatedAt?: string,
 *   createdAt?: string,
 *   actor?: string,
 *   source?: string,
 *   requestHost?: string
 * }} DnsRecordHistoryEntryLike
 *
 * @typedef {{
 *   kv?: KVNamespaceLike | null,
 *   ctx?: ExecutionContextLike | null,
 *   invalidateList?: boolean
 * }} PersistNodesIndexOptions
 *
 * @typedef {{
 *   env?: any,
 *   kv?: KVNamespaceLike | null,
 *   ctx?: ExecutionContextLike | null,
 *   snapshotMeta?: ConfigSnapshotMeta
 * }} PersistRuntimeConfigOptions
 *
 * @typedef {RequestInit & { cf?: { cacheEverything: boolean, cacheTtl: number } }} WorkerRequestInit
 * @typedef {Response & { webSocket?: unknown }} UpgradeableResponse
 * @typedef {Error & { code?: string, status?: number }} AppError
 */

// ============================================================================
// 0. 全局配置与状态 (GLOBAL CONFIG & STATE)
// ============================================================================
const Config = {
  Defaults: {
    JwtExpiry: 60 * 60 * 24 * 30,  
    LoginLockDuration: 900,         
    MaxLoginAttempts: 5,            
    CacheTTL: 60000,                
    CryptoKeyCacheTTL: 86400,       
    CryptoKeyCacheMax: 100,         
    NodeCacheMax: 5000,             
    NodesReadConcurrency: 12,       
    LogRetentionDays: 7,
    LogRetentionDaysMax: 365,
    LogFlushDelayMinutes: 20,
    LogFlushCountThreshold: 50,
    LogBatchChunkSize: 50,
    LogBatchRetryCount: 2,
    LogBatchRetryBackoffMs: 75,
    ScheduledLeaseMinMs: 30 * 1000,
    ScheduledLeaseMs: 5 * 60 * 1000,
    UiRadiusPx: 24,
    CacheTtlImagesDays: 30,
    PingTimeoutMs: 5000,
    PingCacheMinutes: 10,
    NodePanelPingAutoSort: false,
    TgAlertDroppedBatchThreshold: 0,
    TgAlertFlushRetryThreshold: 0,
    TgAlertCooldownMinutes: 30,
    TgAlertOnScheduledFailure: false,
    UpstreamTimeoutMs: 0,
    UpstreamRetryAttempts: 0,
    BufferedRetryBodyMaxBytes: 2 * 1024 * 1024,
    LogQueryDefaultDays: 1,
    LogKeywordMaxWindowDays: 3,
    LogVacuumMinIntervalMs: 7 * 24 * 60 * 60 * 1000,
    KvTidyIntervalMs: 60 * 60 * 1000,
    PrewarmCacheTtl: 180,
    MetadataPrewarmTimeoutMs: 3000,
    PrewarmPrefetchBytes: 4 * 1024 * 1024,
    ConfigSnapshotLimit: 5,
    DnsHistoryLimit: 10,
    CleanupBudgetMs: 1,             
    CleanupChunkSize: 64,           
    AssetHash: "v18.5",           
    Version: "18.5"                 
  }
};

const GLOBALS = {
  NodeCache: new Map(),
  ConfigCache: null,
  CryptoKeyCache: new Map(),
  NodesListCache: null,
  CleanupState: {
    phase: 0,
    iterators: {
      node: null,
      crypto: null,
      rate: null,
      log: null
    }
  },
  NodesIndexCache: null,
  LogQueue: [],
  LogDedupe: new Map(),
  RateLimitCache: new Map(),
  LogFlushPending: false,
  LogLastFlushAt: 0,
  OpsStatusWriteChain: Promise.resolve(),
  OpsStatusDbReady: new WeakMap(),
  InitCheckWarnedFingerprints: new Set(),
  Regex: {
    ImageExt: /\.(?:jpg|jpeg|gif|png|svg|ico|webp)$/i,
    StaticExt: /\.(?:js|css|woff2?|ttf|otf|map|webmanifest)$/i,
    SubtitleExt: /\.(?:srt|ass|vtt|sub)$/i,
    EmbyImages: /(?:\/Images\/|\/Icons\/|\/Branding\/|\/emby\/covers\/)/i,
    ManifestExt: /\.(?:m3u8|mpd)$/i,
    SegmentExt: /\.(?:ts|m4s)$/i,
    Streaming: /\.(?:mp4|m4v|m4a|ogv|webm|mkv|mov|avi|wmv|flv)$/i
  },
  SecurityHeaders: {
    "Referrer-Policy": "origin-when-cross-origin",
    "Strict-Transport-Security": "max-age=15552000; preload",
    "X-Frame-Options": "SAMEORIGIN",
    "X-Content-Type-Options": "nosniff",
    "X-XSS-Protection": "1; mode=block"
  },
  DropRequestHeaders: new Set([
    "host", "x-real-ip", "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "forwarded",
    "connection", "upgrade", "transfer-encoding", "te", "keep-alive",
    "proxy-authorization", "proxy-authenticate", "trailer", "expect"
  ]),
  DropResponseHeaders: new Set([
    "access-control-allow-origin", "access-control-allow-methods", "access-control-allow-headers", "access-control-allow-credentials",
    "x-frame-options", "strict-transport-security", "x-content-type-options", "x-xss-protection", "referrer-policy",
    "x-powered-by", "server" 
  ])
};

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, HEAD",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Emby-Authorization, X-Emby-Token, X-Emby-Client, X-Emby-Device-Id, X-Emby-Device-Name, X-Emby-Client-Version"
};

function mergeVaryHeader(headers, value) {
  const current = headers.get("Vary");
  if (!current) {
    headers.set("Vary", value);
    return;
  }
  const parts = current.split(",").map(v => v.trim()).filter(Boolean);
  if (!parts.includes(value)) parts.push(value);
  headers.set("Vary", parts.join(", "));
}

function applySecurityHeaders(headers) {
  Object.entries(GLOBALS.SecurityHeaders).forEach(([k, v]) => headers.set(k, v));
  return headers;
}

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return '0 B';
  const k = 1024, sizes = ['B', 'KB', 'MB', 'GB', 'TB'], i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function toGraphQLString(value) {
  return JSON.stringify(String(value ?? ""));
}

function toGraphQLStringArray(values) {
  return JSON.stringify((Array.isArray(values) ? values : []).map(value => String(value ?? "")));
}

function getCorsHeadersForResponse(env, request, originOverride = null) {
  const reqOrigin = request.headers.get("Origin");
  const reqHeaders = request.headers.get("Access-Control-Request-Headers") || corsHeaders["Access-Control-Allow-Headers"];
  const allowOrigin = originOverride || reqOrigin || corsHeaders["Access-Control-Allow-Origin"];
  return {
    "Access-Control-Allow-Origin": allowOrigin,
    "Access-Control-Allow-Methods": corsHeaders["Access-Control-Allow-Methods"],
    "Access-Control-Allow-Headers": reqHeaders,
    "Access-Control-Expose-Headers": "Content-Length, Content-Range, X-Emby-Auth-Token",
    "Access-Control-Max-Age": "86400"
  };
}

function safeDecodeSegment(segment = "") {
  if (!segment) return "";
  try { return decodeURIComponent(segment); } catch { return segment; }
}

function sanitizeProxyPath(path) {
  let raw = typeof path === "string" ? path : "/";
  if (!raw) return "/";
  if (!raw.startsWith("/")) raw = "/" + raw;
  raw = raw.replace(/^\/+/, "/");
  return raw;
}

function normalizeAdminPath(value) {
  const fallback = "/admin";
  const raw = String(value || "").trim();
  if (!raw) return fallback;
  let normalized = sanitizeProxyPath(raw);
  normalized = normalized.replace(/\/{2,}/g, "/");
  if (normalized.length > 1) normalized = normalized.replace(/\/+$/, "");
  if (!normalized || normalized === "/" || normalized.toLowerCase().startsWith("/api")) return fallback;
  return normalized;
}

function pathnameMatchesPrefix(pathname, prefix) {
  const safePath = sanitizeProxyPath(pathname || "/");
  const safePrefix = sanitizeProxyPath(prefix || "/");
  return safePath === safePrefix || safePath.startsWith(safePrefix + "/");
}

function getAdminPath(env) {
  return normalizeAdminPath(env?.ADMIN_PATH);
}

function getAdminLoginPath(env) {
  const adminPath = getAdminPath(env);
  return adminPath === "/" ? "/login" : `${adminPath}/login`;
}

function getAdminCookiePath(env) {
  const adminPath = getAdminPath(env);
  return adminPath === "/" ? "/" : adminPath;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function serializeInlineJson(value) {
  return JSON.stringify(value)
    .replace(/</g, "\\u003c")
    .replace(/>/g, "\\u003e")
    .replace(/&/g, "\\u0026")
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029");
}

function buildInitHealth(env) {
  const missing = [];
  if (!env?.JWT_SECRET) missing.push("JWT_SECRET");
  if (!env?.ADMIN_PASS) missing.push("ADMIN_PASS");
  const adminPath = getAdminPath(env);
  const loginPath = getAdminLoginPath(env);
  return {
    ok: missing.length === 0,
    missing,
    adminPath,
    loginPath,
    message: missing.length
      ? `系统未初始化：缺少 ${missing.join("、")}。`
      : "系统初始化检查通过。"
  };
}

function warnInitHealthOnce(env) {
  const health = buildInitHealth(env);
  if (health.ok) return health;
  const fingerprint = health.missing.join("|") || "unknown";
  if (!GLOBALS.InitCheckWarnedFingerprints.has(fingerprint)) {
    GLOBALS.InitCheckWarnedFingerprints.add(fingerprint);
    console.warn(`[Init Check] ${health.message} 管理入口: ${health.adminPath}`);
  }
  return health;
}

function buildInitHealthBannerHtml(initHealth) {
  if (!initHealth || initHealth.ok) return "";
  const missingText = Array.isArray(initHealth.missing) && initHealth.missing.length
    ? initHealth.missing.map(item => `<code class="rounded bg-amber-100/80 px-1.5 py-0.5 text-[11px] font-semibold text-amber-700">${escapeHtml(item)}</code>`).join(" ")
    : `<code class="rounded bg-amber-100/80 px-1.5 py-0.5 text-[11px] font-semibold text-amber-700">UNKNOWN</code>`;
  return `<div id="init-health-banner" class="mx-4 mt-4 rounded-2xl border border-amber-200 bg-amber-50/95 px-4 py-3 text-sm text-amber-900 shadow-sm">
    <div class="flex flex-col gap-1 md:flex-row md:items-center md:justify-between">
      <div class="font-semibold">系统未初始化</div>
      <div class="text-xs text-amber-700">管理入口：${escapeHtml(initHealth.adminPath || "/admin")}</div>
    </div>
    <p class="mt-2 leading-6">检测到关键环境变量缺失：${missingText}</p>
    <p class="mt-1 text-xs leading-5 text-amber-700">请先在 Cloudflare Worker 环境变量中补齐后再使用管理台登录与敏感操作。</p>
  </div>`;
}

function escapeSqlLike(value) {
  return String(value || "").replace(/[\\%_]/g, "\\$&");
}

function isLikelyIpAddress(value) {
  const text = String(value || "").trim();
  if (!text) return false;
  if (/^(?:\d{1,3}\.){3}\d{1,3}$/.test(text)) return true;
  return /^[0-9a-f:]+$/i.test(text) && text.includes(":");
}

function normalizeTargetBasePath(pathname = "/") {
  const safePath = sanitizeProxyPath(pathname || "/");
  if (safePath === "/") return "";
  return safePath.replace(/\/+$/, "");
}

// 保留节点 target 自带的子路径，避免 /node/foo 被错误拼到源站根目录。
function buildUpstreamProxyUrl(targetBase, proxyPath = "/") {
  const baseUrl = targetBase instanceof URL ? new URL(targetBase.toString()) : new URL(String(targetBase || ""));
  const basePath = normalizeTargetBasePath(baseUrl.pathname);
  const safeProxyPath = sanitizeProxyPath(proxyPath);
  const resolvedPath = safeProxyPath === "/"
    ? (basePath ? `${basePath}/` : "/")
    : `${basePath}${safeProxyPath}`;
  baseUrl.pathname = resolvedPath || "/";
  baseUrl.search = "";
  baseUrl.hash = "";
  return baseUrl;
}

function translateUpstreamUrlToProxyLocation(upstreamUrl, activeTargetBase, name, key) {
  try {
    const resolvedUrl = upstreamUrl instanceof URL ? upstreamUrl : new URL(String(upstreamUrl || ""));
    const targetBase = activeTargetBase instanceof URL ? activeTargetBase : new URL(String(activeTargetBase || ""));
    if (resolvedUrl.origin !== targetBase.origin) return resolvedUrl.toString();
    const basePath = normalizeTargetBasePath(targetBase.pathname);
    let proxyPath = resolvedUrl.pathname || "/";
    if (basePath) {
      if (proxyPath === basePath || proxyPath === `${basePath}/`) proxyPath = "/";
      else if (proxyPath.startsWith(`${basePath}/`)) proxyPath = proxyPath.slice(basePath.length);
      else return resolvedUrl.toString();
    }
    proxyPath = sanitizeProxyPath(proxyPath);
    return `${buildProxyPrefix(name, key)}${proxyPath === "/" ? "/" : proxyPath}${resolvedUrl.search}${resolvedUrl.hash}`;
  } catch {
    return null;
  }
}

function sanitizeSyntheticRedirectHeaders(headers) {
  [
    "Age",
    "Accept-Ranges",
    "Content-Disposition",
    "Content-Encoding",
    "Content-Language",
    "Content-Length",
    "Content-Location",
    "Content-Range",
    "Content-Type",
    "ETag",
    "Expires",
    "Last-Modified",
    "Set-Cookie",
    "Transfer-Encoding"
  ].forEach(header => headers.delete(header));
}

function buildProxyPrefix(name, key) {
  const encodedName = encodeURIComponent(String(name || ""));
  if (!key) return "/" + encodedName;
  return "/" + encodedName + "/" + encodeURIComponent(String(key));
}

function normalizePrewarmDepth(value) {
  const normalized = String(value || "").trim().toLowerCase().replace(/[\s-]+/g, "_");
  return normalized === "poster" ? "poster" : "poster_manifest";
}

function normalizeRegionCodeCsv(value = "") {
  return [...new Set(
    String(value || "")
      .split(",")
      .map(item => item.trim().toUpperCase())
      .filter(Boolean)
  )].join(",");
}

function parseContentLengthHeader(value) {
  const raw = String(value || "").trim();
  if (!/^\d+$/.test(raw)) return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function getDefaultCacheHandle() {
  try {
    const cachesHandle = /** @type {{ default?: { match: (...args: any[]) => Promise<any>, put: (...args: any[]) => Promise<any> } | null } | undefined} */ (globalThis.caches);
    return cachesHandle?.default || null;
  } catch {
    return null;
  }
}

const WORKER_CACHE_DROP_QUERY_PARAMS = new Set([
  "apikey",
  "accesstoken",
  "token",
  "authorization",
  "xembytoken",
  "xembyauthorization",
  "deviceid",
  "xembydeviceid",
  "xembydevicename",
  "xembyclient",
  "xembyclientversion",
  "client",
  "clientid",
  "devicename",
  "userid",
  "playsessionid",
  "sessionid"
]);
const WORKER_METADATA_MANIFEST_ALLOWED_PATHS = [
  /^\/Videos\/[^/]+\/(?:main|master|stream)\.m3u8$/i,
  /^\/Videos\/[^/]+\/(?:manifest|main|master|stream)\.mpd$/i,
  /^\/Audio\/[^/]+\/(?:main|master|stream)\.m3u8$/i
];
const WORKER_METADATA_MANIFEST_ALLOWED_PARAMS = new Set([
  "mediasourceid",
  "static",
  "tag",
  "audiostreamindex",
  "subtitlestreamindex",
  "subtitlemethod",
  "starttimeticks"
]);

function normalizeWorkerCacheParamName(name = "") {
  return String(name || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function shouldStripWorkerCacheQueryParam(name = "") {
  return WORKER_CACHE_DROP_QUERY_PARAMS.has(normalizeWorkerCacheParamName(name));
}

function normalizeWorkerCacheUrl(url) {
  const normalizedUrl = url instanceof URL ? new URL(url.toString()) : new URL(String(url || ""));
  normalizedUrl.hash = "";
  const keptParams = [];
  for (const [key, value] of normalizedUrl.searchParams.entries()) {
    if (shouldStripWorkerCacheQueryParam(key)) continue;
    keptParams.push([key, value]);
  }
  keptParams.sort((a, b) => {
    const keyDiff = a[0].localeCompare(b[0]);
    if (keyDiff !== 0) return keyDiff;
    return String(a[1]).localeCompare(String(b[1]));
  });
  normalizedUrl.search = "";
  for (const [key, value] of keptParams) normalizedUrl.searchParams.append(key, value);
  return normalizedUrl;
}

function normalizeMetadataCachePath(pathname = "") {
  const rawPath = String(pathname || "");
  const match = /\/(?:Videos|Audio)\/.+$/i.exec(rawPath);
  return match ? match[0] : rawPath;
}

function buildWorkerCacheKey(url) {
  try {
    return new Request(normalizeWorkerCacheUrl(url).toString(), { method: "GET" });
  } catch {
    return null;
  }
}

function isTranscodingManifestUrl(url) {
  try {
    const normalizedUrl = url instanceof URL ? new URL(url.toString()) : new URL(String(url || ""));
    for (const [key, value] of normalizedUrl.searchParams.entries()) {
      const lowerKey = String(key || "").toLowerCase();
      const lowerValue = String(value || "").toLowerCase();
      if (lowerKey.includes("transcod") || lowerValue.includes("transcod")) return true;
    }
    return false;
  } catch {
    return true;
  }
}

function isWhitelistedMetadataManifestUrl(url) {
  try {
    const normalizedUrl = url instanceof URL ? new URL(url.toString()) : new URL(String(url || ""));
    const normalizedPath = normalizeMetadataCachePath(normalizedUrl.pathname || "");
    if (!GLOBALS.Regex.ManifestExt.test(normalizedPath)) return false;
    if (isTranscodingManifestUrl(normalizedUrl)) return false;
    if (!WORKER_METADATA_MANIFEST_ALLOWED_PATHS.some(rule => rule.test(normalizedPath))) return false;
    for (const [key] of normalizedUrl.searchParams.entries()) {
      if (shouldStripWorkerCacheQueryParam(key)) continue;
      if (!WORKER_METADATA_MANIFEST_ALLOWED_PARAMS.has(normalizeWorkerCacheParamName(key))) return false;
    }
    return true;
  } catch {
    return false;
  }
}

function shouldWorkerCacheMetadataUrl(url) {
  try {
    const normalizedUrl = url instanceof URL ? new URL(url.toString()) : new URL(String(url || ""));
    const pathname = normalizedUrl.pathname || "";
    if (GLOBALS.Regex.EmbyImages.test(pathname) || GLOBALS.Regex.ImageExt.test(pathname)) return true;
    if (GLOBALS.Regex.SubtitleExt.test(pathname)) return true;
    if (GLOBALS.Regex.ManifestExt.test(pathname)) return isWhitelistedMetadataManifestUrl(normalizedUrl);
    return false;
  } catch {
    return false;
  }
}

function isHeavyVideoBytePath(pathname = "") {
  const lowerPath = String(pathname || "").toLowerCase();
  if (!lowerPath) return false;
  if (/\.(?:mp4|m4v|mkv|mov|avi|wmv|flv|ts|m4s)(?:$|[?#])/.test(lowerPath)) return true;
  if (GLOBALS.Regex.ManifestExt.test(lowerPath) || GLOBALS.Regex.SubtitleExt.test(lowerPath)) return false;
  return /\/videos\/[^/]+\/(?:stream|original|download|file)\b/.test(lowerPath) || /\/items\/[^/]+\/download\b/.test(lowerPath);
}

function collectMetadataUrlStrings(input, collector = new Set(), depth = 0) {
  if (input === null || input === undefined || depth > 5) return collector;
  if (typeof input === "string") {
    const value = input.trim();
    if (value && /^(?:https?:\/\/|\/)/i.test(value)) {
      const lowerValue = value.toLowerCase();
      const matchTarget = lowerValue.split(/[?#]/, 1)[0] || lowerValue;
      if (
        GLOBALS.Regex.ManifestExt.test(matchTarget) ||
        GLOBALS.Regex.SubtitleExt.test(matchTarget) ||
        GLOBALS.Regex.EmbyImages.test(lowerValue) ||
        GLOBALS.Regex.ImageExt.test(matchTarget)
      ) {
        collector.add(value);
      }
    }
    return collector;
  }
  if (Array.isArray(input)) {
    input.slice(0, 24).forEach(item => collectMetadataUrlStrings(item, collector, depth + 1));
    return collector;
  }
  if (typeof input === "object") {
    Object.values(input).slice(0, 32).forEach(value => collectMetadataUrlStrings(value, collector, depth + 1));
  }
  return collector;
}

function extractProxyItemId(proxyPath = "") {
  const match = /^\/Items\/([^/]+)(?:\/|$)/i.exec(String(proxyPath || ""));
  return match ? safeDecodeSegment(match[1]) : "";
}

function rankMetadataWarmPath(pathname = "") {
  const lowerPath = String(pathname || "").toLowerCase();
  if (GLOBALS.Regex.EmbyImages.test(lowerPath) || GLOBALS.Regex.ImageExt.test(lowerPath)) return 0;
  if (GLOBALS.Regex.ManifestExt.test(lowerPath)) return 1;
  if (GLOBALS.Regex.SubtitleExt.test(lowerPath)) return 2;
  return 3;
}

const DEFAULT_WANGPAN_DIRECT_TERMS = [
  "115.com", "anxia.com", "jianguoyun", "aliyundrive", "alipan", "aliyundrive.net", "alicloudccp", "myqcloud", "aliyuncs",
  "189.cn", "ctyun.cn", "baidu", "baidupcs", "123pan", "qiniudn", "qbox.me", "myhuaweicloud", "139.com",
  "quark", "yun.uc.cn", "r2.cloudflarestorage", "volces.com", "tos-s3"
];
const DEFAULT_WANGPAN_DIRECT_TEXT = DEFAULT_WANGPAN_DIRECT_TERMS.join(",");

function escapeRegexLiteral(value = "") {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function parseKeywordTerms(raw = "") {
  return String(raw || "")
    .split(/[\n\r,，;；|]+/)
    .map(item => item.trim())
    .filter(Boolean);
}

function buildKeywordFuzzyRegex(raw = "", fallbackTerms = []) {
  const baseTerms = parseKeywordTerms(raw);
  const fallbackList = Array.isArray(fallbackTerms) ? fallbackTerms : parseKeywordTerms(String(fallbackTerms || ""));
  const mergedTerms = baseTerms.length ? baseTerms : fallbackList;
  if (!mergedTerms.length) return null;
  try {
    return new RegExp(mergedTerms.map(escapeRegexLiteral).join("|"), "i");
  } catch {
    return null;
  }
}

function getWangpanDirectText(raw = "") {
  const terms = parseKeywordTerms(raw);
  return (terms.length ? terms : DEFAULT_WANGPAN_DIRECT_TERMS).join(",");
}

function shouldDirectByWangpan(targetUrl, customKeywords = "") {
  let haystack = "";
  try {
    const url = targetUrl instanceof URL ? targetUrl : new URL(String(targetUrl));
    haystack = `${url.hostname} ${url.href}`;
  } catch {
    haystack = String(targetUrl || "");
  }
  const matchRegex = buildKeywordFuzzyRegex(customKeywords, DEFAULT_WANGPAN_DIRECT_TERMS);
  return !!matchRegex && matchRegex.test(haystack);
}

function normalizeNodeNameList(input) {
  const rawList = Array.isArray(input)
    ? input
    : String(input || "").split(/[\\r\\n,，;；|]+/);
  const seen = new Set();
  const result = [];
  for (const item of rawList) {
    const value = String(item || "").trim();
    if (!value) continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(value);
  }
  return result;
}

function isNodeDirectSourceEnabled(node, currentConfig = null) {
  const configuredDirectNodes = normalizeNodeNameList(currentConfig?.sourceDirectNodes ?? currentConfig?.directSourceNodes ?? currentConfig?.nodeDirectList ?? []);
  const nodeName = String(node?.name || "").trim();
  if (nodeName && configuredDirectNodes.some(item => item.toLowerCase() === nodeName.toLowerCase())) return true;
  const proxyMode = String(node?.proxyMode || node?.mode || "").trim().toLowerCase();
  if (["direct", "source-direct", "origin-direct", "node-direct"].includes(proxyMode)) return true;
  if (node?.direct === true || node?.sourceDirect === true || node?.directSource === true || node?.direct2xx === true) return true;
  const explicitText = `${node?.tag || ""} ${node?.remark || ""}`;
  return /(?:^|[\s\[(【])(?:直连|source-direct|origin-direct|node-direct)(?:$|[\s\])】])/i.test(explicitText);
}

function resolveRedirectTarget(location, baseUrl) {
  if (!location) return null;
  try {
    return new URL(location, baseUrl instanceof URL ? baseUrl : String(baseUrl || ""));
  } catch {
    return null;
  }
}

function normalizeRedirectMethod(status, method = "GET") {
  const upperMethod = String(method || "GET").toUpperCase();
  if (status === 303 && upperMethod !== "GET" && upperMethod !== "HEAD") return "GET";
  if ((status === 301 || status === 302) && upperMethod === "POST") return "GET";
  return upperMethod;
}

const CF_DASH_CACHE_VERSION = 5;

function makeCfDashCacheKey(zoneId, dateKey = "") {
  const safeZoneId = encodeURIComponent(String(zoneId || "default").trim() || "default");
  const safeDateKey = encodeURIComponent(String(dateKey || "current").trim() || "current");
  return `sys:cf_dash_cache:${safeZoneId}:${safeDateKey}`;
}

function getVideoRequestWhereClause(column = "request_path") {
  return `(${column} LIKE '%/stream%' OR ${column} LIKE '%/master.m3u8%' OR ${column} LIKE '%/videos/%/original%' OR ${column} LIKE '%/videos/%/download%' OR ${column} LIKE '%/videos/%/file%' OR ${column} LIKE '%/items/%/download%' OR ${column} LIKE '%Static=true%' OR ${column} LIKE '%Download=true%')`;
}

function parseHostnameCandidate(rawHostname) {
  const host = String(rawHostname || "").trim().toLowerCase();
  if (!host) return null;
  const wildcard = host.includes("*");
  const cleaned = host.replace(/^\*\./, "").replace(/^\*+/, "").replace(/\*+$/g, "").replace(/^\.+|\.+$/g, "");
  if (!cleaned) return null;
  return { hostname: cleaned, wildcard };
}

function normalizeHostnameText(rawHostname) {
  return parseHostnameCandidate(rawHostname)?.hostname || "";
}

function isHostnameInsideZone(rawHostname, rawZoneName) {
  const hostname = normalizeHostnameText(rawHostname);
  const zoneName = normalizeHostnameText(rawZoneName);
  if (!hostname || !zoneName) return false;
  return hostname === zoneName || hostname.endsWith(`.${zoneName}`);
}

function extractRouteHostnameInfo(pattern) {
  const rawPattern = String(pattern || "").trim();
  if (!rawPattern) return null;
  const slashIndex = rawPattern.indexOf("/");
  const rawHost = slashIndex === -1 ? rawPattern : rawPattern.slice(0, slashIndex);
  const path = slashIndex === -1 ? "" : rawPattern.slice(slashIndex);
  const parsed = parseHostnameCandidate(rawHost);
  if (!parsed) return null;
  return { ...parsed, path, pattern: rawPattern };
}

function scoreHostnameCandidate(hostname, options = {}) {
  const path = String(options.path || "");
  let score = 0;
  if (!options.wildcard) score += 100;
  if (hostname.includes(".workers.dev")) score -= 20;
  if (path === "/" || path === "/*") score += 20;
  else if (path.endsWith("*")) score += 10;
  else if (path) score += 4;
  score += hostname.split(".").length * 4;
  score -= Math.min(path.length, 30);
  return score;
}

async function fetchCloudflareApiJson(url, apiToken, init = {}) {
  const extraInit = /** @type {any} */ (init && typeof init === "object" ? init : {});
  let extraHeaders = {};
  const rawHeaders = extraInit?.headers;
  if (rawHeaders) {
    if (rawHeaders instanceof Headers) extraHeaders = Object.fromEntries(rawHeaders.entries());
    else if (typeof rawHeaders === "object") extraHeaders = rawHeaders;
  }
  const res = await fetch(url, {
    ...extraInit,
    headers: { "Authorization": `Bearer ${apiToken}`, "Content-Type": "application/json", ...extraHeaders }
  });
  if (!res.ok) throw new Error(`cf_api_http_${res.status}`);
  /** @type {JsonApiEnvelope} */
  const payload = await res.json();
  if (payload?.success === false) {
    const msg = Array.isArray(payload?.errors) ? payload.errors.map(item => item?.message).filter(Boolean).join("; ") : "";
    throw new Error(msg || "cf_api_error");
  }
  return payload;
}

async function fetchCloudflareGraphQL(apiToken, query, variables) {
  const body = variables && typeof variables === "object"
    ? { query, variables }
    : { query };
  const cfRes = await fetch("https://api.cloudflare.com/client/v4/graphql", {
    method: "POST",
    headers: { "Authorization": `Bearer ${apiToken}`, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  if (!cfRes.ok) throw new Error(`cf_graphql_http_${cfRes.status}`);
  /** @type {JsonApiEnvelope} */
  const cfData = await cfRes.json();
  if (Array.isArray(cfData?.errors) && cfData.errors.length) {
    throw new Error(cfData.errors.map(item => item?.message).filter(Boolean).join("; ") || "cf_graphql_error");
  }
  return cfData;
}

async function fetchCloudflareGraphQLZone(zoneId, apiToken, query, variables) {
  const cfData = await fetchCloudflareGraphQL(apiToken, query, variables);
  return cfData?.data?.viewer?.zones?.[0] || null;
}

async function fetchCloudflareGraphQLAccount(accountId, apiToken, query, variables) {
  const cfData = await fetchCloudflareGraphQL(apiToken, query, variables);
  return cfData?.data?.viewer?.accounts?.[0] || null;
}

async function fetchCloudflareZoneDetails(zoneId, apiToken) {
  if (!zoneId || !apiToken) return null;
  const payload = await fetchCloudflareApiJson(`https://api.cloudflare.com/client/v4/zones/${encodeURIComponent(String(zoneId).trim())}`, apiToken);
  return payload?.result || null;
}

async function resolveCloudflareWorkerServices({ cfAccountId, cfZoneId, cfApiToken }) {
  const serviceNames = new Set();
  const pushName = (rawName) => {
    const name = String(rawName || "").trim();
    if (!name) return;
    serviceNames.add(name);
  };

  if (cfAccountId && cfZoneId) {
    try {
      const url = `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(String(cfAccountId).trim())}/workers/domains?zone_id=${encodeURIComponent(String(cfZoneId).trim())}`;
      const payload = await fetchCloudflareApiJson(url, cfApiToken);
      for (const item of payload?.result || []) {
        pushName(item?.service || item?.script || item?.name);
      }
    } catch (e) {
      console.log("CF Workers domains service lookup failed", e);
    }
  }

  if (cfZoneId) {
    try {
      let page = 1;
      let totalPages = 1;
      do {
        const url = `https://api.cloudflare.com/client/v4/zones/${encodeURIComponent(String(cfZoneId).trim())}/workers/routes?page=${page}&per_page=100`;
        const payload = await fetchCloudflareApiJson(url, cfApiToken);
        totalPages = Number(payload?.result_info?.total_pages || payload?.result_info?.totalPages || 1);
        for (const item of payload?.result || []) {
          pushName(item?.script || item?.service);
        }
        page += 1;
      } while (page <= totalPages && page <= 5);
    } catch (e) {
      console.log("CF Workers routes service lookup failed", e);
    }
  }

  return [...serviceNames];
}

async function fetchCloudflareWorkerUsageMetrics({ cfAccountId, cfZoneId, cfApiToken, startIso, endIso }) {
  if (!cfAccountId || !cfApiToken) return null;
  const serviceNames = await resolveCloudflareWorkerServices({ cfAccountId, cfZoneId, cfApiToken });
  if (!serviceNames.length) return null;

  const query = `
  query {
    viewer {
      accounts(filter: { accountTag: ${toGraphQLString(cfAccountId)} }) {
        workersInvocationsAdaptive(limit: 10000, filter: { datetime_geq: ${toGraphQLString(startIso)}, datetime_leq: ${toGraphQLString(endIso)}, scriptName_in: ${toGraphQLStringArray(serviceNames)} }) {
          dimensions { datetime scriptName status }
          sum { requests }
        }
      }
    }
  }`;

  const accountData = await fetchCloudflareGraphQLAccount(cfAccountId, cfApiToken, query);
  const records = Array.isArray(accountData?.workersInvocationsAdaptive) ? accountData.workersInvocationsAdaptive : [];
  const hourlySeries = Array.from({ length: 24 }, (_, hour) => ({ label: String(hour).padStart(2, "0") + ":00", total: 0 }));

  let totalRequests = 0;
  for (const item of records) {
    const req = Number(item?.sum?.requests) || 0;
    totalRequests += req;

    const dtRaw = item?.dimensions?.datetime;
    if (!dtRaw) continue;
    const dt = new Date(dtRaw);
    if (Number.isNaN(dt.getTime())) continue;
    const hour = (dt.getUTCHours() + 8) % 24;
    if (hourlySeries[hour]) hourlySeries[hour].total += req;
  }

  return { totalRequests, hourlySeries, serviceNames };
}

async function resolveCloudflareBoundHostname({ cfAccountId, cfZoneId, cfApiToken, zoneNameFallback = "" }) {
  const candidates = [];
  const pushCandidate = (rawHostname, options = {}) => {
    const parsed = parseHostnameCandidate(rawHostname);
    if (!parsed) return;
    const wildcard = options.wildcard === true || parsed.wildcard === true;
    candidates.push({
      hostname: parsed.hostname,
      path: String(options.path || ""),
      wildcard,
      score: scoreHostnameCandidate(parsed.hostname, { wildcard, path: options.path || "" })
    });
  };

  if (cfAccountId && cfZoneId) {
    try {
      const url = `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(String(cfAccountId).trim())}/workers/domains?zone_id=${encodeURIComponent(String(cfZoneId).trim())}`;
      const payload = await fetchCloudflareApiJson(url, cfApiToken);
      for (const item of payload?.result || []) {
        pushCandidate(item?.hostname);
      }
    } catch (e) {
      console.log("CF Workers domains lookup failed, will try routes", e);
    }
  }

  if (!candidates.length && cfZoneId) {
    try {
      let page = 1;
      let totalPages = 1;
      do {
        const url = `https://api.cloudflare.com/client/v4/zones/${encodeURIComponent(String(cfZoneId).trim())}/workers/routes?page=${page}&per_page=100`;
        const payload = await fetchCloudflareApiJson(url, cfApiToken);
        totalPages = Number(payload?.result_info?.total_pages || payload?.result_info?.totalPages || 1);
        for (const item of payload?.result || []) {
          const info = extractRouteHostnameInfo(item?.pattern);
          if (!info) continue;
          pushCandidate(info.hostname, { wildcard: info.wildcard, path: info.path });
        }
        page += 1;
      } while (page <= totalPages && page <= 5);
    } catch (e) {
      console.log("CF Workers routes lookup failed", e);
    }
  }

  if (candidates.length) {
    candidates.sort((a, b) => (b.score - a.score) || (a.hostname.length - b.hostname.length) || a.hostname.localeCompare(b.hostname));
    return candidates[0].hostname;
  }

  return zoneNameFallback || "未知域名 (请配置 CF 联动)";
}

function sanitizeRuntimeConfig(input = {}) {
  const sanitized = sanitizeConfigWithRules(input, CONFIG_SANITIZE_RULES, { normalizeNodeNameList });
  sanitized.prewarmDepth = normalizePrewarmDepth(sanitized.prewarmDepth);
  delete sanitized.dashboardAutoRefreshEnabled;
  delete sanitized.dashboardAutoRefreshSeconds;
  return sanitized;
}

function serializeConfigValue(value) {
  if (Array.isArray(value)) return JSON.stringify(value);
  if (isPlainObject(value)) return JSON.stringify(value);
  if (value === undefined) return "";
  return JSON.stringify(value);
}

function getConfigDiffEntries(prevConfig = {}, nextConfig = {}) {
  const prev = sanitizeRuntimeConfig(prevConfig);
  const next = sanitizeRuntimeConfig(nextConfig);
  const keys = [...new Set([...Object.keys(prev), ...Object.keys(next)])].sort();
  const entries = [];
  for (const key of keys) {
    if (serializeConfigValue(prev[key]) === serializeConfigValue(next[key])) continue;
    entries.push({
      key,
      previousValue: prev[key],
      nextValue: next[key]
    });
  }
  return entries;
}

function classifyCloudflareAnalyticsError(message, options = {}) {
  const raw = String(message || "").trim();
  const lower = raw.toLowerCase();
  const zoneId = String(options.zoneId || "").trim();
  const result = {
    status: "CF 查询失败",
    hint: "Cloudflare 查询失败，请检查 Zone ID、API 令牌与资源范围",
    detail: raw || (zoneId ? `当前查询的 Zone ID: ${zoneId}` : "")
  };
  if (!raw) return result;
  if (lower.includes("unknown field") || lower.includes("unknown enum") || lower.includes("error parsing args")) {
    return {
      status: "Schema 不兼容",
      hint: "当前账号可用的 GraphQL schema 与脚本查询字段不一致",
      detail: raw
    };
  }
  if (lower.includes("cf_graphql_http_429") || lower.includes("rate limit") || lower.includes("too many requests")) {
    return {
      status: "请求过于频繁",
      hint: "Cloudflare GraphQL 已限流，请稍后再试",
      detail: raw
    };
  }
  if (lower.includes("invalid token") || lower.includes("authentication") || lower.includes("cf_graphql_http_401")) {
    return {
      status: "令牌无效",
      hint: "Cloudflare API 令牌无效，或未启用 GraphQL Analytics 访问",
      detail: raw
    };
  }
  if (lower.includes("not authorized") || lower.includes("permission") || lower.includes("forbidden") || lower.includes("unauthorized") || lower.includes("cf_graphql_http_403")) {
    return {
      status: "权限或范围不匹配",
      hint: "令牌权限不足，或 Account / Zone Resources 未覆盖当前查询",
      detail: raw + (zoneId ? ` | Zone ID: ${zoneId}` : "")
    };
  }
  if (lower.includes("zone") && (lower.includes("not found") || lower.includes("invalid") || lower.includes("unknown"))) {
    return {
      status: "Zone ID 无效",
      hint: "Zone ID 无效，或当前令牌无法访问这个 Zone",
      detail: raw + (zoneId ? ` | Zone ID: ${zoneId}` : "")
    };
  }
  if (lower.includes("cf_graphql_http_400")) {
    return {
      status: "请求参数无效",
      hint: "GraphQL 请求参数无效，请检查 Zone ID 与筛选条件",
      detail: raw + (zoneId ? ` | Zone ID: ${zoneId}` : "")
    };
  }
  return result;
}

async function getRuntimeConfig(env) {
  const kv = Auth.getKV(env);
  if (!kv) return {};
  const now = nowMs();
  const cacheNamespace = String(
    env?.__CONFIG_CACHE_NAMESPACE
    || env?.__WORKER_CACHE_SCOPE
    || (env?.ENI_KV ? "ENI_KV" : "")
    || (env?.KV ? "KV" : "")
    || (env?.EMBY_KV ? "EMBY_KV" : "")
    || (env?.EMBY_PROXY ? "EMBY_PROXY" : "")
    || "default"
  );
  if (GLOBALS.ConfigCache && GLOBALS.ConfigCache.exp > now && GLOBALS.ConfigCache.data && GLOBALS.ConfigCache.namespace === cacheNamespace) return GLOBALS.ConfigCache.data;
  let config = {};
  try { config = sanitizeRuntimeConfig(await kv.get(Database.CONFIG_KEY, { type: "json" }) || {}); } catch {}
  GLOBALS.ConfigCache = { data: config, exp: now + 60000, namespace: cacheNamespace };
  return config;
}

function parseCookieHeader(cookieHeader) {
  const map = new Map();
  if (!cookieHeader || typeof cookieHeader !== "string") return map;
  for (const rawPart of cookieHeader.split(";")) {
    const part = rawPart.trim();
    if (!part) continue;
    const eqIndex = part.indexOf("=");
    const key = (eqIndex === -1 ? part : part.slice(0, eqIndex)).trim();
    const value = eqIndex === -1 ? "" : part.slice(eqIndex + 1).trim();
    if (!key) continue;
    map.set(key, value);
  }
  return map;
}

function serializeCookieMap(cookieMap) {
  const parts = [];
  for (const [key, value] of cookieMap.entries()) {
    parts.push(value === "" ? key : `${key}=${value}`);
  }
  return parts.join("; ");
}

function mergeAndSanitizeCookieHeaders(baseCookieHeader, extraCookieHeader, blockedCookieNames = ["auth_token"]) {
  const blocked = new Set(blockedCookieNames.map(name => String(name || "").trim().toLowerCase()).filter(Boolean));
  const merged = parseCookieHeader(baseCookieHeader);
  for (const key of [...merged.keys()]) {
    if (blocked.has(String(key).trim().toLowerCase())) merged.delete(key);
  }
  const extra = parseCookieHeader(extraCookieHeader);
  for (const [key, value] of extra.entries()) {
    if (blocked.has(String(key).trim().toLowerCase())) continue;
    merged.set(key, value);
  }
  const result = serializeCookieMap(merged);
  return result || null;
}

function jsonHeaders(extra = {}) {
  return { ...GLOBALS.SecurityHeaders, ...corsHeaders, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store, max-age=0", ...extra };
}

function jsonResponse(payload, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(payload), { status, headers: jsonHeaders(extraHeaders) });
}

function jsonError(code, message, status = 400, details = null, extraHeaders = {}) {
  const body = { ok: false, error: { code, message } };
  if (details !== null && details !== undefined) body.error.details = details;
  return jsonResponse(body, status, extraHeaders);
}

function isPlainObject(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function mergeStatusPatch(base, patch) {
  const source = isPlainObject(base) ? base : {};
  const delta = isPlainObject(patch) ? patch : {};
  const merged = { ...source };
  for (const [key, value] of Object.entries(delta)) {
    if (value === undefined) continue;
    if (isPlainObject(value) && isPlainObject(source[key])) merged[key] = mergeStatusPatch(source[key], value);
    else if (isPlainObject(value)) merged[key] = mergeStatusPatch({}, value);
    else merged[key] = value;
  }
  return merged;
}

async function normalizeJsonApiResponse(response) {
  const headers = new Headers(response.headers || {});
  headers.set("Content-Type", "application/json; charset=utf-8");
  headers.set("Cache-Control", "no-store, max-age=0");
  Object.entries(corsHeaders).forEach(([k, v]) => headers.set(k, v));
  applySecurityHeaders(headers);
  if (response.ok) return new Response(response.body, { status: response.status, headers });
  let payload = null, fallbackText = "";
  try { payload = await response.clone().json(); } catch { fallbackText = await response.text().catch(() => ""); }
  const code = payload?.error?.code || (typeof payload?.error === "string" ? payload.error.toUpperCase() : `HTTP_${response.status}`);
  const message = payload?.error?.message || payload?.message || (typeof payload?.error === "string" ? payload.error : fallbackText || response.statusText || "request_failed");
  const details = payload?.error?.details ?? payload?.details ?? null;
  return jsonError(code, message, response.status || 500, details);
}

const nowMs = () => Date.now();
const sleepMs = (ms) => new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));

function setBoundedMapEntry(map, key, value, maxSize) {
  if (map.has(key)) map.delete(key);
  map.set(key, value);
  const limit = Math.floor(Number(maxSize));
  if (!Number.isFinite(limit) || limit < 1) return;
  while (map.size > limit) {
    const oldestKey = map.keys().next().value;
    if (oldestKey === undefined) break;
    map.delete(oldestKey);
  }
}

function touchMapEntry(map, key) {
  if (!map.has(key)) return undefined;
  const value = map.get(key);
  map.delete(key);
  map.set(key, value);
  return value;
}

function clampIntegerConfig(value, fallback, min, max) {
  let num;
  if (typeof value === "number") num = value;
  else if (typeof value === "string") {
    const normalized = value.trim();
    if (!/^-?\d+$/.test(normalized)) return fallback;
    num = Number(normalized);
  } else {
    return fallback;
  }
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, Math.floor(num)));
}

function clampNumberConfig(value, fallback, min, max) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
}

const CONFIG_SANITIZE_RULES = {
  trimFields: ["tgBotToken", "tgChatId", "cfAccountId", "cfZoneId", "cfApiToken", "corsOrigins", "geoAllowlist", "geoBlocklist", "ipBlacklist", "wangpandirect", "prewarmDepth"],
  arrayNormalizers: {
    sourceDirectNodes: "nodeNameList"
  },
  integerFields: {
    logRetentionDays: { fallback: Config.Defaults.LogRetentionDays, min: 1, max: Config.Defaults.LogRetentionDaysMax },
    logFlushCountThreshold: { fallback: Config.Defaults.LogFlushCountThreshold, min: 1, max: 5000 },
    logBatchChunkSize: { fallback: Config.Defaults.LogBatchChunkSize, min: 1, max: 100 },
    logBatchRetryCount: { fallback: Config.Defaults.LogBatchRetryCount, min: 0, max: 5 },
    logBatchRetryBackoffMs: { fallback: Config.Defaults.LogBatchRetryBackoffMs, min: 0, max: 5000 },
    scheduledLeaseMs: { fallback: Config.Defaults.ScheduledLeaseMs, min: Config.Defaults.ScheduledLeaseMinMs, max: 15 * 60 * 1000 },
    uiRadiusPx: { fallback: Config.Defaults.UiRadiusPx, min: 0, max: 48 },
    tgAlertDroppedBatchThreshold: { fallback: Config.Defaults.TgAlertDroppedBatchThreshold, min: 0, max: 5000 },
    tgAlertFlushRetryThreshold: { fallback: Config.Defaults.TgAlertFlushRetryThreshold, min: 0, max: 10 },
    tgAlertCooldownMinutes: { fallback: Config.Defaults.TgAlertCooldownMinutes, min: 1, max: 1440 },
    cacheTtlImages: { fallback: Config.Defaults.CacheTtlImagesDays, min: 0, max: 365 },
    pingTimeout: { fallback: Config.Defaults.PingTimeoutMs, min: 1000, max: 180000 },
    pingCacheMinutes: { fallback: Config.Defaults.PingCacheMinutes, min: 0, max: 1440 },
    upstreamTimeoutMs: { fallback: Config.Defaults.UpstreamTimeoutMs, min: 0, max: 180000 },
    upstreamRetryAttempts: { fallback: Config.Defaults.UpstreamRetryAttempts, min: 0, max: 3 },
    prewarmCacheTtl: { fallback: Config.Defaults.PrewarmCacheTtl, min: 0, max: 3600 },
    prewarmPrefetchBytes: { fallback: Config.Defaults.PrewarmPrefetchBytes, min: 0, max: 64 * 1024 * 1024 }
  },
  numberFields: {
    logWriteDelayMinutes: { fallback: Config.Defaults.LogFlushDelayMinutes, min: 0, max: 1440 }
  },
  booleanTrueFields: [],
  booleanFalseFields: ["tgAlertOnScheduledFailure", "directStaticAssets", "directHlsDash", "disablePrewarmPrefetch", "nodePanelPingAutoSort"]
};

function sanitizeConfigWithRules(input = {}, rules = CONFIG_SANITIZE_RULES, helpers = {}) {
  const config = input && typeof input === "object" ? { ...input } : {};
  for (const key of rules.trimFields || []) {
    if (config[key] === undefined || config[key] === null) continue;
    config[key] = String(config[key]).trim();
  }
  for (const [key, normalizerName] of Object.entries(rules.arrayNormalizers || {})) {
    if (!Array.isArray(config[key])) continue;
    if (normalizerName === "nodeNameList" && typeof helpers.normalizeNodeNameList === "function") {
      config[key] = helpers.normalizeNodeNameList(config[key]);
    }
  }
  for (const [key, rule] of Object.entries(rules.integerFields || {})) {
    config[key] = clampIntegerConfig(config[key], rule.fallback, rule.min, rule.max);
  }
  for (const [key, rule] of Object.entries(rules.numberFields || {})) {
    config[key] = clampNumberConfig(config[key], rule.fallback, rule.min, rule.max);
  }
  for (const key of rules.booleanTrueFields || []) {
    config[key] = config[key] !== false;
  }
  for (const key of rules.booleanFalseFields || []) {
    config[key] = config[key] === true;
  }
  return config;
}

async function runWithConcurrency(items, limit, worker) {
  const results = [], executing = [];
  for (const item of items) {
    const p = Promise.resolve().then(() => worker(item));
    results.push(p);
    if (limit <= items.length) {
      const e = p.catch(() => {}).then(() => {
        const index = executing.indexOf(e);
        if (index >= 0) executing.splice(index, 1);
      });
      executing.push(e);
      if (executing.length >= limit) await Promise.race(executing);
    }
  }
  return Promise.all(results);
}

// ============================================================================
// 1. 认证模块 (AUTH MODULE)
// ============================================================================
const Auth = {
  getKV(env) { return env.ENI_KV || env.KV || env.EMBY_KV || env.EMBY_PROXY; },
  async handleLogin(request, env) {
    const ip = request.headers.get("cf-connecting-ip") || "unknown";
    const kv = this.getKV(env);
    const adminCookiePath = getAdminCookiePath(env);
    
    const config = await getRuntimeConfig(env);
    const jwtDays = Math.max(1, parseInt(config.jwtExpiryDays) || 30);
    const expSeconds = jwtDays * 86400;
    
    const safeKVGet = async (key) => kv ? await kv.get(key).catch(e => null) : null;
    const safeKVPut = async (key, val, opts) => kv ? await kv.put(key, val, opts).catch(e => null) : null;
    const safeKVDelete = async (key) => kv ? await kv.delete(key).catch(e => null) : null;
    try {
      const failKey = `fail:${ip}`;
      const prev = await safeKVGet(failKey);
      const failCount = prev ? parseInt(prev) : 0;
      if (failCount >= Config.Defaults.MaxLoginAttempts) return jsonError("TOO_MANY_ATTEMPTS", "账户已锁定，请稍后再试", 429);
      let password = "";
      const ct = request.headers.get("content-type") || "";
      if (ct.includes("application/json")) {
        const body = await request.json();
        password = (body.password || "").trim();
      }
      if (!env.JWT_SECRET) return jsonError("SERVER_MISCONFIGURED", "JWT_SECRET 未配置", 503);
      if (!env.ADMIN_PASS) return jsonError("SERVER_MISCONFIGURED", "ADMIN_PASS 未配置", 503);
      if (password && password === env.ADMIN_PASS) {
        await safeKVDelete(failKey);
        const jwt = await this.generateJwt(env.JWT_SECRET, expSeconds);
        return jsonResponse({ ok: true, expiresIn: expSeconds }, 200, { "Set-Cookie": `auth_token=${jwt}; Path=${adminCookiePath}; Max-Age=${expSeconds}; HttpOnly; Secure; SameSite=Strict` });
      }
      await safeKVPut(failKey, (failCount + 1).toString(), { expirationTtl: Config.Defaults.LoginLockDuration });
      return jsonResponse({ ok: false, error: { code: "INVALID_PASSWORD", message: "密码错误" }, remain: Math.max(0, Config.Defaults.MaxLoginAttempts - (failCount + 1)) }, 401);
    } catch (e) {
      return jsonError("INVALID_REQUEST", "请求无效", 400, { reason: e.message });
    }
  },
  async verifyRequest(request, env) {
    try {
      const secret = env.JWT_SECRET;
      if (!secret) return false;
      const auth = request.headers.get("Authorization") || "";
      let token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
      if (!token) {
        const match = (request.headers.get("Cookie") || "").match(/(?:^|;\s*)auth_token=([^;]+)/);
        token = match ? match[1] : null;
      }
      if (!token) return false;
      return await this.verifyJwt(token, secret);
    } catch { return false; }
  },
  async generateJwt(secret, expiresIn) {
    const encHeader = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" })).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
    const encPayload = btoa(JSON.stringify({ sub: "admin", exp: Math.floor(Date.now() / 1000) + expiresIn })).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
    const signature = await this.sign(secret, `${encHeader}.${encPayload}`);
    return `${encHeader}.${encPayload}.${signature}`;
  },
  async verifyJwt(token, secret) {
    const parts = token.split(".");
    if (parts.length !== 3) return false;
    if (parts[2] !== await this.sign(secret, `${parts[0]}.${parts[1]}`)) return false;
    try { return JSON.parse(atob(parts[1].replace(/-/g, "+").replace(/_/g, "/"))).exp > Math.floor(Date.now() / 1000); } catch { return false; }
  },
  async sign(secret, data) {
    const enc = new TextEncoder(), now = Date.now();
    let entry = GLOBALS.CryptoKeyCache.get(secret);
    if (!entry || entry.exp <= now) {
      const key = await crypto.subtle.importKey("raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      entry = { key, exp: now + Config.Defaults.CryptoKeyCacheTTL * 1000 };
      setBoundedMapEntry(GLOBALS.CryptoKeyCache, secret, entry, Config.Defaults.CryptoKeyCacheMax);
    } else {
      setBoundedMapEntry(GLOBALS.CryptoKeyCache, secret, entry, Config.Defaults.CryptoKeyCacheMax);
    }
    const signature = await crypto.subtle.sign("HMAC", entry.key, enc.encode(data));
    return btoa(String.fromCharCode(...new Uint8Array(signature))).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  }
};

// ============================================================================
// 2. 数据库与缓存模块 (DATABASE & CACHE MODULE)
// ============================================================================
const CacheManager = {
  async getNodesList(env, ctx) {
    if (GLOBALS.NodesListCache && GLOBALS.NodesListCache.exp > nowMs()) return GLOBALS.NodesListCache.data;
    const kv = Database.getKV(env);
    if (!kv) return [];
    let nodeNames = GLOBALS.NodesIndexCache?.exp > nowMs() ? GLOBALS.NodesIndexCache.data : null;
    if (!nodeNames) {
      try {
        nodeNames = await kv.get(Database.NODES_INDEX_KEY, { type: "json" });
        if (Array.isArray(nodeNames)) GLOBALS.NodesIndexCache = { data: nodeNames, exp: nowMs() + 60000 };
      } catch (e) {}
    }
    if (!nodeNames || !Array.isArray(nodeNames)) {
      try {
        const list = await kv.list({ prefix: "node:" });
        nodeNames = list.keys.map(k => k.name.replace("node:", ""));
        if (ctx && nodeNames.length > 0) ctx.waitUntil(kv.put(Database.NODES_INDEX_KEY, JSON.stringify(nodeNames)));
        GLOBALS.NodesIndexCache = { data: nodeNames, exp: nowMs() + 60000 };
      } catch (e) { return []; }
    }
    const nodes = await runWithConcurrency(nodeNames, Config.Defaults.NodesReadConcurrency, async (name) => {
      try {
        const cached = GLOBALS.NodeCache.get(name);
        let val = null;
        if (cached?.exp > nowMs()) {
          touchMapEntry(GLOBALS.NodeCache, name);
          val = cached.data;
        }
        if (!val) val = await kv.get(`${Database.PREFIX}${name}`, { type: "json" });
        if (!val) return null;
        const { data: normalized, changed } = Database.normalizeNode(name, val);
        if (changed && ctx) ctx.waitUntil(kv.put(`${Database.PREFIX}${name}`, JSON.stringify(normalized)));
        setBoundedMapEntry(GLOBALS.NodeCache, name, { data: normalized, exp: nowMs() + Config.Defaults.CacheTTL }, Config.Defaults.NodeCacheMax);
        return { name, ...normalized };
      } catch { return null; }
    });
    const validNodes = nodes.filter(Boolean);
    GLOBALS.NodesListCache = { data: validNodes, exp: nowMs() + 60000 };
    return validNodes;
  },
  async invalidateList(ctx) { GLOBALS.NodesListCache = null; },
  maybeCleanup() {
    const budget = Config.Defaults.CleanupBudgetMs;
    const chunkSize = Config.Defaults.CleanupChunkSize;
    const state = GLOBALS.CleanupState;
    const iterators = state.iterators || (state.iterators = { node: null, crypto: null, rate: null, log: null });
    const now = nowMs();
    const start = now;
    const cleanMap = (map, shouldDelete, iteratorKey) => {
      let iterator = iterators[iteratorKey];
      if (!iterator) {
        iterator = map.entries();
        iterators[iteratorKey] = iterator;
      }
      let scanned = 0;
      while (scanned < chunkSize && (nowMs() - start) < budget) {
        const next = iterator.next();
        if (next.done) {
          iterators[iteratorKey] = null;
          break;
        }
        scanned += 1;
        const [k, v] = next.value;
        if (!map.has(k)) continue;
        if (shouldDelete(v, now)) map.delete(k);
      }
    };
    if (state.phase === 0) {
      cleanMap(GLOBALS.NodeCache, v => v?.exp && v.exp < now, "node");
      state.phase = 1;
    } else if (state.phase === 1) {
      cleanMap(GLOBALS.CryptoKeyCache, v => v?.exp && v.exp < now, "crypto");
      state.phase = 2;
    } else if (state.phase === 2) {
      cleanMap(GLOBALS.RateLimitCache, v => !v || v.resetAt < now, "rate");
      state.phase = 3;
    } else {
      cleanMap(GLOBALS.LogDedupe, v => !v || (now - v) > 300000, "log");
      state.phase = 0;
    }
  }
};

const Database = {
  PREFIX: "node:", CONFIG_KEY: "sys:theme", NODES_INDEX_KEY: "sys:nodes_index:v1", OPS_STATUS_KEY: "sys:ops_status:v1",
  SCHEDULED_LOCK_KEY: "sys:scheduled_lock:v1",
  CONFIG_SNAPSHOTS_KEY: "sys:config_snapshots:v1",
  DNS_RECORD_HISTORY_PREFIX: "sys:dns_record_history:v1:",
  TELEGRAM_ALERT_STATE_KEY: "sys:telegram_alert_state:v1",
  SYS_STATUS_TABLE: "sys_status",
  OPS_STATUS_DB_SCOPE_ROOT: "ops_status:root",
  OPS_STATUS_SECTION_KEYS: {
    log: "sys:ops_status:log:v1",
    scheduled: "sys:ops_status:scheduled:v1"
  },
  getKV(env) { return Auth.getKV(env); },
  getDB(env) { return env.DB || env.D1 || env.PROXY_LOGS; },
  resolveOpsStatusStores(envOrStore) {
    if (envOrStore && typeof envOrStore.prepare === "function") {
      return { kv: null, db: envOrStore };
    }
    if (envOrStore && typeof envOrStore.get === "function") {
      return { kv: envOrStore, db: null };
    }
    return {
      kv: this.getKV(envOrStore),
      db: this.getDB(envOrStore)
    };
  },
  getOpsStatusDbScope(sectionName = "") {
    return sectionName ? `ops_status:${sectionName}` : this.OPS_STATUS_DB_SCOPE_ROOT;
  },
  async ensureSysStatusTable(db) {
    if (!db || typeof db.prepare !== "function") return false;
    let initTask = GLOBALS.OpsStatusDbReady.get(db);
    if (!initTask) {
      initTask = (async () => {
        try {
          await db.prepare(`CREATE TABLE IF NOT EXISTS ${this.SYS_STATUS_TABLE} (scope TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at INTEGER NOT NULL)`).run();
          await db.prepare(`CREATE INDEX IF NOT EXISTS idx_sys_status_updated_at ON ${this.SYS_STATUS_TABLE} (updated_at DESC)`).run();
          return true;
        } catch (error) {
          console.warn("sys_status init failed", error);
          return false;
        }
      })();
      GLOBALS.OpsStatusDbReady.set(db, initTask);
    }
    return await initTask;
  },
  async getOpsStatusPayloadFromDb(db, scope) {
    if (!db || !scope) return null;
    const ready = await this.ensureSysStatusTable(db);
    if (!ready) return null;
    try {
      const row = await db.prepare(`SELECT payload FROM ${this.SYS_STATUS_TABLE} WHERE scope = ? LIMIT 1`).bind(scope).first();
      if (!row?.payload) return null;
      return typeof row.payload === "string" ? JSON.parse(row.payload) : row.payload;
    } catch {
      return null;
    }
  },
  async putOpsStatusPayloadToDb(db, scope, payload, updatedAtMs) {
    if (!db || !scope || !payload || typeof payload !== "object") return false;
    const ready = await this.ensureSysStatusTable(db);
    if (!ready) return false;
    await db.prepare(`INSERT INTO ${this.SYS_STATUS_TABLE} (scope, payload, updated_at) VALUES (?, ?, ?)
      ON CONFLICT(scope) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at`).bind(scope, JSON.stringify(payload), Number(updatedAtMs) || nowMs()).run();
    return true;
  },
  getOpsStatusSectionEntries() {
    return Object.entries(this.OPS_STATUS_SECTION_KEYS);
  },
  async getOpsStatusRootFromStores(stores) {
    const kv = stores?.kv || null;
    const db = stores?.db || null;
    if (db) {
      const dbRoot = await this.getOpsStatusPayloadFromDb(db, this.getOpsStatusDbScope());
      if (dbRoot && typeof dbRoot === "object") return dbRoot;
    }
    if (!kv) return {};
    try { return await kv.get(this.OPS_STATUS_KEY, { type: "json" }) || {}; } catch { return {}; }
  },
  async getOpsStatusRoot(envOrStore) {
    return this.getOpsStatusRootFromStores(this.resolveOpsStatusStores(envOrStore));
  },
  async getOpsStatusSectionFromStores(stores, sectionName) {
    const kv = stores?.kv || null;
    const db = stores?.db || null;
    if (!sectionName) return {};
    const sectionKey = this.OPS_STATUS_SECTION_KEYS[sectionName];
    if (!sectionKey) return {};
    const loadSectionValue = async () => {
      if (db) {
        const dbValue = await this.getOpsStatusPayloadFromDb(db, this.getOpsStatusDbScope(sectionName));
        if (dbValue && typeof dbValue === "object") return dbValue;
      }
      if (!kv) return null;
      try {
        return await kv.get(sectionKey, { type: "json" });
      } catch {
        return null;
      }
    };
    const [root, sectionValue] = await Promise.all([
      this.getOpsStatusRootFromStores(stores),
      loadSectionValue()
    ]);
    const rootSection = root && typeof root[sectionName] === "object" ? root[sectionName] : {};
    return mergeStatusPatch(rootSection, sectionValue && typeof sectionValue === "object" ? sectionValue : {});
  },
  async getOpsStatusSection(envOrStore, sectionName) {
    return this.getOpsStatusSectionFromStores(this.resolveOpsStatusStores(envOrStore), sectionName);
  },
  async getOpsStatusFromStores(stores) {
    const kv = stores?.kv || null;
    const db = stores?.db || null;
    if (!kv && !db) return {};
    const root = await this.getOpsStatusRootFromStores(stores);
    const status = root && typeof root === "object" ? { ...root } : {};
    let latestUpdatedAt = typeof status.updatedAt === "string" ? status.updatedAt : "";
    const sectionEntries = await Promise.all(this.getOpsStatusSectionEntries().map(async ([sectionName]) => {
      const sectionValue = await this.getOpsStatusSectionFromStores(stores, sectionName);
      return [sectionName, sectionValue];
    }));
    for (const [sectionName, sectionValue] of sectionEntries) {
      if (!sectionValue || typeof sectionValue !== "object") continue;
      if (!Object.keys(sectionValue).length) continue;
      status[sectionName] = mergeStatusPatch(status[sectionName], sectionValue);
      if (typeof sectionValue.updatedAt === "string" && sectionValue.updatedAt > latestUpdatedAt) latestUpdatedAt = sectionValue.updatedAt;
    }
    if (latestUpdatedAt) status.updatedAt = latestUpdatedAt;
    return status;
  },
  async getOpsStatus(envOrStore) {
    return this.getOpsStatusFromStores(this.resolveOpsStatusStores(envOrStore));
  },
  async patchOpsStatus(envOrKv, patch, ctx = null) {
    const stores = this.resolveOpsStatusStores(envOrKv);
    if (!stores.kv && !stores.db) return {};
    const patchObject = patch && typeof patch === "object" ? patch : {};
    const sectionPatches = [];
    const rootPatch = {};
    for (const [key, value] of Object.entries(patchObject)) {
      if (this.OPS_STATUS_SECTION_KEYS[key]) sectionPatches.push([key, value]);
      else rootPatch[key] = value;
    }
    const runPatch = async () => {
      const nowIso = new Date().toISOString();
      const updatedAtMs = nowMs();
      const useDb = stores.db && await this.ensureSysStatusTable(stores.db);
      if (Object.keys(rootPatch).length > 0) {
        const currentRoot = await this.getOpsStatusRootFromStores(stores);
        const nextRoot = mergeStatusPatch(currentRoot, rootPatch);
        nextRoot.updatedAt = nowIso;
        if (useDb) await this.putOpsStatusPayloadToDb(stores.db, this.getOpsStatusDbScope(), nextRoot, updatedAtMs);
        else if (stores.kv) await stores.kv.put(this.OPS_STATUS_KEY, JSON.stringify(nextRoot));
      }
      for (const [sectionName, sectionPatch] of sectionPatches) {
        const currentSection = await this.getOpsStatusSectionFromStores(stores, sectionName);
        const nextSection = mergeStatusPatch(currentSection, sectionPatch);
        nextSection.updatedAt = nowIso;
        if (useDb) await this.putOpsStatusPayloadToDb(stores.db, this.getOpsStatusDbScope(sectionName), nextSection, updatedAtMs);
        else if (stores.kv) await stores.kv.put(this.OPS_STATUS_SECTION_KEYS[sectionName], JSON.stringify(nextSection));
      }
      return this.getOpsStatusFromStores(stores);
    };
    const task = Promise.resolve(GLOBALS.OpsStatusWriteChain)
      .catch(() => {})
      .then(runPatch);
    GLOBALS.OpsStatusWriteChain = task.catch(() => {});
    if (ctx) ctx.waitUntil(task);
    else await task;
    return task;
  },
  async tryAcquireScheduledLease(kv, options = {}) {
    if (!kv) return { acquired: false, reason: "kv_unavailable" };
    const now = nowMs();
    const leaseMs = Math.max(Config.Defaults.ScheduledLeaseMinMs, Number(options.leaseMs) || Config.Defaults.ScheduledLeaseMs);
    const token = String(options.token || `${now}-${Math.random().toString(36).slice(2, 10)}`);
    const owner = String(options.owner || "scheduled");
    let current = null;
    try {
      current = await kv.get(this.SCHEDULED_LOCK_KEY, { type: "json" });
    } catch {}
    if (current && Number(current.expiresAt) > now) {
      return { acquired: false, reason: "lease_held", lock: current };
    }
    const nextLock = {
      token,
      owner,
      acquiredAt: new Date(now).toISOString(),
      expiresAt: now + leaseMs
    };
    await kv.put(this.SCHEDULED_LOCK_KEY, JSON.stringify(nextLock));
    let confirmed = null;
    try {
      confirmed = await kv.get(this.SCHEDULED_LOCK_KEY, { type: "json" });
    } catch {}
    if (confirmed && confirmed.token === token) return { acquired: true, leaseMs, lock: confirmed };
    return { acquired: false, reason: "lease_contended", lock: confirmed };
  },
  async renewScheduledLease(kv, token, leaseMs, options = {}) {
    if (!kv || !token) return null;
    const now = nowMs();
    const safeLeaseMs = Math.max(Config.Defaults.ScheduledLeaseMinMs, Number(leaseMs) || Config.Defaults.ScheduledLeaseMs);
    try {
      const current = await kv.get(this.SCHEDULED_LOCK_KEY, { type: "json" });
      if (!current || current.token !== token) return null;
      const nextLock = {
        ...current,
        owner: String(options.owner || current.owner || "scheduled"),
        renewedAt: new Date(now).toISOString(),
        expiresAt: now + safeLeaseMs
      };
      await kv.put(this.SCHEDULED_LOCK_KEY, JSON.stringify(nextLock));
      const confirmed = await kv.get(this.SCHEDULED_LOCK_KEY, { type: "json" });
      return confirmed && confirmed.token === token ? confirmed : null;
    } catch {
      return null;
    }
  },
  async releaseScheduledLease(kv, token) {
    if (!kv || !token) return false;
    try {
      const current = await kv.get(this.SCHEDULED_LOCK_KEY, { type: "json" });
      if (!current || current.token !== token) return false;
      await kv.delete(this.SCHEDULED_LOCK_KEY);
      return true;
    } catch {
      return false;
    }
  },
  normalizeNodeIndex(index = []) {
    return [...new Set((Array.isArray(index) ? index : []).map(name => String(name || "").toLowerCase().trim()).filter(Boolean))];
  },
  async getNodesIndex(kv) {
    if (GLOBALS.NodesIndexCache?.exp > nowMs() && Array.isArray(GLOBALS.NodesIndexCache.data)) {
      return [...GLOBALS.NodesIndexCache.data];
    }
    if (!kv) return [];
    const index = this.normalizeNodeIndex(await kv.get(this.NODES_INDEX_KEY, { type: "json" }) || []);
    GLOBALS.NodesIndexCache = { data: index, exp: nowMs() + 60000 };
    return [...index];
  },
  /**
   * @param {string | string[]} [nodeNames=[]]
   * @param {{ invalidateList?: boolean }} [options={}]
   */
  invalidateNodeCaches(nodeNames = [], options = {}) {
    for (const rawName of Array.isArray(nodeNames) ? nodeNames : [nodeNames]) {
      const name = String(rawName || "").toLowerCase().trim();
      if (!name) continue;
      GLOBALS.NodeCache.delete(name);
    }
    if (options.invalidateList) GLOBALS.NodesListCache = null;
  },
  /**
   * @param {string[]} index
   * @param {PersistNodesIndexOptions} [options={}]
   */
  async persistNodesIndex(index, options = {}) {
    const { kv, ctx, invalidateList = false } = options;
    const normalizedIndex = this.normalizeNodeIndex(index);
    GLOBALS.NodesIndexCache = { data: normalizedIndex, exp: nowMs() + 60000 };
    if (invalidateList) GLOBALS.NodesListCache = null;
    if (!kv) return normalizedIndex;
    const task = kv.put(this.NODES_INDEX_KEY, JSON.stringify(normalizedIndex));
    if (ctx) ctx.waitUntil(task);
    else await task;
    return normalizedIndex;
  },
  getDnsRecordHistoryKey(zoneId, recordId) {
    const safeZoneId = encodeURIComponent(String(zoneId || "").trim() || "default");
    const safeRecordId = encodeURIComponent(String(recordId || "").trim() || "unknown");
    return `${this.DNS_RECORD_HISTORY_PREFIX}${safeZoneId}:${safeRecordId}`;
  },
  normalizeDnsHistoryValueKey(type, content) {
    return `${String(type || "").trim().toUpperCase()}::${String(content || "").trim().toLowerCase()}`;
  },
  normalizeDnsRecordHistoryEntry(entry = {}) {
    /** @type {DnsRecordHistoryEntryLike} */
    const input = entry && typeof entry === "object" ? entry : {};
    const type = String(input.type || "").trim().toUpperCase();
    const content = String(input.content || "").trim();
    const rawSavedAt = String(input.savedAt || input.updatedAt || input.createdAt || "").trim();
    const parsedSavedAt = rawSavedAt ? new Date(rawSavedAt) : null;
    const savedAt = parsedSavedAt && !Number.isNaN(parsedSavedAt.getTime())
      ? parsedSavedAt.toISOString()
      : new Date().toISOString();
    return {
      id: String(input.id || `dns-hist-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`),
      name: String(input.name || "").trim(),
      type,
      content,
      savedAt,
      actor: String(input.actor || "admin").trim() || "admin",
      source: String(input.source || "ui").trim() || "ui",
      requestHost: normalizeHostnameText(input.requestHost)
    };
  },
  normalizeDnsRecordHistory(entries = []) {
    const history = [];
    const seen = new Set();
    for (const rawEntry of Array.isArray(entries) ? entries : []) {
      const entry = this.normalizeDnsRecordHistoryEntry(rawEntry);
      if (!entry.type || !entry.content) continue;
      const historyKey = this.normalizeDnsHistoryValueKey(entry.type, entry.content);
      if (seen.has(historyKey)) continue;
      seen.add(historyKey);
      history.push(entry);
      if (history.length >= Config.Defaults.DnsHistoryLimit) break;
    }
    return history;
  },
  async getDnsRecordHistory(kv, zoneId, recordId) {
    if (!kv || !zoneId || !recordId) return [];
    try {
      const stored = await kv.get(this.getDnsRecordHistoryKey(zoneId, recordId), { type: "json" });
      return this.normalizeDnsRecordHistory(stored);
    } catch {
      return [];
    }
  },
  async persistDnsRecordHistory(kv, zoneId, recordId, entries) {
    if (!kv || !zoneId || !recordId) return [];
    const normalizedHistory = this.normalizeDnsRecordHistory(entries);
    await kv.put(this.getDnsRecordHistoryKey(zoneId, recordId), JSON.stringify(normalizedHistory));
    return normalizedHistory;
  },
  async recordDnsRecordHistory(kv, zoneId, recordId, entry = {}) {
    if (!kv || !zoneId || !recordId) return [];
    const currentHistory = await this.getDnsRecordHistory(kv, zoneId, recordId);
    const normalizedEntry = this.normalizeDnsRecordHistoryEntry(entry);
    if (!normalizedEntry.type || !normalizedEntry.content) return currentHistory;
    const nextValueKey = this.normalizeDnsHistoryValueKey(normalizedEntry.type, normalizedEntry.content);
    const currentValueKey = currentHistory[0]
      ? this.normalizeDnsHistoryValueKey(currentHistory[0].type, currentHistory[0].content)
      : "";
    if (currentValueKey && currentValueKey === nextValueKey) return currentHistory;
    return this.persistDnsRecordHistory(kv, zoneId, recordId, [normalizedEntry, ...currentHistory]);
  },
  getCurrentDateKey(now = new Date()) {
    const utc8Now = new Date(now.getTime() + 8 * 3600 * 1000);
    return `${utc8Now.getUTCFullYear()}-${String(utc8Now.getUTCMonth() + 1).padStart(2, "0")}-${String(utc8Now.getUTCDate()).padStart(2, "0")}`;
  },
  buildConfigCacheKeys(...configs) {
    const dateKey = this.getCurrentDateKey();
    const staleKeys = new Set(["sys:cf_dash_cache"]);
    for (const config of configs) {
      staleKeys.add(makeCfDashCacheKey(config?.cfZoneId));
      staleKeys.add(makeCfDashCacheKey(config?.cfZoneId, dateKey));
    }
    return [...staleKeys].filter(Boolean);
  },
  async listKvKeys(kv, options = {}) {
    if (!kv || typeof kv.list !== "function") return [];
    const prefix = String(options.prefix || "");
    const collected = [];
    let cursor = "";
    let guard = 0;
    while (guard < 1000) {
      guard += 1;
      const page = cursor
        ? await kv.list({ prefix, cursor })
        : await kv.list({ prefix });
      for (const item of page?.keys || []) {
        const name = String(item?.name || "").trim();
        if (name) collected.push(name);
      }
      const nextCursor = typeof page?.cursor === "string" ? page.cursor : "";
      if (page?.list_complete === true || !nextCursor) break;
      cursor = nextCursor;
    }
    return [...new Set(collected)];
  },
  async readRepairableRuntimeConfig(kv) {
    if (!kv) return { config: {}, hadMalformedValue: false, source: "missing" };
    let rawText = null;
    try {
      rawText = await kv.get(this.CONFIG_KEY);
    } catch {
      return { config: {}, hadMalformedValue: true, source: "read_failed" };
    }
    if (rawText === null || rawText === undefined || rawText === "") {
      return { config: {}, hadMalformedValue: false, source: "missing" };
    }
    try {
      const parsed = JSON.parse(String(rawText));
      return {
        config: sanitizeRuntimeConfig(isPlainObject(parsed) ? parsed : {}),
        hadMalformedValue: !isPlainObject(parsed),
        source: "text_json"
      };
    } catch {
      return { config: {}, hadMalformedValue: true, source: "text_invalid_json" };
    }
  },
  shouldRunKvTidy(lastTidiedAt, options = {}) {
    const now = Number(options.nowMs) || nowMs();
    const minIntervalMs = Math.max(0, Number(options.minIntervalMs) || Config.Defaults.KvTidyIntervalMs);
    if (options.force === true) return true;
    const parsedLastTidiedAt = typeof lastTidiedAt === "string" ? new Date(lastTidiedAt).getTime() : NaN;
    if (!Number.isFinite(parsedLastTidiedAt)) return true;
    return (now - parsedLastTidiedAt) >= minIntervalMs;
  },
  async tidyKvData(env, options = {}) {
    const kv = options.kv || this.getKV(env);
    const ctx = options.ctx || null;
    if (!kv) throw new Error("KV not configured");

    const allKeys = await this.listKvKeys(kv);
    const nodeNames = [];
    const removableKeys = new Set();
    const knownSectionKeys = new Set(Object.values(this.OPS_STATUS_SECTION_KEYS));
    let untouchedOtherKeyCount = 0;

    for (const keyName of allKeys) {
      if (!keyName) continue;
      if (keyName.startsWith(this.PREFIX)) {
        nodeNames.push(keyName.slice(this.PREFIX.length));
        continue;
      }
      if (keyName === "sys:cf_dash_cache" || keyName.startsWith("sys:cf_dash_cache:")) {
        removableKeys.add(keyName);
        continue;
      }
      if (keyName === this.SCHEDULED_LOCK_KEY) {
        let shouldDeleteLock = false;
        try {
          const lock = await kv.get(this.SCHEDULED_LOCK_KEY, { type: "json" });
          shouldDeleteLock = !lock || Number(lock.expiresAt) <= nowMs();
        } catch {
          shouldDeleteLock = true;
        }
        if (shouldDeleteLock) removableKeys.add(keyName);
        continue;
      }
      if (
        keyName === this.CONFIG_KEY
        || keyName === this.NODES_INDEX_KEY
        || keyName === this.CONFIG_SNAPSHOTS_KEY
        || keyName === this.OPS_STATUS_KEY
        || keyName === this.TELEGRAM_ALERT_STATE_KEY
        || knownSectionKeys.has(keyName)
        || keyName.startsWith(this.DNS_RECORD_HISTORY_PREFIX)
      ) {
        continue;
      }
      untouchedOtherKeyCount += 1;
    }

    const repairedTheme = await this.readRepairableRuntimeConfig(kv);
    const config = await this.persistRuntimeConfig(repairedTheme.config, {
      env,
      kv,
      ctx,
      snapshotMeta: {
        reason: "tidy_kv_data",
        section: "all",
        source: "kv_tidy",
        actor: "admin",
        note: repairedTheme.hadMalformedValue ? "repair_malformed_sys_theme" : "sanitize_runtime_config"
      }
    });
    const rebuiltNodeIndex = await this.persistNodesIndex(nodeNames, { kv, ctx, invalidateList: true });
    const removableKeyList = [...removableKeys].sort();
    if (removableKeyList.length) {
      const deleteTasks = removableKeyList.map(key => kv.delete(key));
      if (ctx) ctx.waitUntil(Promise.all(deleteTasks));
      else await Promise.all(deleteTasks);
    }

    GLOBALS.ConfigCache = null;
    GLOBALS.NodesListCache = null;
    GLOBALS.NodeCache.clear();

    return {
      config,
      nodesIndex: rebuiltNodeIndex,
      summary: {
        scannedKeyCount: allKeys.length,
        preservedNodeKeyCount: nodeNames.length,
        rebuiltNodeCount: rebuiltNodeIndex.length,
        deletedKeyCount: removableKeyList.length,
        deletedCacheKeyCount: removableKeyList.filter(key => key === "sys:cf_dash_cache" || key.startsWith("sys:cf_dash_cache:")).length,
        deletedExpiredScheduledLock: removableKeys.has(this.SCHEDULED_LOCK_KEY),
        untouchedOtherKeyCount,
        themeWasMalformed: repairedTheme.hadMalformedValue,
        themeReadSource: repairedTheme.source
      }
    };
  },
  shouldRunLogsVacuum(lastVacuumAt, options = {}) {
    const now = Number(options.nowMs) || nowMs();
    const minIntervalMs = Math.max(0, Number(options.minIntervalMs) || Config.Defaults.LogVacuumMinIntervalMs);
    if (options.force === true) return true;
    const parsedLastVacuumAt = typeof lastVacuumAt === "string" ? new Date(lastVacuumAt).getTime() : NaN;
    if (!Number.isFinite(parsedLastVacuumAt)) return true;
    return (now - parsedLastVacuumAt) >= minIntervalMs;
  },
  async vacuumLogsDb(db) {
    if (!db) return false;
    await db.prepare("VACUUM").run();
    return true;
  },
  /**
   * @param {ConfigSnapshotMeta} [meta={}]
   */
  normalizeConfigSnapshotMeta(meta = {}) {
    /** @type {ConfigSnapshotMeta} */
    const input = meta && typeof meta === "object" ? meta : {};
    return {
      reason: String(input.reason || "save_config").trim() || "save_config",
      section: String(input.section || "all").trim() || "all",
      actor: String(input.actor || "admin").trim() || "admin",
      source: String(input.source || "ui").trim() || "ui",
      note: String(input.note || "").trim()
    };
  },
  async getConfigSnapshots(kv, options = {}) {
    if (!kv) return [];
    let rawSnapshots = [];
    try {
      const stored = await kv.get(this.CONFIG_SNAPSHOTS_KEY, { type: "json" });
      rawSnapshots = Array.isArray(stored) ? stored : [];
    } catch {}
    const includeConfig = options.withConfig === true;
    return rawSnapshots
      .filter(item => item && typeof item === "object" && Array.isArray(item.changedKeys) && item.createdAt)
      .map(item => includeConfig ? { ...item } : {
        id: item.id,
        createdAt: item.createdAt,
        reason: item.reason,
        section: item.section,
        actor: item.actor,
        source: item.source,
        note: item.note || "",
        changedKeys: [...item.changedKeys],
        changeCount: Number(item.changeCount) || item.changedKeys.length || 0
      });
  },
  async getConfigSnapshotById(kv, snapshotId) {
    const snapshots = await this.getConfigSnapshots(kv, { withConfig: true });
    return snapshots.find(item => item.id === snapshotId) || null;
  },
  async clearConfigSnapshots(kv) {
    if (!kv) return;
    await kv.delete(this.CONFIG_SNAPSHOTS_KEY);
  },
  async recordConfigSnapshot(kv, prevConfig, nextConfig, meta = {}) {
    if (!kv) return null;
    const diffEntries = getConfigDiffEntries(prevConfig, nextConfig);
    if (!diffEntries.length) return null;
    const snapshotMeta = this.normalizeConfigSnapshotMeta(meta);
    const currentSnapshots = await this.getConfigSnapshots(kv, { withConfig: true });
    const snapshot = {
      id: `cfg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      createdAt: new Date().toISOString(),
      reason: snapshotMeta.reason,
      section: snapshotMeta.section,
      actor: snapshotMeta.actor,
      source: snapshotMeta.source,
      note: snapshotMeta.note,
      changedKeys: diffEntries.map(item => item.key),
      changeCount: diffEntries.length,
      config: sanitizeRuntimeConfig(prevConfig)
    };
    const nextSnapshots = [snapshot, ...currentSnapshots].slice(0, Config.Defaults.ConfigSnapshotLimit);
    await kv.put(this.CONFIG_SNAPSHOTS_KEY, JSON.stringify(nextSnapshots));
    return snapshot;
  },
  /**
   * @param {any} rawConfig
   * @param {PersistRuntimeConfigOptions} [options={}]
   */
  async persistRuntimeConfig(rawConfig, options = {}) {
    const { env, kv, ctx, snapshotMeta } = options;
    if (!kv) return sanitizeRuntimeConfig(rawConfig);
    const prevConfig = env
      ? await getRuntimeConfig(env)
      : sanitizeRuntimeConfig(await kv.get(this.CONFIG_KEY, { type: "json" }) || {});
    const nextConfig = sanitizeRuntimeConfig(rawConfig);
    await this.recordConfigSnapshot(kv, prevConfig, nextConfig, snapshotMeta);
    await kv.put(this.CONFIG_KEY, JSON.stringify(nextConfig));
    GLOBALS.ConfigCache = null;
    const deleteTasks = this.buildConfigCacheKeys(prevConfig, nextConfig).map(key => kv.delete(key));
    if (deleteTasks.length) {
      if (ctx) ctx.waitUntil(Promise.all(deleteTasks));
      else await Promise.all(deleteTasks);
    }
    return nextConfig;
  },
  async sendTelegramMessage({ tgBotToken, tgChatId, text }) {
      const botToken = String(tgBotToken || "").trim();
      const chatId = String(tgChatId || "").trim();
      if (!botToken || !chatId) throw new Error("请先完善 Telegram Bot Token 和 Chat ID 配置");
      const tgUrl = `https://api.telegram.org/bot${botToken}/sendMessage`;
      const res = await fetch(tgUrl, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({ chat_id: chatId, text: String(text || "") })
      });
      /** @type {JsonApiEnvelope} */
      const tgData = await res.json();
      if (!tgData.ok) throw new Error(tgData.description || "Telegram API 返回错误");
      return tgData;
  },
  
  async sendDailyTelegramReport(env) {
      const db = this.getDB(env);
      const kv = this.getKV(env);
      if (!db || !kv) throw new Error("Database or KV not configured");

      const config = await kv.get(this.CONFIG_KEY, { type: "json" }) || {};
      const tgBotToken = String(config.tgBotToken || "").trim();
      const tgChatId = String(config.tgChatId || "").trim();
      const cfAccountId = String(config.cfAccountId || "").trim();
      const cfZoneId = String(config.cfZoneId || "").trim();
      const cfApiToken = String(config.cfApiToken || "").trim();
      if (!tgBotToken || !tgChatId) throw new Error("请先完善 Telegram Bot Token 和 Chat ID 配置");

      const now = new Date();
      const utc8Ms = now.getTime() + 8 * 3600 * 1000;
      const d = new Date(utc8Ms);
      const yyyy = d.getUTCFullYear();
      const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(d.getUTCDate()).padStart(2, '0');
      const todayStr = `${mm}-${dd}`;
      const dateString = `${yyyy}-${mm}-${dd}`;

      const startOfDayTs = Date.UTC(yyyy, d.getUTCMonth(), d.getUTCDate()) - 8 * 3600 * 1000;
      const endOfDayTs = startOfDayTs + 86400000 - 1;
      const videoWhereClause = getVideoRequestWhereClause();

      let reqTotal = 0, playCount = 0, infoCount = 0;
      let cfTrafficStatus = "未找到今日缓存 (需打开面板刷新)";
      let domainName = cfZoneId ? "Cloudflare (读取自缓存)" : "未接入 CF (读取自缓存)";

      try {
          const cacheKey = makeCfDashCacheKey(cfZoneId, dateString);
          let cached = await kv.get(cacheKey, { type: "json" });
          
          // 👇 加回这三行：如果缓存不存在，让定时任务主动假装前端请求一次，生成最新数据
          if (!cached || cached.ver !== CF_DASH_CACHE_VERSION) {
              await this.ApiHandlers.getDashboardStats({}, { env, ctx: null, kv, db }).catch(() => null);
              cached = await kv.get(cacheKey, { type: "json" });
          }

          if (cached && cached.ver === CF_DASH_CACHE_VERSION) {
              reqTotal = Number(cached.todayRequests) || 0;
              cfTrafficStatus = cached.todayTraffic || "0 B";
              if (cfTrafficStatus === "未配置") cfTrafficStatus = "缓存暂无流量数据";
              playCount = cached.playCount || 0;
              infoCount = cached.infoCount || 0;
          }
      } catch (e) {
          cfTrafficStatus = "读取面板缓存异常";
          console.log("Read CF cache failed", e);
      }

      let reqStr = reqTotal.toString();
      if (reqTotal > 1000) reqStr = (reqTotal / 1000).toFixed(2) + "k";

      const msgText = `📊 Cloudflare Zone 每日报表 (UTC+8)\n域名: ${domainName}\n\n📅 今天 (${todayStr})\n请求数: ${reqStr}\n视频流量 (CF 总计): ${cfTrafficStatus}\n请求: 播放请求 ${playCount} 次 | 获取播放信息 ${infoCount} 次\n#Cloudflare #Emby #日报`;
      await this.sendTelegramMessage({ tgBotToken, tgChatId, text: msgText });
      return true;
  },
  async maybeSendRuntimeAlerts(env, scheduledState = null) {
      const kv = this.getKV(env);
      if (!kv) return { sent: false, reason: "kv_unavailable" };
      const config = sanitizeRuntimeConfig(await getRuntimeConfig(env));
      const tgBotToken = String(config.tgBotToken || "").trim();
      const tgChatId = String(config.tgChatId || "").trim();
      if (!tgBotToken || !tgChatId) return { sent: false, reason: "telegram_not_configured" };

      const droppedThreshold = clampIntegerConfig(config.tgAlertDroppedBatchThreshold, Config.Defaults.TgAlertDroppedBatchThreshold, 0, 5000);
      const retryThreshold = clampIntegerConfig(config.tgAlertFlushRetryThreshold, Config.Defaults.TgAlertFlushRetryThreshold, 0, 10);
      const cooldownMinutes = clampIntegerConfig(config.tgAlertCooldownMinutes, Config.Defaults.TgAlertCooldownMinutes, 1, 1440);
      const alertOnScheduledFailure = config.tgAlertOnScheduledFailure === true;
      if (droppedThreshold <= 0 && retryThreshold <= 0 && !alertOnScheduledFailure) {
        return { sent: false, reason: "thresholds_disabled" };
      }

      const opsStatus = await this.getOpsStatus(env);
      const log = opsStatus && typeof opsStatus.log === "object" ? opsStatus.log : {};
      const scheduled = scheduledState && typeof scheduledState === "object" && Object.keys(scheduledState).length
        ? scheduledState
        : (opsStatus && typeof opsStatus.scheduled === "object" ? opsStatus.scheduled : {});
      const issues = [];

      const droppedCount = Number(log.lastDroppedBatchSize) || 0;
      if (droppedThreshold > 0 && droppedCount >= droppedThreshold) {
        issues.push({
          code: "log_drop",
          message: `日志刷盘疑似丢弃批次：${droppedCount} 条（阈值 ${droppedThreshold}）`,
          eventAt: log.lastFlushErrorAt || log.lastOverflowAt || log.updatedAt || opsStatus.updatedAt || ""
        });
      }

      const retryCount = Number(log.lastFlushRetryCount) || 0;
      if (retryThreshold > 0 && retryCount >= retryThreshold) {
        issues.push({
          code: "log_retry",
          message: `D1 写入重试次数偏高：${retryCount} 次（阈值 ${retryThreshold}）`,
          eventAt: log.lastFlushAt || log.lastFlushErrorAt || log.updatedAt || opsStatus.updatedAt || ""
        });
      }

      const scheduledStatus = String(scheduled.status || "").toLowerCase();
      if (alertOnScheduledFailure && (scheduledStatus === "failed" || scheduledStatus === "partial_failure")) {
        issues.push({
          code: "scheduled_failure",
          message: `定时任务状态异常：${scheduled.status}${scheduled.lastError ? `，错误：${scheduled.lastError}` : ""}`,
          eventAt: scheduled.lastFinishedAt || scheduled.lastErrorAt || scheduled.updatedAt || opsStatus.updatedAt || ""
        });
      }

      if (!issues.length) return { sent: false, reason: "no_alerts" };

      const signature = JSON.stringify(issues.map(item => ({ code: item.code, eventAt: item.eventAt, message: item.message })));
      let lastAlertState = null;
      try {
        lastAlertState = await kv.get(this.TELEGRAM_ALERT_STATE_KEY, { type: "json" });
      } catch {}
      const now = Date.now();
      const cooldownMs = cooldownMinutes * 60 * 1000;
      if (lastAlertState && lastAlertState.signature === signature && Number(lastAlertState.sentAtMs) > 0 && (now - Number(lastAlertState.sentAtMs)) < cooldownMs) {
        return { sent: false, reason: "cooldown_active" };
      }

      const lines = [
        "⚠️ Emby Proxy 运行时异常告警",
        "",
        ...issues.map(item => `- ${item.message}`),
        "",
        `时间：${new Date().toLocaleString("zh-CN", { hour12: false, timeZone: "Asia/Shanghai" })}`,
        "#Emby #Alert"
      ];
      await this.sendTelegramMessage({ tgBotToken, tgChatId, text: lines.join("\n") });
      await kv.put(this.TELEGRAM_ALERT_STATE_KEY, JSON.stringify({
        signature,
        sentAt: new Date(now).toISOString(),
        sentAtMs: now,
        issues
      }));
      return { sent: true, issueCount: issues.length };
  },

  sanitizeHeaders(input) {
    if (!input || typeof input !== "object" || Array.isArray(input)) return {};
    const out = {};
    for (const [rawKey, rawValue] of Object.entries(input)) {
      const key = String(rawKey || "").trim();
      if (!key) continue;
      if (GLOBALS.DropRequestHeaders.has(key.toLowerCase())) continue;
      out[key] = String(rawValue ?? "");
    }
    return out;
  },
  normalizeTargets(targetValue) {
    const parts = String(targetValue || "").split(",").map(v => v.trim()).filter(Boolean);
    if (!parts.length) return null;
    const normalized = [];
    for (const part of parts) {
      try {
        const url = new URL(part);
        if (!["http:", "https:"].includes(url.protocol)) return null;
        normalized.push(url.toString().replace(/\/$/, ""));
      } catch {
        return null;
      }
    }
    return normalized.length ? normalized.join(",") : null;
  },
  normalizeSingleTarget(targetValue) {
    const normalizedTargets = this.normalizeTargets(targetValue);
    if (!normalizedTargets) return null;
    const [firstTarget] = normalizedTargets.split(",").map(item => item.trim()).filter(Boolean);
    return firstTarget || null;
  },
  buildDefaultLineName(index) {
    return `线路${Number(index) + 1}`;
  },
  normalizeLineId(value, fallbackIndex = 0) {
    const normalized = String(value || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, "-")
      .replace(/^-+|-+$/g, "");
    return normalized || `line-${Number(fallbackIndex) + 1}`;
  },
  normalizeIsoDatetime(value) {
    if (!value) return "";
    const date = new Date(value);
    return Number.isFinite(date.getTime()) ? date.toISOString() : "";
  },
  normalizeLines(rawLines, fallbackTarget = "") {
    const sourceLines = Array.isArray(rawLines) && rawLines.length
      ? rawLines
      : String(this.normalizeTargets(fallbackTarget) || "")
          .split(",")
          .map(item => item.trim())
          .filter(Boolean)
          .map((target, index) => ({
            id: `line-${index + 1}`,
            name: this.buildDefaultLineName(index),
            target
          }));
    if (!sourceLines.length) return [];

    const normalized = [];
    const usedIds = new Set();
    sourceLines.forEach((rawLine, index) => {
      const line = rawLine && typeof rawLine === "object" && !Array.isArray(rawLine)
        ? rawLine
        : { target: rawLine };
      const target = this.normalizeSingleTarget(line?.target);
      if (!target) return;

      const baseId = this.normalizeLineId(line?.id, index);
      let nextId = baseId;
      let suffix = 2;
      while (usedIds.has(nextId)) {
        nextId = `${baseId}-${suffix}`;
        suffix += 1;
      }
      usedIds.add(nextId);

      const latencyCandidate = Number(line?.latencyMs);
      normalized.push({
        id: nextId,
        name: String(line?.name || "").trim() || this.buildDefaultLineName(index),
        target,
        latencyMs: Number.isFinite(latencyCandidate) && latencyCandidate >= 0 ? Math.round(latencyCandidate) : null,
        latencyUpdatedAt: this.normalizeIsoDatetime(line?.latencyUpdatedAt)
      });
    });
    return normalized;
  },
  resolveActiveLineId(activeLineId, lines, rawLines = []) {
    if (!Array.isArray(lines) || !lines.length) return "";
    const explicitId = String(activeLineId || "").trim();
    if (explicitId && lines.some(line => line.id === explicitId)) return explicitId;

    if (Array.isArray(rawLines)) {
      for (const rawLine of rawLines) {
        if (!rawLine || typeof rawLine !== "object" || Array.isArray(rawLine) || rawLine.enabled !== true) continue;
        const rawId = String(rawLine.id || "").trim();
        if (rawId && lines.some(line => line.id === rawId)) return rawId;
        const rawTarget = this.normalizeSingleTarget(rawLine.target);
        if (!rawTarget) continue;
        const matched = lines.find(line => line.target === rawTarget);
        if (matched) return matched.id;
      }
    }

    return lines[0].id;
  },
  buildLegacyTargetFromLines(lines = []) {
    return (Array.isArray(lines) ? lines : [])
      .map(line => String(line?.target || "").trim())
      .filter(Boolean)
      .join(",");
  },
  getActiveNodeLine(node) {
    const lines = Array.isArray(node?.lines) ? node.lines : [];
    if (!lines.length) return null;
    const activeLineId = String(node?.activeLineId || "").trim();
    return lines.find(line => line.id === activeLineId) || lines[0];
  },
  getOrderedNodeLines(node) {
    const lines = Array.isArray(node?.lines) ? node.lines.slice() : [];
    if (lines.length <= 1) return lines;
    const activeLine = this.getActiveNodeLine(node);
    if (!activeLine) return lines;
    return [activeLine, ...lines.filter(line => line.id !== activeLine.id)];
  },
  sortNodeLinesByLatency(lines = []) {
    return (Array.isArray(lines) ? lines : [])
      .map((line, index) => ({ line, index }))
      .sort((left, right) => {
        const leftMs = Number.isFinite(left.line?.latencyMs) ? left.line.latencyMs : Number.POSITIVE_INFINITY;
        const rightMs = Number.isFinite(right.line?.latencyMs) ? right.line.latencyMs : Number.POSITIVE_INFINITY;
        if (leftMs !== rightMs) return leftMs - rightMs;
        return left.index - right.index;
      })
      .map(item => item.line);
  },
  isPingCacheFresh(line, cacheMinutes) {
    const latencyMs = Number(line?.latencyMs);
    const checkedAt = Date.parse(String(line?.latencyUpdatedAt || ""));
    if (!Number.isFinite(latencyMs) || !Number.isFinite(checkedAt)) return false;
    const ttlMs = Math.max(0, Number(cacheMinutes) || 0) * 60 * 1000;
    if (ttlMs <= 0) return false;
    return nowMs() - checkedAt < ttlMs;
  },
  async pingTarget(target, timeoutMs) {
    const controller = new AbortController();
    const startedAt = nowMs();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    try {
      await fetch(target, { method: "HEAD", signal: controller.signal });
      return nowMs() - startedAt;
    } catch {
      return 9999;
    } finally {
      clearTimeout(timeoutId);
    }
  },
  normalizeNode(nodeName, data) {
    const n = { ...data };
    let changed = false;
    const normalizedLines = this.normalizeLines(n.lines, n.target);
    const nextActiveLineId = this.resolveActiveLineId(n.activeLineId, normalizedLines, Array.isArray(n.lines) ? n.lines : []);
    const legacyTarget = this.buildLegacyTargetFromLines(normalizedLines);
    if (JSON.stringify(normalizedLines) !== JSON.stringify(Array.isArray(n.lines) ? n.lines : [])) changed = true;
    if (String(n.activeLineId || "") !== nextActiveLineId) changed = true;
    if (String(n.target || "") !== legacyTarget) changed = true;
    n.lines = normalizedLines;
    n.activeLineId = nextActiveLineId;
    n.target = legacyTarget;
    if (n.secret === undefined) { n.secret = ""; changed = true; }
    if (n.tag === undefined) { n.tag = ""; changed = true; }
    if (n.remark === undefined) { n.remark = ""; changed = true; }
    if (n.tagColor === undefined) { n.tagColor = ""; changed = true; }
    if (n.remarkColor === undefined) { n.remarkColor = ""; changed = true; }
    if (n.displayName === undefined) { n.displayName = ""; changed = true; }
    const normalizedHeaders = this.sanitizeHeaders(n.headers);
    if (JSON.stringify(normalizedHeaders) !== JSON.stringify(n.headers || {})) changed = true;
    n.headers = normalizedHeaders;
    delete n.videoThrottling;
    delete n.interceptMs;
    if (n.schemaVersion !== 3) { n.schemaVersion = 3; changed = true; }
    if (!n.createdAt) { n.createdAt = new Date().toISOString(); changed = true; }
    if (!n.updatedAt) { n.updatedAt = n.createdAt; changed = true; }
    return { data: n, changed };
  },
  buildNodeRecord(name, rawNode, existingNode = {}) {
    let parsedHeaders = rawNode?.headers !== undefined ? rawNode.headers : existingNode.headers;
    if (typeof parsedHeaders === "string") {
      try { parsedHeaders = JSON.parse(parsedHeaders); } catch { parsedHeaders = {}; }
    }
    const candidateRawLines = Array.isArray(rawNode?.lines)
      ? rawNode.lines
      : (rawNode?.target !== undefined ? [] : existingNode.lines);
    const candidateFallbackTarget = rawNode?.target !== undefined ? rawNode.target : existingNode.target;
    const normalizedLines = this.normalizeLines(candidateRawLines, candidateFallbackTarget);
    if (!normalizedLines.length) return null;
    const nextActiveLineId = this.resolveActiveLineId(
      rawNode?.activeLineId !== undefined ? rawNode.activeLineId : existingNode.activeLineId,
      normalizedLines,
      Array.isArray(rawNode?.lines) ? rawNode.lines : existingNode.lines
    );
    return this.normalizeNode(name, {
      target: this.buildLegacyTargetFromLines(normalizedLines),
      lines: normalizedLines,
      activeLineId: nextActiveLineId,
      secret: rawNode?.secret !== undefined ? rawNode.secret : (existingNode.secret || ""),
      tag: rawNode?.tag !== undefined ? rawNode.tag : (existingNode.tag || ""),
      remark: rawNode?.remark !== undefined ? rawNode.remark : (existingNode.remark || ""),
      tagColor: rawNode?.tagColor !== undefined ? String(rawNode.tagColor || "").trim() : (existingNode.tagColor || ""),
      remarkColor: rawNode?.remarkColor !== undefined ? String(rawNode.remarkColor || "").trim() : (existingNode.remarkColor || ""),
      displayName: rawNode?.displayName !== undefined ? String(rawNode.displayName || "").trim() : (existingNode.displayName || ""),
      headers: this.sanitizeHeaders(parsedHeaders),
      schemaVersion: 3,
      createdAt: existingNode.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString()
    }).data;
  },
  async getNode(nodeName, env, ctx) {
    nodeName = String(nodeName).toLowerCase();
    const kv = this.getKV(env); if (!kv) return null;
    const mem = GLOBALS.NodeCache.get(nodeName);
    if (mem && mem.exp > Date.now()) {
      touchMapEntry(GLOBALS.NodeCache, nodeName);
      return mem.data;
    }
    try {
      const nodeData = await kv.get(`${this.PREFIX}${nodeName}`, { type: "json" });
      if (!nodeData) return null;
      const { data: normalized, changed } = this.normalizeNode(nodeName, nodeData);
      if (changed && ctx) ctx.waitUntil(kv.put(`${this.PREFIX}${nodeName}`, JSON.stringify(normalized)));
      setBoundedMapEntry(GLOBALS.NodeCache, nodeName, { data: normalized, exp: Date.now() + Config.Defaults.CacheTTL }, Config.Defaults.NodeCacheMax);
      return normalized;
    } catch { return null; }
  },
  normalizeAdminActionRequest(input) {
    if (!input || typeof input !== "object" || Array.isArray(input)) return null;
    const payload = input.payload && typeof input.payload === "object" && !Array.isArray(input.payload)
      ? { ...input.payload }
      : null;
    const action = String(input.action ?? payload?.action ?? "").trim();
    const meta = input.meta && typeof input.meta === "object" && !Array.isArray(input.meta) ? { ...input.meta } : {};
    const data = payload
      ? { ...payload, action, meta }
      : { ...input, action, meta };
    return { action, data, meta };
  },
  // ============================================================================
  // 管理 API 动作表 (ADMIN ACTION MAP)
  // 读取导航：
  // - 面板统计 / 运行状态：getDashboardStats / getRuntimeStatus
  // - 配置与备份：loadConfig / previewConfig / saveConfig / exportConfig / importFull
  // - 节点治理：list / saveOrImport / delete / pingNode
  // - 运维动作：getLogs / clearLogs / initLogsDb / purgeCache / tidyKvData / testTelegram / sendDailyReport
  // 设计意图：
  // - 维持单文件部署，但把“动作分发”和“动作实现”拆成两个认知层次。
  // - 新增 action 时，优先在这里挂处理器，再在 handleApi 做最小派发。
  //
  // [新增] API 路由处理器 (Action Handlers)
  // 通过分离业务逻辑，消除 switch-case 带来的上下文污染
  // ============================================================================
  ApiHandlers: {
    async getDashboardStats(data, { env, ctx, kv, db }) {
      const config = sanitizeRuntimeConfig(await getRuntimeConfig(env));
      let todayRequests = 0, todayTraffic = "未配置", nodeCount = 0;
      let cfAnalyticsLoaded = false, requestsLoaded = false;
      let cfAnalyticsStatus = "", cfAnalyticsError = "", cfAnalyticsDetail = "";
      let requestSource = "pending", requestSourceText = "等待数据加载", trafficSourceText = "视频流量口径：CF Zone 总流量";
      let generatedAt = new Date().toISOString();
      let hourlySeries = Array.from({ length: 24 }, (_, hour) => ({ label: String(hour).padStart(2, "0") + ":00", total: 0 }));
      let playCount = 0, infoCount = 0;

      const nodes = await CacheManager.getNodesList(env, ctx);
      nodeCount = nodes.length || 0;

      const now = new Date();
      const utc8Ms = now.getTime() + 8 * 3600 * 1000;
      const d = new Date(utc8Ms);
      const dateString = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
      const startOfDayTs = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) - 8 * 3600 * 1000;
      const endOfDayTs = startOfDayTs + 86400000 - 1;

      const cfZoneId = String(config.cfZoneId || "").trim();
      const cfApiToken = String(config.cfApiToken || "").trim();
      const cacheKey = makeCfDashCacheKey(cfZoneId, dateString);
      let cached = await kv.get(cacheKey, { type: "json" });

      if (cached && cached.ver === CF_DASH_CACHE_VERSION && (Date.now() - cached.ts < 3600000) && Array.isArray(cached.hourlySeries)) {
          return new Response(JSON.stringify({ nodeCount, ...cached, generatedAt: cached.generatedAt || new Date(cached.ts).toISOString(), cacheStatus: "cache" }), { headers: { ...corsHeaders } });
      } 

      if (cfZoneId && cfApiToken) {
          const startIso = new Date(startOfDayTs).toISOString();
          const endIso = new Date(endOfDayTs).toISOString();
          const query = `
          query {
            viewer {
              zones(filter: { zoneTag: ${toGraphQLString(cfZoneId)} }) {
                series: httpRequestsAdaptiveGroups(limit: 10000, filter: { datetime_geq: ${toGraphQLString(startIso)}, datetime_leq: ${toGraphQLString(endIso)} }) {
                  count
                  dimensions { datetimeHour }
                  sum { edgeResponseBytes }
                }
              }
            }
          }`;
          try {
              const zoneData = await fetchCloudflareGraphQLZone(cfZoneId, cfApiToken, query);
              if (zoneData) {
                  let zoneTotalReq = 0, totalBytes = 0;
                  let zoneHourlySeries = Array.from({ length: 24 }, (_, hour) => ({ label: String(hour).padStart(2, "0") + ":00", total: 0 }));
                  const seriesData = Array.isArray(zoneData.series) ? [...zoneData.series].sort((a, b) => String(a?.dimensions?.datetimeHour || "").localeCompare(String(b?.dimensions?.datetimeHour || ""))) : [];
                  seriesData.forEach(item => {
                      const req = Number(item.count) || 0;
                      const byt = Number(item.sum?.edgeResponseBytes) || 0;
                      zoneTotalReq += req;
                      totalBytes += byt;
                      const dtRaw = item?.dimensions?.datetimeHour;
                      if (dtRaw && !Number.isNaN(new Date(dtRaw).getTime())) {
                          zoneHourlySeries[(new Date(dtRaw).getUTCHours() + 8) % 24].total += req;
                      }
                  });
                  todayTraffic = formatBytes(totalBytes);
                  cfAnalyticsLoaded = true;
                  cfAnalyticsStatus = "Cloudflare 统计正常";
                  trafficSourceText = "视频流量当前对齐：CF Zone 总流量（edgeResponseBytes）";

                  let resolvedRequestSource = "zone_analytics";
                  try {
                      const workerUsage = await fetchCloudflareWorkerUsageMetrics({ cfAccountId: String(config.cfAccountId || "").trim(), cfZoneId, cfApiToken, startIso, endIso });
                      if (workerUsage && Number.isFinite(workerUsage.totalRequests)) {
                          todayRequests = workerUsage.totalRequests;
                          hourlySeries = workerUsage.hourlySeries;
                          requestsLoaded = true;
                          resolvedRequestSource = "workers_usage";
                          requestSource = "workers_usage";
                          requestSourceText = "今日请求量当前对齐：Cloudflare Workers Usage";
                          cfAnalyticsStatus = "Cloudflare 统计正常（请求数已对齐 Workers Usage）";
                          cfAnalyticsDetail = workerUsage.serviceNames?.length ? `已对齐脚本: ${workerUsage.serviceNames.join(", ")}` : cfAnalyticsDetail;
                      }
                  } catch (e) { console.log("CF workers usage fetch failed", e); }

                  if (!requestsLoaded) {
                      todayRequests = zoneTotalReq;
                      hourlySeries = zoneHourlySeries;
                      requestsLoaded = true;
                      requestSource = "zone_analytics";
                      requestSourceText = "今日请求量当前对齐：Cloudflare Zone Analytics";
                  }
              } else {
                  cfAnalyticsStatus = "Zone 未命中";
                  cfAnalyticsError = "GraphQL 返回空；请检查 Zone ID 或权限";
                  todayTraffic = "CF 无统计数据";
              }
          } catch (e) {
              const cfDiag = classifyCloudflareAnalyticsError(e?.message || e, { zoneId: cfZoneId });
              cfAnalyticsStatus = cfDiag.status;
              cfAnalyticsError = cfDiag.hint;
              cfAnalyticsDetail = cfDiag.detail;
              todayTraffic = "CF 查询失败";
          }
      } else {
          cfAnalyticsStatus = "未配置 Cloudflare";
          cfAnalyticsError = "请在账号设置中填写并保存 Cloudflare Zone ID 与 API 令牌";
          requestSourceText = "今日请求量当前对齐：本地 D1 日志（兜底口径）";
          trafficSourceText = "视频流量当前对齐：未配置 Cloudflare，无法获取 CF Zone 总流量";
      }
            if (db) {
                try {
                    const videoWhereClause = getVideoRequestWhereClause();
                    playCount = (await db.prepare(`SELECT COUNT(*) as c FROM proxy_logs WHERE timestamp >= ? AND timestamp <= ? AND ${videoWhereClause}`).bind(startOfDayTs, endOfDayTs).first())?.c || 0;
                    infoCount = (await db.prepare(`SELECT COUNT(*) as c FROM proxy_logs WHERE timestamp >= ? AND timestamp <= ? AND request_path LIKE '%/PlaybackInfo%'`).bind(startOfDayTs, endOfDayTs).first())?.c || 0;

                    if (!requestsLoaded) {
                        todayRequests = (await db.prepare(`SELECT COUNT(*) as total FROM proxy_logs WHERE timestamp >= ? AND timestamp <= ?`).bind(startOfDayTs, endOfDayTs).first())?.total || 0;
                        const dbHourly = await db.prepare(`SELECT strftime('%H', datetime(timestamp / 1000 + 28800, 'unixepoch')) as hour, COUNT(*) as total FROM proxy_logs WHERE timestamp >= ? AND timestamp <= ? GROUP BY hour ORDER BY hour ASC`).bind(startOfDayTs, endOfDayTs).all();
                        for (const row of dbHourly?.results || []) {
                            const index = Number.parseInt(row.hour, 10);
                            if (!Number.isNaN(index) && hourlySeries[index]) hourlySeries[index].total += (Number(row.total) || 0);
                        }
                        requestsLoaded = true;
                        requestSource = "d1_logs";
                        requestSourceText = "今日请求量当前对齐：本地 D1 日志（兜底口径）";
                    }
                } catch (dbErr) {
                    // 静默吞掉错误 (如新用户尚未初始化表)，确保 CF 流量数据仍能正常下发
                    console.log("DB Stats read failed (table not init?):", dbErr);
                }
            }

            const cachePayload = JSON.stringify({
          ver: CF_DASH_CACHE_VERSION, ts: Date.now(),
          todayRequests, todayTraffic, hourlySeries,
          requestSource, requestSourceText, trafficSourceText,
          generatedAt,
          cfAnalyticsLoaded, cfAnalyticsStatus, cfAnalyticsError, cfAnalyticsDetail,
          playCount, infoCount
      });
      
      if (ctx) ctx.waitUntil(kv.put(cacheKey, cachePayload));
      else await kv.put(cacheKey, cachePayload);

      return new Response(JSON.stringify({ todayRequests, todayTraffic, nodeCount, hourlySeries, cfAnalyticsLoaded, cfAnalyticsStatus, cfAnalyticsError, cfAnalyticsDetail, requestSource, requestSourceText, trafficSourceText, generatedAt, cacheStatus: "live", playCount, infoCount }), { headers: { ...corsHeaders } });      
    },

    async loadConfig(data, { env }) {
      return new Response(JSON.stringify({ config: await getRuntimeConfig(env) }), { headers: { ...corsHeaders } });
    },

    async previewConfig(data) {
      const rawConfig = data?.config && typeof data.config === "object" && !Array.isArray(data.config)
        ? data.config
        : {};
      return jsonResponse({ config: sanitizeRuntimeConfig(rawConfig) });
    },

    async getRuntimeStatus(data, { env }) {
      return jsonResponse({ status: await Database.getOpsStatus(env) });
    },

    async saveConfig(data, { env, ctx, kv, meta }) {
      const savedConfig = data.config
        ? await Database.persistRuntimeConfig(data.config, {
            env,
            kv,
            ctx,
            snapshotMeta: {
              reason: "save_config",
              section: String(meta?.section || "all"),
              source: String(meta?.source || "ui"),
              actor: "admin"
            }
          })
        : await getRuntimeConfig(env);
      return jsonResponse({ success: true, config: savedConfig });
    },

    async exportConfig(data, { env, ctx }) {
      return new Response(JSON.stringify({ 
        version: Config.Defaults.Version, 
        exportTime: new Date().toISOString(), 
        nodes: (await CacheManager.getNodesList(env, ctx)).filter(Boolean), 
        config: await getRuntimeConfig(env) 
      }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    },

    async exportSettings(data, { env }) {
      return jsonResponse({
        version: Config.Defaults.Version,
        type: "settings-only",
        exportTime: new Date().toISOString(),
        config: await getRuntimeConfig(env)
      });
    },

    async importSettings(data, { env, ctx, kv, meta }) {
      const importedConfig = data?.config && typeof data.config === "object" && !Array.isArray(data.config)
        ? data.config
        : (data?.settings && typeof data.settings === "object" && !Array.isArray(data.settings) ? data.settings : null);
      if (!importedConfig) return jsonError("INVALID_SETTINGS_BACKUP", "设置备份文件无效，缺少 config/settings 对象");
      const savedConfig = await Database.persistRuntimeConfig(importedConfig, {
        env,
        kv,
        ctx,
        snapshotMeta: {
          reason: "import_settings",
          section: "all",
          source: String(meta?.source || "settings_backup"),
          actor: "admin"
        }
      });
      return jsonResponse({ success: true, config: savedConfig });
    },

    async getConfigSnapshots(data, { kv }) {
      return jsonResponse({ snapshots: await Database.getConfigSnapshots(kv) });
    },

    async clearConfigSnapshots(data, { kv }) {
      await Database.clearConfigSnapshots(kv);
      return jsonResponse({ success: true, snapshots: [] });
    },

    async restoreConfigSnapshot(data, { env, ctx, kv }) {
      const snapshotId = String(data?.id || "").trim();
      if (!snapshotId) return jsonError("SNAPSHOT_ID_REQUIRED", "请提供要恢复的快照 ID");
      const snapshot = await Database.getConfigSnapshotById(kv, snapshotId);
      if (!snapshot) return jsonError("SNAPSHOT_NOT_FOUND", "指定的配置快照不存在", 404);
      const savedConfig = await Database.persistRuntimeConfig(snapshot.config || {}, {
        env,
        kv,
        ctx,
        snapshotMeta: {
          reason: "restore_snapshot",
          section: "all",
          source: "snapshot",
          actor: "admin",
          note: snapshotId
        }
      });
      return jsonResponse({ success: true, config: savedConfig, restoredSnapshotId: snapshotId });
    },

    async list(data, { env, ctx }) {
      return new Response(JSON.stringify({ nodes: await CacheManager.getNodesList(env, ctx) }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    },

    async saveOrImport(data, { action, ctx, kv }) {
      const nodesToSave = action === "save" ? [data] : data.nodes;
      const savedNodes = [];
      let index = await Database.getNodesIndex(kv);
      
      for (const n of nodesToSave) {
        if (!n.name || (!n.target && !(Array.isArray(n.lines) && n.lines.length))) continue;
        const name = String(n.name).toLowerCase();
        const originalName = n.originalName ? String(n.originalName).toLowerCase() : null;
        const isRename = !!(originalName && originalName !== name);
        
        let existingNode = {};
        if (isRename) {
            existingNode = await kv.get(`${Database.PREFIX}${originalName}`, { type: "json" }) || {};
        } else {
            existingNode = await kv.get(`${Database.PREFIX}${name}`, { type: "json" }) || {};
        }
        const val = Database.buildNodeRecord(name, n, existingNode);
        if (!val) continue;
        
        await kv.put(`${Database.PREFIX}${name}`, JSON.stringify(val));
        if (isRename) {
          await kv.delete(`${Database.PREFIX}${originalName}`);
          Database.invalidateNodeCaches([originalName, name], { invalidateList: true });
          index = index.filter(x => x !== originalName);
        } else {
          Database.invalidateNodeCaches(name, { invalidateList: true });
        }
        savedNodes.push({ name, ...val });
        index.push(name);
      }
      
      if (savedNodes.length > 0) { 
        await Database.persistNodesIndex(index, { kv, ctx, invalidateList: true });
      }
      
      if (action === "save" && savedNodes.length === 0) return jsonError("INVALID_TARGET", "目标源站必须是有效的 http/https URL");
      return new Response(JSON.stringify({ success: true, node: action === "save" ? savedNodes[0] : undefined, nodes: action === "import" ? savedNodes : undefined }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    },

    async importFull(data, { env, ctx, kv }) {
      let savedConfig = null;
      if (data.config) {
        savedConfig = await Database.persistRuntimeConfig(data.config, {
          env,
          kv,
          ctx,
          snapshotMeta: {
            reason: "import_full",
            section: "all",
            source: "full_backup",
            actor: "admin"
          }
        });
      }
      if (data.nodes && Array.isArray(data.nodes)) {
          const savedNodes = [];
          let index = await Database.getNodesIndex(kv);
          for (const n of data.nodes) {
            if (!n.name || (!n.target && !(Array.isArray(n.lines) && n.lines.length))) continue;
            const name = String(n.name).toLowerCase(); 
            const existingNode = await kv.get(`${Database.PREFIX}${name}`, { type: "json" }) || {};
            const val = Database.buildNodeRecord(name, n, existingNode);
            if (!val) continue;
            
            await kv.put(`${Database.PREFIX}${name}`, JSON.stringify(val));
            Database.invalidateNodeCaches(name, { invalidateList: true });
            savedNodes.push(name);
            index.push(name);
          }
          if (savedNodes.length > 0) {
            await Database.persistNodesIndex(index, { kv, ctx, invalidateList: true });
          }
      }
      return jsonResponse({ success: true, config: savedConfig || await getRuntimeConfig(env) });
    },

    async delete(data, { ctx, kv }) {
      if (data.name) {
        const delName = String(data.name).toLowerCase(); 
        await kv.delete(`${Database.PREFIX}${delName}`); 
        Database.invalidateNodeCaches(delName, { invalidateList: true });
        const index = (await Database.getNodesIndex(kv)).filter(n => n !== delName);
        await Database.persistNodesIndex(index, { kv, ctx, invalidateList: true });
      }
      return new Response(JSON.stringify({ success: true }), { headers: { ...corsHeaders } });
    },

    async purgeCache(data, { kv }) {
        const config = await kv.get(Database.CONFIG_KEY, { type: "json" }) || {};
        if (!config.cfZoneId || !config.cfApiToken) return jsonError("CF_API_ERROR", "请在账号设置中完善 Zone ID 和 API 令牌");
        try {
            const res = await fetch(`https://api.cloudflare.com/client/v4/zones/${encodeURIComponent(String(config.cfZoneId).trim())}/purge_cache`, {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${config.cfApiToken}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ purge_everything: true })
            });
            if (res.ok) return jsonResponse({ success: true });
            return jsonError("PURGE_FAILED", "清理失败，请检查密钥权限");
        } catch(e) { return jsonError("PURGE_ERROR", e.message); }
    },

    async tidyKvData(data, { env, ctx, kv }) {
      if (!kv) return jsonError("KV_NOT_CONFIGURED", "请先绑定 ENI_KV / KV Namespace");
      try {
        const result = await Database.tidyKvData(env, { kv, ctx });
        const nowIso = new Date().toISOString();
        await Database.patchOpsStatus(env, {
          scheduled: {
            kvTidy: {
              status: "success",
              lastSuccessAt: nowIso,
              lastTriggeredBy: "manual",
              summary: result.summary
            }
          }
        }).catch(() => {});
        return jsonResponse({ success: true, ...result });
      } catch (error) {
        const message = error?.message || String(error);
        await Database.patchOpsStatus(env, {
          scheduled: {
            kvTidy: {
              status: "failed",
              lastErrorAt: new Date().toISOString(),
              lastError: message,
              lastTriggeredBy: "manual"
            }
          }
        }).catch(() => {});
        return jsonError("KV_TIDY_FAILED", message, 500);
      }
    },

    async listDnsRecords(data, { env, kv, request }) {
        const config = sanitizeRuntimeConfig(await getRuntimeConfig(env));
        const cfZoneId = String(config.cfZoneId || "").trim();
        const cfApiToken = String(config.cfApiToken || "").trim();
        if (!cfZoneId || !cfApiToken) return jsonError("CF_API_ERROR", "请在账号设置中完善 Zone ID 和 API 令牌");

        try {
            const zone = await fetchCloudflareZoneDetails(cfZoneId, cfApiToken).catch(() => null);
            const requestHost = normalizeHostnameText(new URL(request.url).hostname);
            const records = [];
            let page = 1;
            let totalPages = 1;
            const perPage = 100;
            do {
                const url = `https://api.cloudflare.com/client/v4/zones/${encodeURIComponent(cfZoneId)}/dns_records?page=${page}&per_page=${perPage}`;
                const payload = await fetchCloudflareApiJson(url, cfApiToken);
                if (Array.isArray(payload?.result)) records.push(...payload.result);
                totalPages = Number(payload?.result_info?.total_pages || payload?.result_info?.totalPages || 1);
                page += 1;
            } while (page <= totalPages && page <= 20);

            const normalized = records.map((r) => ({
                id: String(r?.id || ""),
                type: String(r?.type || ""),
                name: String(r?.name || ""),
                content: String(r?.content || ""),
                ttl: Number(r?.ttl) || 1,
                proxied: r?.proxied === true
            })).filter(r => r.id && r.name);

            const zoneName = String(zone?.name || "").trim() || "";
            const inferredZoneName = zoneName || normalizeHostnameText(records[0]?.name || "");
            let currentHost = requestHost;
            if (!isHostnameInsideZone(currentHost, inferredZoneName || zoneName)) {
              currentHost = normalizeHostnameText(await resolveCloudflareBoundHostname({
                cfAccountId: config.cfAccountId,
                cfZoneId,
                cfApiToken,
                zoneNameFallback: inferredZoneName || zoneName || requestHost
              }));
            }

            const filteredRecords = currentHost
              ? normalized.filter(record => normalizeHostnameText(record.name) === currentHost)
              : normalized;
            const recordsWithHistory = await Promise.all(filteredRecords.map(async (record) => ({
              ...record,
              history: await Database.getDnsRecordHistory(kv, cfZoneId, record.id)
            })));

            return jsonResponse({
                ok: true,
                zoneId: cfZoneId,
                zoneName,
                currentHost,
                totalRecords: normalized.length,
                filteredCount: recordsWithHistory.length,
                records: recordsWithHistory
            });
        } catch (e) {
            const msg = String(e?.message || e || "unknown_error");
            const hint = msg.includes("cf_api_http_403")
              ? "Cloudflare DNS 读取失败：API 令牌权限不足（需要 Zone.DNS:Read）"
              : msg.includes("cf_api_http_401")
                ? "Cloudflare DNS 读取失败：API 令牌无效"
                : "Cloudflare DNS 读取失败";
            return jsonError("CF_DNS_LIST_FAILED", hint, 400, { reason: msg });
        }
    },

    async updateDnsRecord(data, { env, kv, request }) {
        if (request.headers.get("X-Admin-Confirm") !== "updateDnsRecord") {
            return jsonError("CONFIRMATION_REQUIRED", "敏感 DNS 操作需要显式确认头", 428);
        }
        const recordId = String(data?.recordId || data?.id || "").trim();
        const nextType = String(data?.type || "").trim().toUpperCase();
        const nextContent = String(data?.content || "").trim();

        if (!recordId) return jsonError("MISSING_PARAMS", "recordId 不能为空");
        if (!["A", "AAAA", "CNAME"].includes(nextType)) return jsonError("INVALID_TYPE", "Type 仅允许 A / AAAA / CNAME");
        if (!nextContent) return jsonError("INVALID_CONTENT", "Content 不能为空");

        const config = sanitizeRuntimeConfig(await getRuntimeConfig(env));
        const cfZoneId = String(config.cfZoneId || "").trim();
        const cfApiToken = String(config.cfApiToken || "").trim();
        if (!cfZoneId || !cfApiToken) return jsonError("CF_API_ERROR", "请在账号设置中完善 Zone ID 和 API 令牌");

        const isAllowedRecordType = (value) => {
            const t = String(value || "").toUpperCase();
            return t === "A" || t === "AAAA" || t === "CNAME";
        };

        const isValidIpv4 = (value) => {
            const v = String(value || "").trim();
            const parts = v.split(".");
            if (parts.length !== 4) return false;
            for (const part of parts) {
                if (!/^[0-9]{1,3}$/.test(part)) return false;
                const num = Number(part);
                if (!Number.isFinite(num) || num < 0 || num > 255) return false;
            }
            return true;
        };

        const isValidIpv6 = (value) => {
            const v = String(value || "").trim();
            if (!v || !v.includes(":")) return false;
            if (/\s/.test(v)) return false;
            try {
                new URL(`http://[${v}]/`);
                return true;
            } catch {
                return false;
            }
        };

        if (nextType === "A" && !isValidIpv4(nextContent)) return jsonError("INVALID_CONTENT", "A 记录 Content 必须是合法 IPv4 地址");
        if (nextType === "AAAA" && !isValidIpv6(nextContent)) return jsonError("INVALID_CONTENT", "AAAA 记录 Content 必须是合法 IPv6 地址");
        if (nextType === "CNAME" && /\s/.test(nextContent)) return jsonError("INVALID_CONTENT", "CNAME 记录 Content 不能包含空格");

        try {
            const getUrl = `https://api.cloudflare.com/client/v4/zones/${encodeURIComponent(cfZoneId)}/dns_records/${encodeURIComponent(recordId)}`;
            const existingPayload = await fetchCloudflareApiJson(getUrl, cfApiToken);
            const existing = existingPayload?.result;
            if (!existing) return jsonError("NOT_FOUND", "DNS 记录不存在", 404);

            const currentType = String(existing?.type || "").toUpperCase();
            if (!isAllowedRecordType(currentType)) {
                return jsonError("UNSUPPORTED_RECORD_TYPE", "该 DNS 记录类型不支持编辑", 400, { currentType });
            }

            const updateBody = {
                type: nextType,
                name: String(existing?.name || ""),
                content: nextContent,
                ttl: Number(existing?.ttl) || 1,
                proxied: existing?.proxied === true
            };
            if (typeof existing?.comment === "string") updateBody.comment = existing.comment;
            if (Array.isArray(existing?.tags)) updateBody.tags = existing.tags.map(tag => String(tag));

            const updatePayload = await fetchCloudflareApiJson(getUrl, cfApiToken, {
                method: "PUT",
                body: JSON.stringify(updateBody)
            });

            const updated = updatePayload?.result || null;
            const record = updated
              ? {
                  id: String(updated?.id || recordId),
                  type: String(updated?.type || updateBody.type),
                  name: String(updated?.name || updateBody.name),
                  content: String(updated?.content || updateBody.content),
                  ttl: Number(updated?.ttl) || updateBody.ttl,
                  proxied: updated?.proxied === true
                }
              : { id: recordId, ...updateBody };
            const history = await Database.recordDnsRecordHistory(kv, cfZoneId, record.id, {
              name: record.name,
              type: record.type,
              content: record.content,
              actor: "admin",
              source: "ui",
              requestHost: normalizeHostnameText(new URL(request.url).hostname),
              savedAt: new Date().toISOString()
            });
            return jsonResponse({
                ok: true,
                record,
                history
            });
        } catch (e) {
            const msg = String(e?.message || e || "unknown_error");
            const hint = msg.includes("cf_api_http_403")
              ? "Cloudflare DNS 更新失败：API 令牌权限不足（需要 Zone.DNS:Edit）"
              : msg.includes("cf_api_http_401")
                ? "Cloudflare DNS 更新失败：API 令牌无效"
                : "Cloudflare DNS 更新失败";
            return jsonError("CF_DNS_UPDATE_FAILED", hint, 400, { reason: msg });
        }
    },

    async testTelegram(data) {
        const { tgBotToken, tgChatId } = data;
        if (!tgBotToken || !tgChatId) return jsonError("MISSING_PARAMS", "请先填写 Bot Token 和 Chat ID");
        try {
            const msgText = "✅ Emby Proxy: Telegram 机器人测试通知成功！\n如果您能看到这条消息，说明您的通知配置完全正确。";
            await Database.sendTelegramMessage({ tgBotToken, tgChatId, text: msgText });
            return jsonResponse({ success: true });
        } catch (e) {
            return jsonError("NETWORK_ERROR", e.message);
        }
    },

    async sendDailyReport(data, { env }) {
        try {
            await Database.sendDailyTelegramReport(env);
            return new Response(JSON.stringify({ success: true }), { headers: { ...corsHeaders } });
        } catch (e) {
            return jsonError("REPORT_FAILED", e.message);
        }
    },

    async pingNode(data, { env, ctx }) {
        const currentConfig = await getRuntimeConfig(env);
        const timeoutMs = clampIntegerConfig(data.timeout, currentConfig.pingTimeout ?? Config.Defaults.PingTimeoutMs, 1000, 180000);
        const forceRefresh = data.forceRefresh === true;

        if (data.target) {
          const normalizedTarget = Database.normalizeSingleTarget(data.target);
          if (!normalizedTarget) return jsonError("INVALID_TARGET", "目标源站必须是有效的 http/https URL");
          const ms = await Database.pingTarget(normalizedTarget, timeoutMs);
          return jsonResponse({ ms, target: normalizedTarget, usedCache: false, scope: "target" });
        }

        const nodeName = String(data.name || "").trim();
        const node = await Database.getNode(nodeName, env, ctx);
        if (!node || !Array.isArray(node.lines) || !node.lines.length) return jsonError("NOT_FOUND", "节点不存在");

        const cacheMinutes = clampIntegerConfig(currentConfig.pingCacheMinutes, Config.Defaults.PingCacheMinutes, 0, 1440);
        const requestedLineId = String(data.lineId || "").trim();
        const silent = data.silent === true && !!requestedLineId;
        const linesToProbe = requestedLineId
          ? node.lines.filter(line => line.id === requestedLineId)
          : node.lines.slice();
        if (requestedLineId && !linesToProbe.length) return jsonError("LINE_NOT_FOUND", "线路不存在", 404);

        const probedLines = await Promise.all(linesToProbe.map(async (line) => {
          const useCache = !forceRefresh && Database.isPingCacheFresh(line, cacheMinutes);
          if (useCache) return { ...line, usedCache: true };
          const ms = await Database.pingTarget(line.target, timeoutMs);
          return {
            ...line,
            latencyMs: ms,
            latencyUpdatedAt: new Date().toISOString(),
            usedCache: false
          };
        }));

        let allUsedCache = probedLines.length > 0 && probedLines.every(line => line.usedCache === true);
        let nextLines = node.lines.map(line => {
          const updated = probedLines.find(item => item.id === line.id);
          return updated
            ? {
                id: updated.id,
                name: updated.name,
                target: updated.target,
                latencyMs: updated.latencyMs,
                latencyUpdatedAt: updated.latencyUpdatedAt
              }
            : line;
        });
        let nextActiveLineId = Database.resolveActiveLineId(node.activeLineId, nextLines, nextLines);

        if (!silent) {
          nextLines = Database.sortNodeLinesByLatency(nextLines);
          nextActiveLineId = nextLines[0]?.id || nextActiveLineId;
        }

        const normalizedNode = Database.normalizeNode(nodeName, {
          ...node,
          lines: nextLines,
          activeLineId: nextActiveLineId,
          updatedAt: new Date().toISOString()
        }).data;

        const kv = Database.getKV(env);
        if (kv) {
          await kv.put(`${Database.PREFIX}${nodeName.toLowerCase()}`, JSON.stringify(normalizedNode));
          Database.invalidateNodeCaches(nodeName, { invalidateList: true });
          setBoundedMapEntry(GLOBALS.NodeCache, nodeName.toLowerCase(), { data: normalizedNode, exp: nowMs() + Config.Defaults.CacheTTL }, Config.Defaults.NodeCacheMax);
        }

        const activeLine = Database.getActiveNodeLine(normalizedNode);
        const matchedLine = requestedLineId
          ? normalizedNode.lines.find(line => line.id === requestedLineId)
          : activeLine;
        return jsonResponse({
          ms: Number(matchedLine?.latencyMs ?? activeLine?.latencyMs ?? 9999),
          usedCache: allUsedCache,
          sorted: !silent,
          activeLineId: normalizedNode.activeLineId,
          activeLineName: activeLine?.name || "",
          line: matchedLine || null,
          node: { name: nodeName.toLowerCase(), ...normalizedNode }
        });
    },

    async getLogs(data, { db }) {
      if (!db) return new Response(JSON.stringify({ error: "D1 not configured" }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
      const { page = 1, pageSize = 50, filters = {} } = data;
      const safePage = Math.max(1, parseInt(page, 10) || 1);
      const safePageSize = Math.min(200, Math.max(1, parseInt(pageSize, 10) || 50));
      const offset = (safePage - 1) * safePageSize;
      const now = Date.now();
      const defaultWindowMs = Config.Defaults.LogQueryDefaultDays * 24 * 60 * 60 * 1000;
      const parseStartDate = (value) => {
        if (!value) return null;
        const ts = new Date(String(value)).getTime();
        return Number.isFinite(ts) ? ts : null;
      };
      const parseEndDate = (value) => {
        if (!value) return null;
        const ts = new Date(String(value) + "T23:59:59.999").getTime();
        return Number.isFinite(ts) ? ts : null;
      };

      let startTs = parseStartDate(filters.startDate);
      let endTs = parseEndDate(filters.endDate);
      if (!Number.isFinite(endTs)) endTs = now;
      if (!Number.isFinite(startTs)) startTs = Math.max(0, endTs - defaultWindowMs);
      if (startTs > endTs) [startTs, endTs] = [Math.max(0, endTs - defaultWindowMs), endTs];

      const whereClause = ["timestamp >= ?", "timestamp <= ?"];
      /** @type {(number | string)[]} */
      const params = [startTs, endTs];
      const keyword = String(filters.keyword || "").trim();
      if (keyword) {
        const maxKeywordWindowMs = Config.Defaults.LogKeywordMaxWindowDays * 24 * 60 * 60 * 1000;
        if ((endTs - startTs) > maxKeywordWindowMs) {
          return jsonError("LOG_QUERY_RANGE_TOO_WIDE", `关键词搜索必须限制在 ${Config.Defaults.LogKeywordMaxWindowDays} 天内`, 400, {
            maxWindowDays: Config.Defaults.LogKeywordMaxWindowDays
          });
        }
        if (/^\d{3}$/.test(keyword)) {
          whereClause.push("status_code = ?");
          params.push(Number(keyword));
        } else if (isLikelyIpAddress(keyword)) {
          whereClause.push("client_ip = ?");
          params.push(keyword);
        } else {
          const likeKeyword = `%${escapeSqlLike(keyword)}%`;
          whereClause.push("(node_name LIKE ? ESCAPE '\\' OR request_path LIKE ? ESCAPE '\\' OR user_agent LIKE ? ESCAPE '\\' OR error_detail LIKE ? ESCAPE '\\')");
          params.push(likeKeyword, likeKeyword, likeKeyword, likeKeyword);
        }
      }
      if (filters.category) {
        whereClause.push("category = ?");
        params.push(String(filters.category));
      }
      if (filters.playbackMode) {
        whereClause.push("error_detail LIKE ? ESCAPE '\\'");
        params.push(`%${escapeSqlLike(`Playback=${String(filters.playbackMode)}`)}%`);
      }

      const where = "WHERE " + whereClause.join(" AND ");
      const total = (await db.prepare(`SELECT COUNT(*) as total FROM proxy_logs ${where}`).bind(...params).first())?.total || 0;
      const logsResult = await db.prepare(`SELECT * FROM proxy_logs ${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`).bind(...params, safePageSize, offset).all();
      
      return new Response(JSON.stringify({
        logs: logsResult.results || [],
        total,
        page: safePage,
        pageSize: safePageSize,
        totalPages: Math.ceil(total / safePageSize),
        range: {
          startDate: new Date(startTs).toISOString(),
          endDate: new Date(endTs).toISOString()
        }
      }), { headers: { ...corsHeaders } });
    },

    async clearLogs(data, { db }) {
      if (!db) return new Response(JSON.stringify({ error: "D1 not configured" }), { status: 500, headers: { ...corsHeaders } });
      await db.prepare("DELETE FROM proxy_logs").run();
      let vacuumed = false;
      try {
        vacuumed = await this.vacuumLogsDb(db);
      } catch (error) {
        console.warn("clearLogs VACUUM failed", error);
      }
      return new Response(JSON.stringify({ success: true, vacuumed }), { headers: { ...corsHeaders } });
    },

    async initLogsDb(data, { db }) {
      if (!db) return new Response(JSON.stringify({ error: "D1 not configured" }), { status: 500, headers: { ...corsHeaders } });
      await db.prepare(`CREATE TABLE IF NOT EXISTS proxy_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp INTEGER NOT NULL, node_name TEXT NOT NULL, request_path TEXT NOT NULL, request_method TEXT NOT NULL, status_code INTEGER NOT NULL, response_time INTEGER NOT NULL, client_ip TEXT NOT NULL, user_agent TEXT, referer TEXT, category TEXT DEFAULT 'api', error_detail TEXT, created_at TEXT NOT NULL)`).run();
      let existingColumns = new Set();
      try {
        const schemaRows = await db.prepare(`PRAGMA table_info(proxy_logs)`).all();
        existingColumns = new Set((schemaRows?.results || []).map(row => String(row?.name || "").toLowerCase()).filter(Boolean));
      } catch {}
      if (!existingColumns.has("category")) {
        await db.prepare(`ALTER TABLE proxy_logs ADD COLUMN category TEXT DEFAULT 'api'`).run();
        existingColumns.add("category");
      }
      if (!existingColumns.has("error_detail")) {
        await db.prepare(`ALTER TABLE proxy_logs ADD COLUMN error_detail TEXT`).run();
      }
      
      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_proxy_logs_timestamp ON proxy_logs (timestamp)`).run();
      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_proxy_logs_client_ip ON proxy_logs (client_ip)`).run();
      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_proxy_logs_node_time ON proxy_logs (node_name, timestamp)`).run();
      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_proxy_logs_category ON proxy_logs (category)`).run();
      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_proxy_logs_status_time ON proxy_logs (status_code, timestamp)`).run();
      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_proxy_logs_category_time ON proxy_logs (category, timestamp)`).run();
      await this.ensureSysStatusTable(db);
      
      return new Response(JSON.stringify({ success: true, schemaVersion: 2, categoryEnabled: true }), { headers: { ...corsHeaders } });
    }
  },

  // ============================================================================
  // 重构后的 handleApi 主函数：极简派发器
  // 边界说明：
  // 1. 这里只做四件事：鉴别 KV、解析 JSON、归一 action、构造上下文后派发。
  // 2. 这里不承载业务判断，业务复杂度应留在 ApiHandlers 的具体动作中。
  // 3. 当需要新增管理功能时，优先保证这里继续保持“薄派发层”。
  // ============================================================================
  async handleApi(request, env, ctx) {
    const kv = this.getKV(env);
    if (!kv) {
        return new Response(JSON.stringify({ error: "kv_missing" }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    let data; 
    try { 
        data = await request.json(); 
    } catch { 
        return jsonError("INVALID_JSON", "请求 JSON 无效", 400); 
    }

    const normalizedRequest = this.normalizeAdminActionRequest(data);
    if (!normalizedRequest) {
        return jsonError("INVALID_REQUEST", "请求体必须是 JSON 对象", 400);
    }

    const actionName = (normalizedRequest.action === "save" || normalizedRequest.action === "import") ? "saveOrImport" : normalizedRequest.action;
    const handler = this.ApiHandlers[actionName];

    if (!handler) {
        return jsonError("INVALID_ACTION", "未知的管理动作", 400, { action: normalizedRequest.action || null });
    }

    const context = {
        action: normalizedRequest.action,
        meta: normalizedRequest.meta,
        request,
        env,
        ctx,
        kv,
        db: this.getDB(env)
    };

    return await handler.call(this, normalizedRequest.data, context);
  }
};

// ============================================================================
// 3. 代理模块 (PROXY MODULE - 核心缓冲防护与 CORS 重构)
// ============================================================================
function normalizeEmbyAuthHeaders(headers, method = "GET", path = "") {
  // 1. 安全提取并清洗现有 Header
  const embyAuth = headers.get("X-Emby-Authorization")?.trim();
  const stdAuth = headers.get("Authorization")?.trim();
  const isEmbyStd = stdAuth?.toLowerCase().startsWith("emby ");

  // 2. 确立单一真相源 (Source of Truth)
  // 优先级: X-Emby-Auth > 符合规范的 Std Auth > null
  let finalAuth = embyAuth || (isEmbyStd ? stdAuth : null);

  // 3. 登录 API 强制补头兜底
  if (!finalAuth && method.toUpperCase() === "POST" && path.toLowerCase().includes("/users/authenticatebyname")) {
      finalAuth = 'Emby Client="Emby Proxy Patch", Device="Browser", DeviceId="proxy-login-patch", Version="1.0.0"';
  }

  // 4. 双向同步 (解决冲突与缺失)
  if (finalAuth) {
      headers.set("X-Emby-Authorization", finalAuth);
      
      // 仅在 Authorization 为空，或确认其也是 Emby 格式时才覆盖
      // 绝对不覆盖正常的 Bearer/Basic 认证头
      if (!stdAuth || isEmbyStd) {
          headers.set("Authorization", finalAuth);
      }
  }
  
  return headers;
}
const Proxy = {
  // Proxy 模块阅读顺序建议：
  // 1. resolve/evaluate/classify：环境裁决与请求分类
  // 2. build*：请求状态、响应头、跳转头整形
  // 3. perform/fetch*：上游访问与重试循环
  // 4. handle：把上述阶段串成完整代理链路
  resolveCorsOrigin(currentConfig, request) {
    const reqOrigin = request.headers.get("Origin");
    const allowedOrigins = String(currentConfig.corsOrigins || "").split(",").map(i => i.trim()).filter(Boolean);
    if (allowedOrigins.length > 0) return reqOrigin && allowedOrigins.includes(reqOrigin) ? reqOrigin : allowedOrigins[0];
    return reqOrigin || "*";
  },
  buildEdgeResponseHeaders(finalOrigin, extra = {}) {
    const headers = new Headers({ "Access-Control-Allow-Origin": finalOrigin, "Cache-Control": "no-store", ...extra });
    applySecurityHeaders(headers);
    return headers;
  },
  classifyRequest(request, proxyPath, requestUrl, currentConfig, options = {}) {
    const rangeHeader = request.headers.get("Range");
    const isImage = GLOBALS.Regex.EmbyImages.test(proxyPath) || GLOBALS.Regex.ImageExt.test(proxyPath);
    const isStaticFile = GLOBALS.Regex.StaticExt.test(proxyPath);
    const isSubtitle = GLOBALS.Regex.SubtitleExt.test(proxyPath);
    const isManifest = GLOBALS.Regex.ManifestExt.test(proxyPath);
    const isSegment = GLOBALS.Regex.SegmentExt.test(proxyPath);
    const isWsUpgrade = request.headers.get("Upgrade")?.toLowerCase() === "websocket";
    const looksLikeVideoRoute = GLOBALS.Regex.Streaming.test(proxyPath) || /\/videos\/[^/]+\/(stream|original|download|file)/i.test(proxyPath) || /\/items\/[^/]+\/download/i.test(proxyPath) || requestUrl.searchParams.get("Static") === "true" || requestUrl.searchParams.get("Download") === "true";
    const isSafeMethod = request.method === "GET" || request.method === "HEAD";
    const directStaticAssets = options.directStaticAssets === true && isSafeMethod && isStaticFile;
    // WebVTT 字幕轨继续走 Worker 缓存：307 直连会额外多一次跳转，双语字幕场景通常更容易比代理缓存更慢。
    const directHlsDash = options.directHlsDash === true && isSafeMethod && (isManifest || isSegment);
    const direct307Mode = options.nodeDirectSource === true || directStaticAssets || directHlsDash;
    const enablePrewarm = currentConfig.enablePrewarm !== false && !direct307Mode;
    const prewarmCacheTtl = clampIntegerConfig(currentConfig.prewarmCacheTtl, Config.Defaults.PrewarmCacheTtl, 0, 3600);
    const prewarmDepth = normalizePrewarmDepth(currentConfig.prewarmDepth);
    const isBigStream = looksLikeVideoRoute && !isManifest && !isSegment && !isSubtitle && !isImage;
    const isMetadataCacheable = request.method === "GET" && !isWsUpgrade && !direct307Mode && (isImage || isSubtitle || isManifest);
    const isCacheableAsset = request.method === "GET" && !isWsUpgrade && (isImage || isStaticFile || isSubtitle || isSegment || isManifest);
    return {
      rangeHeader,
      enablePrewarm,
      prewarmCacheTtl,
      prewarmDepth,
      isImage,
      isStaticFile,
      isSubtitle,
      isManifest,
      isSegment,
      isWsUpgrade,
      looksLikeVideoRoute,
      isBigStream,
      isMetadataCacheable,
      isCacheableAsset,
      directStaticAssets,
      directHlsDash,
      direct307Mode
    };
  },
  evaluateFirewall(currentConfig, clientIp, country, finalOrigin) {
    const ipBlacklist = String(currentConfig.ipBlacklist || "").split(",").map(i => i.trim()).filter(Boolean);
    if (ipBlacklist.includes(clientIp)) {
      return new Response("Forbidden by IP Firewall", { status: 403, headers: this.buildEdgeResponseHeaders(finalOrigin) });
    }

    const geoAllow = String(currentConfig.geoAllowlist || "").split(",").map(i => i.trim().toUpperCase()).filter(Boolean);
    const geoBlock = String(currentConfig.geoBlocklist || "").split(",").map(i => i.trim().toUpperCase()).filter(Boolean);
    if ((geoAllow.length > 0 && !geoAllow.includes(country)) || (geoBlock.length > 0 && geoBlock.includes(country))) {
      return new Response("Forbidden by Geo Firewall", { status: 403, headers: this.buildEdgeResponseHeaders(finalOrigin) });
    }

    return null;
  },
  applyRateLimit(currentConfig, clientIp, requestTraits, startTime, finalOrigin) {
    const rpmLimit = parseInt(currentConfig.rateLimitRpm) || 0;
    const shouldRateLimit = rpmLimit > 0 && !(requestTraits.isManifest || requestTraits.isSegment || requestTraits.isBigStream);
    if (!shouldRateLimit) return null;
    let rlData = GLOBALS.RateLimitCache.get(clientIp);
    if (!rlData || startTime > rlData.resetAt) rlData = { count: 0, resetAt: startTime + 60000 };
    rlData.count += 1;
    GLOBALS.RateLimitCache.set(clientIp, rlData);
    if (rlData.count > rpmLimit) {
      return new Response("Rate Limit Exceeded", { status: 429, headers: this.buildEdgeResponseHeaders(finalOrigin) });
    }
    return null;
  },
  parseTargetBases(node, finalOrigin) {
    const orderedLines = Database.getOrderedNodeLines(node);
    const rawTargets = orderedLines.length
      ? orderedLines.map(line => line.target)
      : String(node.target || "").split(",").map(item => item.trim()).filter(Boolean);
    const targetBases = rawTargets.map(item => {
      try { return new URL(item); } catch { return null; }
    }).filter(url => url && ["http:", "https:"].includes(url.protocol));
    if (!targetBases.length) {
      return { targetBases, invalidResponse: new Response("Invalid Node Target", { status: 502, headers: this.buildEdgeResponseHeaders(finalOrigin) }) };
    }
    return { targetBases, invalidResponse: null };
  },
  async buildProxyRequestState(request, node, proxyPath, requestUrl, clientIp, requestTraits, forceH1, targetBases) {
    const newHeaders = new Headers(request.headers);
    GLOBALS.DropRequestHeaders.forEach(h => newHeaders.delete(h));

    const adminCustomHeaders = new Set();
    let adminCustomCookie = null;
    if (node.headers && typeof node.headers === "object") {
      for (const [hKey, hVal] of Object.entries(node.headers)) {
        const lowerKey = String(hKey).toLowerCase();
        if (GLOBALS.DropRequestHeaders.has(lowerKey)) continue;
        adminCustomHeaders.add(lowerKey);
        if (lowerKey === "cookie") adminCustomCookie = String(hVal);
        else newHeaders.set(hKey, String(hVal));
      }
    }

    const mergedCookie = mergeAndSanitizeCookieHeaders(newHeaders.get("Cookie"), adminCustomCookie, ["auth_token"]);
    if (mergedCookie) newHeaders.set("Cookie", mergedCookie);
    else newHeaders.delete("Cookie");

    normalizeEmbyAuthHeaders(newHeaders, request.method, proxyPath);

    newHeaders.set("X-Real-IP", clientIp);
    newHeaders.set("X-Forwarded-For", clientIp);
    newHeaders.set("X-Forwarded-Host", requestUrl.host);
    newHeaders.set("X-Forwarded-Proto", requestUrl.protocol.replace(":", ""));
    if (requestTraits.isWsUpgrade) {
      newHeaders.set("Upgrade", "websocket");
      newHeaders.set("Connection", "Upgrade");
    } else if (forceH1) {
      newHeaders.set("Connection", "keep-alive");
    }
    if ((requestTraits.isBigStream || requestTraits.isSegment || requestTraits.isManifest) && !adminCustomHeaders.has("referer")) {
      newHeaders.delete("Referer");
    }

    const isNonIdempotent = request.method !== "GET" && request.method !== "HEAD";
    let preparedBody = null;
    let preparedBodyMode = "none";
    if (isNonIdempotent && request.body) {
      const contentLength = parseContentLengthHeader(request.headers.get("Content-Length"));
      const canBufferRetryBody = Number.isFinite(contentLength) && contentLength >= 0 && contentLength <= Config.Defaults.BufferedRetryBodyMaxBytes;
      if (canBufferRetryBody) {
        try {
          preparedBody = await request.clone().arrayBuffer();
          preparedBodyMode = "buffered";
        } catch {
          preparedBody = request.body;
          preparedBodyMode = "stream";
        }
      } else {
        preparedBody = request.body;
        preparedBodyMode = "stream";
      }
    }
    const retryTargets = isNonIdempotent ? targetBases.slice(0, 1) : targetBases;
    const allowAutomaticRetry = !isNonIdempotent;

    return {
      newHeaders,
      adminCustomHeaders,
      preparedBody,
      preparedBodyMode,
      retryTargets,
      allowAutomaticRetry
    };
  },
  evaluateRedirectDecision(nextUrl, activeTargetBase, redirectMethod, redirectBodyMode, policy) {
    const isSameOriginRedirect = nextUrl.origin === activeTargetBase.origin;
    const mustDirect = isSameOriginRedirect
      ? !policy.sourceSameOriginProxy
      : (!policy.forceExternalProxy || shouldDirectByWangpan(nextUrl, policy.wangpanDirectKeywords));
    if (mustDirect) {
      return { mustDirect: true, nextMethod: null, nextBodyMode: redirectBodyMode, isSameOriginRedirect };
    }
    const nextMethod = normalizeRedirectMethod(policy.currentStatus, redirectMethod);
    let nextBodyMode = redirectBodyMode;
    if (nextMethod === "GET" || nextMethod === "HEAD") nextBodyMode = "none";
    else if (redirectBodyMode === "stream") {
      return { mustDirect: true, nextMethod, nextBodyMode: redirectBodyMode, isSameOriginRedirect };
    }
    return { mustDirect: false, nextMethod, nextBodyMode, isSameOriginRedirect };
  },
  buildProxyResponseHeaders(response, request, dynamicCors, finalOrigin, requestTraits, options = {}) {
    const modifiedHeaders = new Headers(response.headers);

    if (GLOBALS.DropResponseHeaders) {
      GLOBALS.DropResponseHeaders.forEach(h => modifiedHeaders.delete(h));
    }

    modifiedHeaders.set("Access-Control-Allow-Origin", finalOrigin);

    if (dynamicCors && dynamicCors["Access-Control-Expose-Headers"]) {
      modifiedHeaders.set("Access-Control-Expose-Headers", dynamicCors["Access-Control-Expose-Headers"]);
    }

    if (dynamicCors && dynamicCors["Access-Control-Allow-Methods"]) {
      modifiedHeaders.set("Access-Control-Allow-Methods", dynamicCors["Access-Control-Allow-Methods"]);
    }

    const resReqHeaders = request.headers.get("Access-Control-Request-Headers");
    if (resReqHeaders) {
      modifiedHeaders.set("Access-Control-Allow-Headers", resReqHeaders);
      mergeVaryHeader(modifiedHeaders, "Access-Control-Request-Headers");
    } else if (dynamicCors && dynamicCors["Access-Control-Allow-Headers"]) {
      modifiedHeaders.set("Access-Control-Allow-Headers", dynamicCors["Access-Control-Allow-Headers"]);
    }

    if (finalOrigin !== "*") {
      mergeVaryHeader(modifiedHeaders, "Origin");
    }

    if (!options.enableH3 || options.forceH1) {
      modifiedHeaders.delete("Alt-Svc");
    }

    const imageCacheMaxAge = clampIntegerConfig(options.imageCacheMaxAge, Config.Defaults.CacheTtlImagesDays * 86400, 0, 365 * 86400);
    if (response.status >= 400 || requestTraits.isManifest) {
      modifiedHeaders.set("Cache-Control", "no-store");
    } else if (requestTraits.isImage) {
      modifiedHeaders.set("Cache-Control", `public, max-age=${imageCacheMaxAge}`);
    } else if (requestTraits.isStaticFile || requestTraits.isSubtitle) {
      modifiedHeaders.set("Cache-Control", "public, max-age=86400");
    } else if (options.proxiedExternalRedirect) {
      modifiedHeaders.set("Cache-Control", "no-store");
    }

    applySecurityHeaders(modifiedHeaders);
    return modifiedHeaders;
  },
  applyProxyRedirectHeaders(modifiedHeaders, response, activeTargetBase, name, key, directRedirectUrl, responseUrl) {
    if (directRedirectUrl) {
      sanitizeSyntheticRedirectHeaders(modifiedHeaders);
      modifiedHeaders.set("Location", directRedirectUrl.toString());
      modifiedHeaders.set("Cache-Control", "no-store");
      return;
    }
    if (!(response.status >= 300 && response.status < 400)) return;
    const location = modifiedHeaders.get("Location");
    if (!location) return;
    const resolvedLocation = resolveRedirectTarget(location, responseUrl || activeTargetBase);
    const rewrittenLocation = translateUpstreamUrlToProxyLocation(resolvedLocation, activeTargetBase, name, key);
    if (rewrittenLocation) modifiedHeaders.set("Location", rewrittenLocation);
  },
  classifyProxyLogCategory(requestTraits) {
    if (requestTraits.isSegment) return "segment";
    if (requestTraits.isManifest) return "manifest";
    if (requestTraits.isBigStream) return "stream";
    if (requestTraits.isImage) return "image";
    if (requestTraits.isSubtitle) return "subtitle";
    if (requestTraits.isStaticFile) return "asset";
    if (requestTraits.isWsUpgrade) return "websocket";
    return "api";
  },
  isPlaybackInfoRequest(proxyPath) {
    return /\/playbackinfo\b/i.test(String(proxyPath || ""));
  },
  async extractPlaybackInfoDiagnostic(proxyPath, requestUrl, response) {
    if (!this.isPlaybackInfoRequest(proxyPath)) return null;
    if (!(response.status >= 200 && response.status < 300)) return null;
    const contentType = String(response.headers.get("Content-Type") || "").toLowerCase();
    if (!contentType.includes("json")) return null;
    try {
      const payload = await response.clone().json();
      const mediaSource = Array.isArray(payload?.MediaSources) ? payload.MediaSources[0] : null;
      if (!mediaSource || typeof mediaSource !== "object") return null;
      const transcodeUrl = String(mediaSource.TranscodingUrl || "");
      const supportsDirectPlay = mediaSource.SupportsDirectPlay === true;
      const supportsDirectStream = mediaSource.SupportsDirectStream === true;
      const mode = transcodeUrl
        ? "transcode"
        : supportsDirectPlay
          ? "direct_play"
          : supportsDirectStream
            ? "direct_stream"
            : "unknown";
      const hints = [`Playback=${mode}`];
      const subtitleStreamIndex = requestUrl.searchParams.get("SubtitleStreamIndex");
      if (subtitleStreamIndex !== null && subtitleStreamIndex !== "") hints.push(`ReqSubtitle=${subtitleStreamIndex}`);
      const subtitleMethod = requestUrl.searchParams.get("SubtitleMethod");
      if (subtitleMethod) hints.push(`SubtitleMethod=${subtitleMethod}`);
      const subtitleStreams = Array.isArray(mediaSource.MediaStreams)
        ? mediaSource.MediaStreams.filter(stream => String(stream?.Type || "").toLowerCase() === "subtitle")
        : [];
      if (subtitleStreams.length > 0) hints.push(`SubtitleTracks=${subtitleStreams.length}`);
      if (subtitleStreams.some(stream => stream?.IsExternal === true)) hints.push("ExternalSubtitle=yes");
      if (transcodeUrl) {
        if (/subtitle/i.test(transcodeUrl)) hints.push("SubtitleInTranscode=yes");
        if (/burn/i.test(transcodeUrl)) hints.push("SubtitleBurn=yes");
      }
      return hints.join(" | ");
    } catch {
      return null;
    }
  },
  extractProxyErrorDetail(response) {
    if (response.status < 400) return null;
    const hints = [];
    const srv = response.headers.get("Server");
    if (srv) hints.push(`Server: ${srv}`);
    const ray = response.headers.get("CF-Ray");
    if (ray) hints.push(`CF-Ray: ${ray}`);
    const embyErr = response.headers.get("X-Application-Error-Code") || response.headers.get("X-Emby-Error");
    if (embyErr) hints.push(`Emby-Error: ${embyErr}`);
    const cfCache = response.headers.get("CF-Cache-Status");
    if (cfCache) hints.push(`CF-Cache: ${cfCache}`);
    return hints.length > 0 ? hints.join(" | ") : response.statusText;
  },
  buildMetadataCacheStorageResponse(response, requestTraits, options = {}) {
    const cacheHeaders = new Headers(response.headers);
    cacheHeaders.delete("Set-Cookie");
    if (requestTraits.isImage) {
      cacheHeaders.set("Cache-Control", `public, max-age=${Math.max(0, Number(options.imageCacheMaxAge) || 0)}`);
    } else if (requestTraits.isSubtitle) {
      cacheHeaders.set("Cache-Control", "public, max-age=86400");
    } else if (requestTraits.isManifest) {
      cacheHeaders.set("Cache-Control", `public, max-age=${Math.max(0, Number(options.prewarmCacheTtl) || 0)}`);
    }
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: cacheHeaders
    });
  },
  async storeMetadataCache(cacheKey, response, requestTraits, options = {}) {
    const cache = getDefaultCacheHandle();
    if (!cache || !cacheKey || !response || response.status !== 200) return false;
    if (requestTraits.isManifest && !shouldWorkerCacheMetadataUrl(options.sourceUrl)) return false;
    try {
      await cache.put(cacheKey, this.buildMetadataCacheStorageResponse(response, requestTraits, options));
      return true;
    } catch {
      return false;
    }
  },
  resolveMetadataTarget(candidate, activeTargetBase, name, key) {
    const raw = String(candidate || "").trim();
    if (!raw) return null;
    let upstreamUrl;
    try {
      if (/^https?:\/\//i.test(raw)) {
        upstreamUrl = new URL(raw);
      } else {
        const relativeUrl = new URL(raw, "https://metadata-prewarm.invalid");
        upstreamUrl = buildUpstreamProxyUrl(activeTargetBase, relativeUrl.pathname || "/");
        upstreamUrl.search = relativeUrl.search || "";
        upstreamUrl.hash = relativeUrl.hash || "";
      }
    } catch {
      return null;
    }
    if (isHeavyVideoBytePath(upstreamUrl.pathname)) return null;
    const proxyLocation = translateUpstreamUrlToProxyLocation(upstreamUrl, activeTargetBase, name, key);
    if (!proxyLocation) return null;
    let proxyUrl;
    try {
      proxyUrl = new URL(proxyLocation, "https://worker.invalid");
    } catch {
      return null;
    }
    const pathname = proxyUrl.pathname || "/";
    if (!(GLOBALS.Regex.EmbyImages.test(pathname) || GLOBALS.Regex.ImageExt.test(pathname) || GLOBALS.Regex.ManifestExt.test(pathname) || GLOBALS.Regex.SubtitleExt.test(pathname))) {
      return null;
    }
    return { upstreamUrl, proxyPath: pathname, proxySearch: proxyUrl.search || "" };
  },
  buildMetadataPrewarmTargets(proxyPath, payload, activeTargetBase, name, key, prewarmDepth) {
    const candidates = new Map();
    const itemId = extractProxyItemId(proxyPath);
    if (itemId) {
      const posterTarget = this.resolveMetadataTarget(`/Items/${encodeURIComponent(itemId)}/Images/Primary`, activeTargetBase, name, key);
      if (posterTarget) candidates.set(`${posterTarget.proxyPath}${posterTarget.proxySearch}`, posterTarget);
    }
    if (prewarmDepth !== "poster") {
      collectMetadataUrlStrings(payload).forEach(value => {
        const target = this.resolveMetadataTarget(value, activeTargetBase, name, key);
        if (!target) return;
        candidates.set(`${target.proxyPath}${target.proxySearch}`, target);
      });
    }
    return [...candidates.values()]
      .sort((a, b) => rankMetadataWarmPath(a.proxyPath) - rankMetadataWarmPath(b.proxyPath))
      .slice(0, 4);
  },
  async maybePrewarmMetadataResponse(request, response, requestTraits, activeTargetBase, buildFetchOptions, name, key, requestUrl, ctx, options = {}) {
    if (!ctx || request.method !== "GET" || requestTraits.enablePrewarm !== true) return;
    if (requestTraits.isImage || requestTraits.isSubtitle || requestTraits.isManifest || requestTraits.isSegment || requestTraits.isBigStream) return;
    if (!(response.status >= 200 && response.status < 300)) return;
    const contentType = String(response.headers.get("Content-Type") || "").toLowerCase();
    if (!contentType.includes("json")) return;
    let payload;
    try {
      payload = await response.clone().json();
    } catch {
      return;
    }
    const targets = this.buildMetadataPrewarmTargets(options.proxyPath, payload, activeTargetBase, name, key, requestTraits.prewarmDepth);
    if (!targets.length) return;
    ctx.waitUntil((async () => {
      const cache = getDefaultCacheHandle();
      for (const target of targets) {
        if (!shouldWorkerCacheMetadataUrl(target.upstreamUrl)) continue;
        const proxyUrl = new URL(`${target.proxyPath}${target.proxySearch}`, requestUrl.origin);
        const cacheKey = buildWorkerCacheKey(proxyUrl);
        if (cache && cacheKey) {
          try {
            const existing = await cache.match(cacheKey);
            if (existing) continue;
          } catch {}
        }
        try {
          const prewarmOptions = await buildFetchOptions(target.upstreamUrl, { method: "GET" });
          const prewarmHeaders = new Headers(prewarmOptions.headers);
          prewarmHeaders.delete("Range");
          prewarmHeaders.delete("If-Modified-Since");
          prewarmHeaders.delete("If-None-Match");
          prewarmHeaders.set("X-Metadata-Prewarm", "1");
          prewarmOptions.headers = prewarmHeaders;
          const prewarmTimeoutMs = clampIntegerConfig(options.prewarmTimeoutMs, Config.Defaults.MetadataPrewarmTimeoutMs, 250, 10000);
          let timeoutId = null;
          try {
            if (prewarmTimeoutMs > 0) {
              const controller = new AbortController();
              prewarmOptions.signal = controller.signal;
              timeoutId = setTimeout(() => controller.abort(), prewarmTimeoutMs);
            }
            const prewarmResponse = await fetch(target.upstreamUrl.toString(), prewarmOptions);
            const warmTraits = {
              isImage: GLOBALS.Regex.EmbyImages.test(target.proxyPath) || GLOBALS.Regex.ImageExt.test(target.proxyPath),
              isSubtitle: GLOBALS.Regex.SubtitleExt.test(target.proxyPath),
              isManifest: GLOBALS.Regex.ManifestExt.test(target.proxyPath)
            };
            await this.storeMetadataCache(cacheKey, prewarmResponse, warmTraits, { ...options, sourceUrl: target.upstreamUrl });
          } finally {
            if (timeoutId !== null) clearTimeout(timeoutId);
          }
        } catch {}
      }
    })());
  },
  shouldRetryWithProtocolFallback(response, state = {}) {
    if (response.status !== 403) return false;
    if (state.isRetry !== false) return false;
    if (state.protocolFallback !== true) return false;
    if (state.allowAutomaticRetry !== true) return false;
    if (state.preparedBodyMode === "stream") return false;
    return true;
  },
  async performFetchWithTimeout(finalUrl, buildFetchOptions, options = {}) {
    const fetchOptions = await buildFetchOptions(finalUrl, options);
    const timeoutMs = Math.max(0, Number(options.timeoutMs) || 0);
    let timeoutId = null;
    let controller = null;
    if (timeoutMs > 0) {
      controller = new AbortController();
      fetchOptions.signal = controller.signal;
      timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    }
    try {
      const response = await fetch(finalUrl.toString(), fetchOptions);
      return { response, finalUrl };
    } catch (error) {
      if (timeoutMs > 0 && (error?.name === "AbortError" || String(error?.message || "").toLowerCase().includes("abort"))) {
        /** @type {AppError} */
        const timeoutError = new Error(`upstream_timeout_${timeoutMs}ms`);
        timeoutError.code = "UPSTREAM_TIMEOUT";
        throw timeoutError;
      }
      throw error;
    } finally {
      if (timeoutId !== null) clearTimeout(timeoutId);
    }
  },
  async performUpstreamFetch(targetBase, proxyPath, requestUrl, buildFetchOptions, options = {}) {
    const finalUrl = buildUpstreamProxyUrl(targetBase, proxyPath);
    finalUrl.search = requestUrl.search;
    const result = await this.performFetchWithTimeout(finalUrl, buildFetchOptions, options);
    return { ...result, targetBase };
  },
  async fetchAbsoluteWithRetryLoop(state) {
    let lastError = null;
    let lastResponse = null;
    const absoluteUrl = state.absoluteUrl instanceof URL ? new URL(state.absoluteUrl.toString()) : new URL(String(state.absoluteUrl || ""));
    const totalPasses = Math.max(1, clampIntegerConfig(state.maxExtraAttempts, Config.Defaults.UpstreamRetryAttempts, 0, 3) + 1);

    for (let pass = 0; pass < totalPasses; pass++) {
      const effectiveRetry = state.isRetry === true || pass > 0;
      try {
        const upstream = await this.performFetchWithTimeout(absoluteUrl, state.buildFetchOptions, {
          ...state.fetchOptions,
          isRetry: effectiveRetry,
          timeoutMs: state.upstreamTimeoutMs
        });
        const response = upstream.response;

        if (response.status === 101) {
          return upstream;
        }

        if (this.shouldRetryWithProtocolFallback(response, { ...state, isRetry: effectiveRetry })) {
          try { response.body?.cancel?.(); } catch {}
          return await this.fetchAbsoluteWithRetryLoop({ ...state, isRetry: true });
        }

        const isLastPass = pass === totalPasses - 1;
        if (state.allowAutomaticRetry !== true || !state.retryableStatuses.has(response.status) || isLastPass) {
          return upstream;
        }

        if (lastResponse) {
          try { lastResponse.body?.cancel?.(); } catch {}
        }
        lastResponse = response;
      } catch (error) {
        lastError = error;
        const isLastPass = pass === totalPasses - 1;
        if (state.allowAutomaticRetry !== true || isLastPass) throw error;
      }
    }

    if (lastResponse) return { response: lastResponse, finalUrl: absoluteUrl };
    throw lastError || new Error("redirect_fetch_failed");
  },
  async fetchUpstreamWithRetryLoop(state) {
    let lastError = null;
    let lastResponse = null;
    let lastBase = state.retryTargets[0];
    let lastFinalUrl = buildUpstreamProxyUrl(lastBase, state.proxyPath);
    lastFinalUrl.search = state.requestUrl.search;

    const totalPasses = Math.max(1, clampIntegerConfig(state.maxExtraAttempts, Config.Defaults.UpstreamRetryAttempts, 0, 3) + 1);
    for (let pass = 0; pass < totalPasses; pass++) {
      for (let index = 0; index < state.retryTargets.length; index++) {
        const targetBase = state.retryTargets[index];
        lastBase = targetBase;
        const effectiveRetry = state.isRetry === true || pass > 0;
        try {
          const upstream = await this.performUpstreamFetch(targetBase, state.proxyPath, state.requestUrl, state.buildFetchOptions, {
            isRetry: effectiveRetry,
            timeoutMs: state.upstreamTimeoutMs
          });
          lastFinalUrl = upstream.finalUrl;
          const response = upstream.response;

          if (response.status === 101) {
            return upstream;
          }

          if (this.shouldRetryWithProtocolFallback(response, { ...state, isRetry: effectiveRetry })) {
            try { response.body?.cancel?.(); } catch {}
            return await this.fetchUpstreamWithRetryLoop({ ...state, isRetry: true });
          }

          const isLastTarget = index === state.retryTargets.length - 1;
          const isLastPass = pass === totalPasses - 1;
          if (state.allowAutomaticRetry !== true || !state.retryableStatuses.has(response.status) || (isLastTarget && isLastPass)) {
            return upstream;
          }

          if (lastResponse) {
            try { lastResponse.body?.cancel?.(); } catch {}
          }
          lastResponse = response;
        } catch (error) {
          lastError = error;
          const isLastTarget = index === state.retryTargets.length - 1;
          const isLastPass = pass === totalPasses - 1;
          if (isLastTarget && isLastPass) throw error;
        }
      }
    }

    if (lastResponse) return { response: lastResponse, targetBase: lastBase, finalUrl: lastFinalUrl };
    throw lastError || new Error("upstream_fetch_failed");
  },
  async handle(request, node, path, name, key, env, ctx, options = {}) {
    // Proxy.handle 阶段图（单文件内的执行主链）：
    // Phase A. 环境准备：配置、来源、CORS、客户端身份
    // Phase B. 前置裁决：OPTIONS / 防火墙 / 限流 / 目标源合法性
    // Phase C. 请求整备：分类、头部整理、body/重试目标准备
    // Phase D. 上游访问：fetch + 协议回退 + 多目标重试
    // Phase E. 跳转决策：同源/异源、直连/继续代理
    // Phase F. 响应整形：缓存头、CORS、Location 改写
    // Phase G. 观测记录：分类、状态码、错误细节、耗时
    const startTime = Date.now();
    CacheManager.maybeCleanup();
    if (!node || !node.target) return new Response("Invalid Node", { status: 502, headers: applySecurityHeaders(new Headers()) });

    const currentConfig = await getRuntimeConfig(env);
    const requestUrl = options.requestUrl || new URL(request.url);
    const proxyPath = sanitizeProxyPath(path);
    const clientIp = request.headers.get("cf-connecting-ip") || "unknown";
    const country = request.cf?.country || "UNKNOWN";
    const finalOrigin = this.resolveCorsOrigin(currentConfig, request);
    const dynamicCors = getCorsHeadersForResponse(env, request, finalOrigin);

    if (request.method === "OPTIONS") {
      const headers = new Headers(dynamicCors);
      applySecurityHeaders(headers);
      if (finalOrigin !== "*") mergeVaryHeader(headers, "Origin");
      return new Response(null, { headers });
    }

    const blockedResponse = this.evaluateFirewall(currentConfig, clientIp, country, finalOrigin);
    if (blockedResponse) return blockedResponse;

    const nodeDirectSource = isNodeDirectSourceEnabled(node, currentConfig);
    const requestTraits = this.classifyRequest(request, proxyPath, requestUrl, currentConfig, {
      nodeDirectSource,
      directStaticAssets: currentConfig.directStaticAssets === true,
      directHlsDash: currentConfig.directHlsDash === true
    });
    const { rangeHeader, prewarmCacheTtl, isImage, isStaticFile, isSubtitle, isManifest, isSegment, isWsUpgrade, isBigStream, isMetadataCacheable, directStaticAssets, directHlsDash } = requestTraits;

    const rateLimitResponse = this.applyRateLimit(currentConfig, clientIp, requestTraits, startTime, finalOrigin);
    if (rateLimitResponse) return rateLimitResponse;

    const enableH2 = currentConfig.enableH2 === true;
    const enableH3 = currentConfig.enableH3 === true;
    const peakDowngrade = currentConfig.peakDowngrade !== false;
    const protocolFallback = currentConfig.protocolFallback !== false; 
    const upstreamTimeoutMs = clampIntegerConfig(currentConfig.upstreamTimeoutMs, Config.Defaults.UpstreamTimeoutMs, 0, 180000);
    const upstreamRetryAttempts = clampIntegerConfig(currentConfig.upstreamRetryAttempts, Config.Defaults.UpstreamRetryAttempts, 0, 3);
    const imageCacheMaxAge = clampIntegerConfig(currentConfig.cacheTtlImages, Config.Defaults.CacheTtlImagesDays, 0, 365) * 86400;
    const utc8Hour = (new Date().getUTCHours() + 8) % 24;
    const isPeakHour = utc8Hour >= 20 && utc8Hour < 24;
    const forceH1 = (peakDowngrade && isPeakHour) || (!enableH2 && !enableH3);

    const metadataCacheKey = (isMetadataCacheable && shouldWorkerCacheMetadataUrl(requestUrl)) ? buildWorkerCacheKey(requestUrl) : null;
    const metadataCache = metadataCacheKey ? getDefaultCacheHandle() : null;
    if (metadataCache && metadataCacheKey) {
      try {
        const cachedResponse = await metadataCache.match(metadataCacheKey);
        if (cachedResponse) {
          const modifiedHeaders = this.buildProxyResponseHeaders(cachedResponse, request, dynamicCors, finalOrigin, requestTraits, {
            enableH3,
            forceH1,
            imageCacheMaxAge
          });
          Logger.record(env, ctx, {
            nodeName: name,
            requestPath: proxyPath,
            requestMethod: request.method,
            statusCode: cachedResponse.status,
            responseTime: Date.now() - startTime,
            clientIp,
            userAgent: request.headers.get("User-Agent"),
            referer: request.headers.get("Referer"),
            category: this.classifyProxyLogCategory(requestTraits),
            errorDetail: this.extractProxyErrorDetail(cachedResponse)
          });
          return new Response(cachedResponse.body, {
            status: cachedResponse.status,
            statusText: cachedResponse.statusText,
            headers: modifiedHeaders
          });
        }
      } catch {}
    }

    const { targetBases, invalidResponse } = this.parseTargetBases(node, finalOrigin);
    if (invalidResponse) return invalidResponse;
    const { newHeaders, adminCustomHeaders, preparedBody, preparedBodyMode, retryTargets, allowAutomaticRetry } =
      await this.buildProxyRequestState(request, node, proxyPath, requestUrl, clientIp, requestTraits, forceH1, targetBases);

    const sourceSameOriginProxy = currentConfig.sourceSameOriginProxy !== false;
    const forceExternalProxy = currentConfig.forceExternalProxy !== false;
    const wangpanDirectKeywords = getWangpanDirectText(currentConfig.wangpandirect || "");
    const buildFetchOptions = async (targetUrl, options = {}) => {
      const headers = new Headers(newHeaders);
      const finalTargetUrl = targetUrl instanceof URL ? targetUrl : new URL(String(targetUrl));
      const targetOrigin = finalTargetUrl.origin;
      const effectiveMethod = String(options.method || request.method || "GET").toUpperCase();
      const effectiveBodyMode = options.bodyMode || preparedBodyMode;
      const effectiveBody = options.body !== undefined ? options.body : preparedBody;
      const isRetry = options.isRetry === true;
      const isExternalRedirect = options.isExternalRedirect === true;

      if (headers.has("Origin") && !adminCustomHeaders.has("origin")) {
        headers.set("Origin", targetOrigin);
      }

      if (headers.has("Referer") && !adminCustomHeaders.has("referer")) {
        try {
          const originalReferer = new URL(headers.get("Referer"));
          if (originalReferer.origin !== targetOrigin) {
            const safeReferer = new URL(originalReferer.pathname + originalReferer.search, targetOrigin);
            headers.set("Referer", safeReferer.toString());
          }
        } catch {
          headers.set("Referer", targetOrigin + "/");
        }
      }

      if (isExternalRedirect) {
        headers.delete("Authorization");
        headers.delete("X-Emby-Authorization");
        headers.delete("Cookie");
        if (!adminCustomHeaders.has("origin")) headers.delete("Origin");
        if (!adminCustomHeaders.has("referer")) headers.delete("Referer");
      }

      if (isRetry && protocolFallback) {
        headers.delete("Authorization");
        headers.delete("X-Emby-Authorization");
        headers.set("Connection", "keep-alive");
      }

      if (effectiveMethod === "GET" || effectiveMethod === "HEAD") {
        headers.delete("Content-Length");
      }

      const canEdgeCacheSubtitle = effectiveMethod === "GET" && !rangeHeader && isSubtitle;
      /** @type {WorkerRequestInit} */
      const fetchOptions = { 
        method: effectiveMethod, 
        headers, 
        redirect: "manual"
      };
      // [字幕优化] 只在明确需要时附加 cf 缓存提示，避免覆盖 Dashboard Cache Rules。
      if (canEdgeCacheSubtitle) fetchOptions.cf = { cacheEverything: true, cacheTtl: 86400 };
      if (effectiveMethod !== "GET" && effectiveMethod !== "HEAD") {
        if (effectiveBodyMode === "buffered" && effectiveBody !== null && effectiveBody !== undefined) fetchOptions.body = effectiveBody.slice(0);
        else if (effectiveBodyMode === "stream") fetchOptions.body = effectiveBody;
      }
      return fetchOptions;
    };

    const retryableStatuses = new Set([500, 502, 503, 504, 522, 523, 524, 525, 526, 530]); 

    let response;
    let finalUrl;
    let activeTargetBase;
    let proxiedExternalRedirect = false;
    let directRedirectUrl = null;
    let directRedirectStatus = null;

    try {
      const upstream = await this.fetchUpstreamWithRetryLoop({
        retryTargets,
        proxyPath,
        requestUrl,
        buildFetchOptions,
        retryableStatuses,
        protocolFallback,
        preparedBodyMode,
        allowAutomaticRetry,
        upstreamTimeoutMs,
        maxExtraAttempts: allowAutomaticRetry ? upstreamRetryAttempts : 0,
        isRetry: false
      });
      response = upstream.response;
      activeTargetBase = upstream.targetBase;
      finalUrl = upstream.finalUrl;

      let redirectHop = 0;
      let redirectMethod = String(request.method || "GET").toUpperCase();
      let redirectBodyMode = preparedBodyMode;
      let redirectBody = preparedBody;
      while (response.status >= 300 && response.status < 400 && redirectHop < 8) {
        const location = response.headers.get("Location");
        const nextUrl = resolveRedirectTarget(location, finalUrl || activeTargetBase);
        if (!nextUrl) break;

        const redirectDecision = this.evaluateRedirectDecision(nextUrl, activeTargetBase, redirectMethod, redirectBodyMode, {
          sourceSameOriginProxy,
          forceExternalProxy,
          wangpanDirectKeywords,
          currentStatus: response.status
        });

        if (redirectDecision.mustDirect) {
          directRedirectUrl = nextUrl;
          break;
        }

        const nextMethod = redirectDecision.nextMethod;
        const nextBodyMode = redirectDecision.nextBodyMode;
        const nextBody = nextBodyMode === "none" ? null : redirectBody;

        try { response.body?.cancel?.(); } catch {}

        const redirectUpstream = await this.fetchAbsoluteWithRetryLoop({
          absoluteUrl: nextUrl,
          buildFetchOptions,
          fetchOptions: {
            method: nextMethod,
            bodyMode: nextBodyMode,
            body: nextBody,
            isExternalRedirect: !redirectDecision.isSameOriginRedirect
          },
          retryableStatuses,
          protocolFallback,
          preparedBodyMode: nextBodyMode,
          allowAutomaticRetry,
          upstreamTimeoutMs,
          maxExtraAttempts: allowAutomaticRetry ? upstreamRetryAttempts : 0,
          isRetry: false
        });
        response = redirectUpstream.response;
        finalUrl = redirectUpstream.finalUrl;
        redirectMethod = nextMethod;
        redirectBodyMode = nextBodyMode;
        redirectBody = nextBody;
        if (!redirectDecision.isSameOriginRedirect) proxiedExternalRedirect = true;
        redirectHop += 1;
      }

      if (!directRedirectUrl && response.status >= 200 && response.status < 300 && (request.method === "GET" || request.method === "HEAD") && (nodeDirectSource || directStaticAssets || directHlsDash)) {
        directRedirectUrl = finalUrl instanceof URL ? new URL(finalUrl.toString()) : buildUpstreamProxyUrl(activeTargetBase, proxyPath);
        if (!(finalUrl instanceof URL)) directRedirectUrl.search = requestUrl.search;
        directRedirectStatus = 307;
        try { response.body?.cancel?.(); } catch {}
      }

      const finalStatus = directRedirectStatus || response.status;
      const finalStatusText = directRedirectStatus ? "Temporary Redirect" : response.statusText;
      const modifiedHeaders = this.buildProxyResponseHeaders(response, request, dynamicCors, finalOrigin, requestTraits, {
        enableH3,
        forceH1,
        proxiedExternalRedirect,
        imageCacheMaxAge
      });
      this.applyProxyRedirectHeaders(modifiedHeaders, response, activeTargetBase, name, key, directRedirectUrl, finalUrl);

      const reqCategory = this.classifyProxyLogCategory(requestTraits);
      const playbackDiagnostic = await this.extractPlaybackInfoDiagnostic(proxyPath, requestUrl, response);
      const errorDetail = this.extractProxyErrorDetail(response) || playbackDiagnostic;

      Logger.record(env, ctx, {
        nodeName: name,
        requestPath: proxyPath,
        requestMethod: request.method,
        statusCode: finalStatus,
        responseTime: Date.now() - startTime,
        clientIp,
        userAgent: request.headers.get("User-Agent"),
        referer: request.headers.get("Referer"),
        category: reqCategory,
        errorDetail: errorDetail // [新增]
      });

      if (metadataCacheKey && ctx && response.status === 200) {
        const cacheClone = response.clone();
        ctx.waitUntil(this.storeMetadataCache(metadataCacheKey, cacheClone, requestTraits, {
          sourceUrl: requestUrl,
          prewarmCacheTtl,
          imageCacheMaxAge
        }));
      }
      await this.maybePrewarmMetadataResponse(request, response, requestTraits, activeTargetBase, buildFetchOptions, name, key, requestUrl, ctx, {
        proxyPath,
        prewarmCacheTtl,
        imageCacheMaxAge
      });
      /** @type {UpgradeableResponse} */
      const upgradeResponse = response;
      if (!directRedirectStatus && response.status === 101 && upgradeResponse.webSocket) {
        /** @type {ResponseInit & { webSocket?: unknown }} */
        const upgradeInit = {
          status: 101,
          statusText: response.statusText,
          headers: modifiedHeaders,
          webSocket: upgradeResponse.webSocket
        };
        return new Response(null, upgradeInit);
      }
      return new Response(directRedirectStatus ? null : response.body, {
        status: finalStatus,
        statusText: finalStatusText,
        headers: modifiedHeaders
      });

    } catch (err) {
      Logger.record(env, ctx, {
        nodeName: name,
        requestPath: proxyPath,
        requestMethod: request.method,
        statusCode: 502,
        responseTime: Date.now() - startTime,
        clientIp,
        category: "error",
        errorDetail: err.message || "网关或 CF Workers 内部崩溃" // [新增]
      });

      const errHeaders = new Headers({
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": finalOrigin || "*",
        "Cache-Control": "no-store"
      });

      if (finalOrigin !== "*") mergeVaryHeader(errHeaders, "Origin");
      applySecurityHeaders(errHeaders);

      return new Response(
        JSON.stringify({ error: "Bad Gateway", code: 502, message: "All proxy attempts failed." }),
        { status: 502, headers: errHeaders }
      );
    }
  }
};

// ============================================================================
// 4. 日志与观测模块 (LOGGER & OPS MODULE)
// 说明：
// - 这里负责请求日志的内存排队、批量刷入 D1，以及运行状态的最小回写。
// - 这是“可解释观测”边界，不承诺强一致审计。
// ============================================================================
const Logger = {
  record(env, ctx, logData) {
    const db = Database.getDB(env);
    if (!db || !ctx) return;
    if (logData.requestMethod === "OPTIONS") return;

    const currentMs = nowMs();
    let dedupeWindow = 0;
    if (logData.requestMethod === "HEAD") dedupeWindow = 300000;
    else if (logData.category === "segment" || logData.category === "prewarm") dedupeWindow = 30000;

    if (dedupeWindow > 0) {
      const dedupKey = [logData.nodeName || "unknown", logData.requestMethod || "GET", logData.statusCode || 0, logData.requestPath || "/", logData.clientIp || "unknown"].join("|");
      const lastSeen = GLOBALS.LogDedupe.get(dedupKey);
      if (lastSeen && (currentMs - lastSeen) < dedupeWindow) return;
      GLOBALS.LogDedupe.set(dedupKey, currentMs);
      if (GLOBALS.LogDedupe.size > 10000) {
        const scannedEntries = [];
        for (const [key, ts] of GLOBALS.LogDedupe) {
          scannedEntries.push([key, ts]);
          if (scannedEntries.length >= 5000) break;
        }
        for (const [key, ts] of scannedEntries) {
          if (!GLOBALS.LogDedupe.has(key)) continue;
          if ((currentMs - ts) > dedupeWindow) {
            GLOBALS.LogDedupe.delete(key);
          }
          if (GLOBALS.LogDedupe.size <= 5000) break;
        }
      }
    }

    GLOBALS.LogQueue.push({
      timestamp: currentMs,
      nodeName: logData.nodeName || "unknown",
      requestPath: logData.requestPath || "/",
      requestMethod: logData.requestMethod || "GET",
      statusCode: Number(logData.statusCode) || 0,
      responseTime: Number(logData.responseTime) || 0,
      clientIp: logData.clientIp || "unknown",
      userAgent: logData.userAgent || null,
      referer: logData.referer || null,
      category: logData.category || "api",
      errorDetail: logData.errorDetail || null, // [新增] 记录错误详情
      createdAt: new Date().toISOString()
    });
    // 💡 [极简修复 1] 内存泄流阀：如果 D1 阻塞导致队列堆积，强行丢弃最老的日志，死守内存底线
    if (GLOBALS.LogQueue.length > 2000) {
      GLOBALS.LogQueue.splice(0, 1000); 
      Database.patchOpsStatus(env, {
        log: {
          lastOverflowAt: new Date().toISOString(),
          lastOverflowDropCount: 1000,
          queueLengthAfterDrop: GLOBALS.LogQueue.length
        }
      }, ctx);
      console.error("Log queue overflow, dropping 1000 logs to prevent OOM.");
    }

    if (!GLOBALS.LogLastFlushAt) GLOBALS.LogLastFlushAt = currentMs;
    const configuredDelayMinutes = Number(GLOBALS.ConfigCache?.data?.logWriteDelayMinutes);
    const configuredFlushCount = Number(GLOBALS.ConfigCache?.data?.logFlushCountThreshold);
    const flushWindowMs = Math.max(0, Number.isFinite(configuredDelayMinutes) ? configuredDelayMinutes * 60000 : Config.Defaults.LogFlushDelayMinutes * 60000);
    const flushCountThreshold = Math.max(1, Number.isFinite(configuredFlushCount) ? Math.floor(configuredFlushCount) : Config.Defaults.LogFlushCountThreshold);
    const shouldFlush = GLOBALS.LogQueue.length >= flushCountThreshold || flushWindowMs === 0 || (currentMs - GLOBALS.LogLastFlushAt) >= flushWindowMs;
    if (shouldFlush && !GLOBALS.LogFlushPending) {
      GLOBALS.LogFlushPending = true;
      ctx.waitUntil(this.flush(env).finally(() => {
        GLOBALS.LogFlushPending = false;
        GLOBALS.LogLastFlushAt = nowMs();
      }));
    }
  },
  async flush(env) {
    const db = Database.getDB(env);
    if (!db || GLOBALS.LogQueue.length === 0) return;
    const configuredChunkSize = Number(GLOBALS.ConfigCache?.data?.logBatchChunkSize);
    const configuredRetryCount = Number(GLOBALS.ConfigCache?.data?.logBatchRetryCount);
    const configuredRetryBackoffMs = Number(GLOBALS.ConfigCache?.data?.logBatchRetryBackoffMs);
    const chunkSize = clampIntegerConfig(configuredChunkSize, Config.Defaults.LogBatchChunkSize, 1, 100);
    const maxRetryCount = clampIntegerConfig(configuredRetryCount, Config.Defaults.LogBatchRetryCount, 0, 5);
    const retryBackoffMs = clampIntegerConfig(configuredRetryBackoffMs, Config.Defaults.LogBatchRetryBackoffMs, 0, 5000);
    let writtenCount = 0;
    let retryCount = 0;
    let activeBatchSize = 0;
    let activeBatchWrittenCount = 0;
    try {
      // 同一次 flush 持续排空期间新增的日志，避免首批写完后尾批滞留到下一次请求。
      while (GLOBALS.LogQueue.length > 0) {
        const batchLogs = GLOBALS.LogQueue.splice(0, GLOBALS.LogQueue.length);
        activeBatchSize = batchLogs.length;
        activeBatchWrittenCount = 0;
        for (let index = 0; index < batchLogs.length; index += chunkSize) {
          const chunk = batchLogs.slice(index, index + chunkSize);
          const statements = chunk.map(item => db.prepare(`INSERT INTO proxy_logs (timestamp, node_name, request_path, request_method, status_code, response_time, client_ip, user_agent, referer, category, error_detail, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).bind(item.timestamp, item.nodeName, item.requestPath, item.requestMethod, item.statusCode, item.responseTime, item.clientIp, item.userAgent, item.referer, item.category, item.errorDetail, item.createdAt));
          let attempt = 0;
          while (true) {
            try {
              await db.batch(statements);
              break;
            } catch (error) {
              if (attempt >= maxRetryCount) throw error;
              attempt += 1;
              retryCount += 1;
              if (retryBackoffMs > 0) await sleepMs(retryBackoffMs * attempt);
            }
          }
          writtenCount += chunk.length;
          activeBatchWrittenCount += chunk.length;
        }
      }
      await Database.patchOpsStatus(env, {
        log: {
          lastFlushAt: new Date().toISOString(),
          lastFlushCount: writtenCount,
          lastFlushStatus: "success",
          lastFlushRetryCount: retryCount,
          queueLengthAfterFlush: GLOBALS.LogQueue.length,
          lastFlushError: null,
          lastFlushErrorAt: null,
          lastDroppedBatchSize: 0,
          lastFlushWrittenBeforeError: 0
        }
      });
    } catch (e) {
      // 🌟 性能防御：D1 写入失败直接丢弃批次，严禁 unshift 导致队列内存堆积与时间轴错乱
      await Database.patchOpsStatus(env, {
        log: {
          lastFlushErrorAt: new Date().toISOString(),
          lastFlushStatus: "failed",
          lastFlushError: e?.message || String(e),
          lastFlushRetryCount: retryCount,
          lastDroppedBatchSize: Math.max(0, activeBatchSize - activeBatchWrittenCount),
          lastFlushWrittenBeforeError: writtenCount,
          queueLengthAfterFlush: GLOBALS.LogQueue.length
        }
      });
      console.log("Log flush failed, dropping batch.", e);
    }
  }
};

// ============================================================================
// 5. 新版 SAAS UI (纯净版：彻底删除所有冗余设置)
// ============================================================================
const UI_HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
  <title>Emby Proxy V18.5 - SaaS Dashboard</title>
  <script>
    window.__ADMIN_BOOTSTRAP__ = __ADMIN_BOOTSTRAP__;
    window.__ADMIN_UI_BOOTED__ = false;
    window.__ADMIN_UI_DEPENDENCY_TIMEOUT__ = setTimeout(function watchdog() {
      if (window.__ADMIN_UI_BOOTED__ || window.Vue) return;
      var target = document.getElementById('app') || document.body;
      if (!target) {
        setTimeout(watchdog, 500);
        return;
      }
      target.innerHTML = '<div class="min-h-screen flex items-center justify-center px-6 py-10"><div class="max-w-lg w-full rounded-[28px] border border-red-200 bg-white p-6 shadow-xl"><h1 class="text-xl font-bold text-slate-900">管理台资源加载失败</h1><p class="mt-3 text-sm leading-6 text-slate-600">检测到前端依赖长时间未完成加载，可能是当前网络环境无法稳定访问 CDN。</p><p class="mt-2 text-sm leading-6 text-slate-600">请稍后重试，或检查是否需要自建前端资源镜像。</p></div></div>';
    }, 8000);
  </script>
  <script src="https://cdn.tailwindcss.com/3.4.17"></script>
  <script src="https://cdn.jsdelivr.net/npm/vue@3/dist/vue.global.prod.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/lucide@latest/dist/umd/lucide.min.js"></script>
  <script defer src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: { extend: { colors: { brand: { 50: '#eff6ff', 500: '#3b82f6', 600: '#2563eb' } } } }
    }
  </script>
  <style>
    [v-cloak] { display: none; }
    .glass-card { background: #ffffff; border: 1px solid #e2e8f0; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.04); }
    .dark .glass-card { background: #020617; border: 1px solid #1e293b; box-shadow: none; }
    :root { --ui-radius-px: 24px; }
    .glass-card,
    .ui-radius-card,
    #view-settings .settings-nav-shell,
    #view-settings .settings-panel,
    #view-settings .settings-block,
    #view-settings .settings-list-shell,
    #node-modal > div {
      border-radius: var(--ui-radius-px) !important;
    }
    .view-section { display: none; }
    .view-section.active { display: block; animation: fadeIn 0.3s ease-out; }
    @keyframes fadeIn { from { opacity: 0; transform: translateY(5px); } to { opacity: 1; transform: translateY(0); } }
    aside { transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1); }
    #view-settings .settings-nav-shell,
    #view-settings .settings-panel,
    #view-settings .settings-block,
    #view-settings .settings-list-shell {
      box-shadow: none !important;
      backdrop-filter: none;
      -webkit-backdrop-filter: none;
    }
    #view-settings .settings-nav-shell,
    #view-settings .settings-panel {
      background: #ffffff !important;
    }
    #view-settings .settings-block,
    #view-settings .settings-list-shell {
      background: #f8fafc !important;
    }
    .dark #view-settings .settings-nav-shell,
    .dark #view-settings .settings-panel {
      background: #0f172a !important;
    }
    .dark #view-settings .settings-block,
    .dark #view-settings .settings-list-shell {
      background: #020617 !important;
    }
    @media (min-width: 768px) {
      #app-shell.settings-split-layout #content-area {
        overflow: hidden;
      }
      #app-shell.settings-split-layout #view-settings {
        height: 100%;
        min-height: 0;
        overflow: hidden;
      }
      #app-shell.settings-split-layout #view-settings .settings-view-layout {
        height: 100%;
        min-height: 0;
      }
      #app-shell.settings-split-layout #view-settings .settings-nav-shell {
        position: sticky;
        top: 0;
        max-height: 100%;
        overflow-y: auto;
      }
      #app-shell.settings-split-layout #view-settings #settings-forms {
        height: 100%;
        min-height: 0;
        overflow-y: auto;
        padding-right: 0.25rem;
        scrollbar-gutter: stable;
      }
    }
  </style>
</head>
<body class="bg-slate-50 text-slate-900 antialiased overflow-hidden h-[100dvh]">
  <div id="app" v-cloak></div>

  <template id="tpl-copy-button">
    <button type="button" @click="copyText" class="flex-1 py-2 text-sm font-medium border border-slate-200 dark:border-slate-700 rounded-xl hover:bg-slate-50 dark:hover:bg-slate-800 transition">
      {{ copied ? '已复制' : label }}
    </button>
  </template>

  <template id="tpl-node-card">
    <div class="glass-card p-6 rounded-3xl flex flex-col justify-between" v-lucide-icons>
      <div>
        <div class="flex items-end mb-2 w-full gap-3">
          <div class="inline-flex items-center justify-center rounded-full px-2.5 py-1 text-sm leading-5 font-semibold border truncate max-w-[7rem]" :class="tagToneClass">{{ hasTag ? hydratedNode.tag : '无标签' }}</div>
          <div class="flex-1 min-w-0 flex items-end gap-2">
            <h3 class="font-bold text-xl md:text-2xl transition-colors min-w-0 truncate" :class="statusMeta.titleClass">{{ displayName }}</h3>
            <span class="inline-flex max-w-full flex-shrink-0 items-center rounded-lg border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-xs font-semibold text-emerald-800 dark:border-emerald-700 dark:bg-emerald-900 dark:text-emerald-100" title="当前启用线路">
              <span class="truncate max-w-[9rem]">{{ activeLineName }}</span>
            </span>
          </div>
        </div>

        <div class="text-sm text-slate-500 dark:text-slate-400 mb-2 flex justify-between tracking-wide">
          <div class="flex items-center min-w-0">
            <span class="w-3 h-3 rounded-full mr-2 transition-colors duration-500 flex-shrink-0 shadow-inner" :class="statusMeta.dotClass"></span>
            <span>Ping: <span :class="statusMeta.textClass" :title="latencyTitle">{{ statusMeta.text }}</span></span>
          </div>
          <span class="truncate ml-2 text-right"><i data-lucide="shield" class="w-3 h-3 inline"></i> {{ hydratedNode.secret ? '已防护' : '未防护' }}</span>
        </div>

        <div class="mt-2 mb-3 border-t border-dashed border-slate-200/80 dark:border-slate-700/70"></div>

        <div class="text-xs text-slate-500 dark:text-slate-400 mb-3 space-y-1">
          <div v-if="remarkValue" class="flex items-center min-w-0">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="w-4 h-4 mr-1.5 flex-shrink-0 text-red-500 dark:text-red-400">
              <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
              <line x1="12" y1="9" x2="12" y2="13"></line>
              <line x1="12" y1="17" x2="12.01" y2="17"></line>
            </svg>
            <span class="truncate flex-1 min-w-0 text-[15px] md:text-base leading-6 font-medium text-red-600 dark:text-red-400">{{ remarkValue }}</span>
          </div>
          <div class="flex items-center min-w-0">
            <i data-lucide="route" class="w-3 h-3 mr-1.5 flex-shrink-0 text-emerald-500"></i>
            <span class="truncate flex-1 min-w-0 text-[15px] md:text-base leading-6 font-medium text-emerald-700 dark:text-emerald-300">线路：共 {{ lineCount }} 条</span>
          </div>
        </div>
      </div>

      <div>
        <div class="flex items-center bg-slate-100 dark:bg-slate-800 p-2 rounded-xl mb-4 border border-slate-200 dark:border-slate-700">
          <input :type="revealLink ? 'text' : 'password'" readonly :value="link" class="bg-transparent border-none flex-1 min-w-0 text-xs outline-none text-slate-600 dark:text-slate-300">
          <button type="button" class="text-slate-400 hover:text-brand-500 ml-2" @click="toggleLinkVisibility"><i :data-lucide="revealLink ? 'eye-off' : 'eye'" class="w-4 h-4"></i></button>
        </div>

        <div class="flex gap-2">
          <button type="button" :disabled="pingPending" class="px-3 border border-emerald-200 dark:border-emerald-800/50 text-emerald-600 dark:text-emerald-400 rounded-xl hover:bg-emerald-50 dark:hover:bg-emerald-900/30 transition flex items-center justify-center flex-shrink-0 disabled:opacity-60 disabled:cursor-not-allowed" title="测试当前启用线路" @click="pingNode">
            <i v-if="!pingPending" data-lucide="activity" class="w-4 h-4"></i>
            <i v-else data-lucide="loader" class="w-4 h-4 animate-spin"></i>
          </button>
          <copy-button :text="link" label="复制"></copy-button>
          <button type="button" class="flex-1 py-2 text-sm font-medium bg-brand-50 text-brand-600 dark:bg-brand-500/10 dark:text-brand-400 rounded-xl hover:bg-brand-100 dark:hover:bg-brand-500/20 transition" @click="editNode">编辑</button>
          <button type="button" class="px-3 border border-red-100 dark:border-red-900/30 text-red-500 rounded-xl hover:bg-red-50 dark:hover:bg-red-900/20 transition flex items-center justify-center flex-shrink-0" @click="deleteNode"><i data-lucide="trash-2" class="w-4 h-4"></i></button>
        </div>
      </div>
    </div>
  </template>

  <template id="tpl-app">
  <div class="h-full" :class="{ dark: App.isDarkTheme }">
  <div id="app-shell" v-lucide-icons class="bg-slate-50 text-slate-900 dark:bg-slate-950 dark:text-slate-100 antialiased overflow-hidden flex h-[100dvh]" :class="{ 'settings-split-layout': App.isDesktopSettingsLayout }" :style="{ '--ui-radius-px': App.uiRadiusCssValue, colorScheme: App.isDarkTheme ? 'dark' : 'light' }">

  <div id="sidebar-backdrop" @click="App.toggleSidebar()" class="fixed inset-0 bg-slate-950/60 z-20 backdrop-blur-sm transition-opacity" :class="{ hidden: !App.sidebarOpen }"></div>

  <aside id="sidebar" class="w-64 h-full border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 flex flex-col z-30 absolute md:relative shadow-2xl md:shadow-none pt-[env(safe-area-inset-top)] pb-[env(safe-area-inset-bottom)] pl-[env(safe-area-inset-left)]" :class="App.sidebarOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0'">
    <div class="h-16 flex items-center px-6 border-b border-slate-200 dark:border-slate-800">
      <div class="w-8 h-8 rounded-lg bg-gradient-to-br from-brand-500 to-indigo-600 flex items-center justify-center text-white font-bold text-lg flex-shrink-0">E</div>
      <h1 class="ml-3 font-semibold tracking-tight text-lg flex items-center gap-2">
        Emby Proxy 
        <span class="px-1.5 py-0.5 rounded bg-brand-100 text-brand-600 dark:bg-brand-500/20 dark:text-brand-400 text-[10px] font-bold mt-0.5">V18.5</span>
      </h1>
    </div>
    <nav class="flex-1 overflow-y-auto py-4 px-3 space-y-1">
      <a v-for="item in App.navItems.slice(0, 4)" :key="item.hash" :href="item.hash" @click.prevent="App.navigate(item.hash)" class="nav-item flex items-center px-3 py-2.5 rounded-xl text-sm font-medium transition-colors text-slate-600 dark:text-slate-400 hover:text-slate-900 hover:bg-slate-100 dark:hover:text-white dark:hover:bg-slate-800/50" :class="App.getNavItemClass(item.hash)"><i :data-lucide="item.icon" class="w-5 h-5 mr-3"></i> {{ item.label }}</a>
      <div class="my-4 border-t border-slate-200 dark:border-slate-800"></div>
      <a :href="App.navItems[4].hash" @click.prevent="App.navigate(App.navItems[4].hash)" class="nav-item flex items-center px-3 py-2.5 rounded-xl text-sm font-medium transition-colors text-slate-600 dark:text-slate-400 hover:text-slate-900 hover:bg-slate-100 dark:hover:text-white dark:hover:bg-slate-800/50" :class="App.getNavItemClass(App.navItems[4].hash)"><i :data-lucide="App.navItems[4].icon" class="w-5 h-5 mr-3"></i> {{ App.navItems[4].label }}</a>
    </nav>
  </aside>

  <main class="flex-1 flex flex-col h-full min-w-0 relative">
    <header class="flex items-center justify-between px-6 bg-white dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 z-10 sticky top-0 h-[calc(4rem+env(safe-area-inset-top))] pt-[env(safe-area-inset-top)] pl-[max(1.5rem,env(safe-area-inset-left))] pr-[max(1.5rem,env(safe-area-inset-right))]">
      <div class="flex items-center">
        <button @click="App.toggleSidebar()" class="md:hidden mr-4 text-slate-500 hover:text-slate-900"><i data-lucide="menu" class="w-5 h-5"></i></button>
        <h2 id="page-title" class="text-lg font-semibold tracking-tight">{{ App.pageTitle }}</h2>
      </div>
      <div class="flex items-center space-x-4">
        <a href="https://github.com/axuitomo/CF-EMBY-PROXY-UI" target="_blank" class="text-slate-400 hover:text-slate-900 dark:hover:text-white transition"><i data-lucide="github" class="w-5 h-5"></i></a>
        <button @click="App.toggleTheme()" v-auto-animate="{ duration: 180 }" class="text-slate-400 hover:text-brand-500 transition">
          <span v-if="!App.isDarkTheme" key="theme-icon-sun"><i data-lucide="sun" class="w-5 h-5"></i></span>
          <span v-else key="theme-icon-moon"><i data-lucide="moon" class="w-5 h-5"></i></span>
        </button>
      </div>
    </header>

    <div id="content-area" v-scroll-reset="App.contentScrollResetKey" class="flex-1 overflow-y-auto p-4 md:p-8 pb-[calc(1rem+env(safe-area-inset-bottom))] md:pb-[calc(2rem+env(safe-area-inset-bottom))] pl-[max(1rem,env(safe-area-inset-left))] pr-[max(1rem,env(safe-area-inset-right))]">
      
      <div id="view-dashboard" class="view-section w-full mx-auto space-y-6" :class="{ active: App.currentHash === '#dashboard' }">
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
           <div class="glass-card rounded-3xl p-6 shadow-sm border-l-4 border-blue-500 min-w-0 overflow-hidden relative"><p class="text-sm text-slate-500 truncate">今日请求量</p><h3 class="text-2xl md:text-3xl font-bold mt-2 break-all" id="dash-req-count" :title="App.dashboardView.requests.title">{{ App.dashboardView.requests.count }}</h3><p class="text-xs font-medium text-slate-500 mt-2 break-all" id="dash-req-hint" :title="App.dashboardView.requests.title">{{ App.dashboardView.requests.hint }}</p><div id="dash-req-meta" class="flex flex-wrap gap-2 mt-3"><span v-for="(badge, badgeIndex) in App.dashboardView.requests.badges" :key="'req-badge-' + badgeIndex + '-' + badge.label" class="px-2.5 py-1 rounded-full text-[11px] font-medium" :class="App.getDashboardBadgeClass(badge.tone)">{{ badge.label }}</span></div><p class="text-[11px] font-medium text-brand-600 dark:text-brand-400 mt-2 break-all bg-brand-50 dark:bg-brand-500/10 inline-block px-2.5 py-1 rounded-md" id="dash-emby-metrics">{{ App.dashboardView.requests.embyMetrics }}</p></div>
           <div class="glass-card rounded-3xl p-6 shadow-sm border-l-4 border-emerald-500 min-w-0 overflow-hidden"><p class="text-sm text-slate-500 truncate">视频流量 (CF Zone 总流量)</p><h3 class="text-2xl md:text-3xl font-bold mt-2 break-all" id="dash-traffic-count" :title="App.dashboardView.traffic.title">{{ App.dashboardView.traffic.count }}</h3><p class="text-xs font-medium text-slate-500 mt-2 break-all" id="dash-traffic-hint" :title="App.dashboardView.traffic.title">{{ App.dashboardView.traffic.hint }}</p><div id="dash-traffic-meta" class="flex flex-wrap gap-2 mt-3"><span v-for="(badge, badgeIndex) in App.dashboardView.traffic.badges" :key="'traffic-badge-' + badgeIndex + '-' + badge.label" class="px-2.5 py-1 rounded-full text-[11px] font-medium" :class="App.getDashboardBadgeClass(badge.tone)">{{ badge.label }}</span></div><p class="text-[11px] text-slate-400 mt-2 break-all whitespace-pre-line" id="dash-traffic-detail">{{ App.dashboardView.traffic.detail }}</p></div>
           <div class="glass-card rounded-3xl p-6 shadow-sm border-l-4 border-purple-500 min-w-0 overflow-hidden"><p class="text-sm text-slate-500 truncate">接入节点</p><h3 class="text-2xl md:text-3xl font-bold mt-2 break-all" id="dash-node-count">{{ App.dashboardView.nodes.count }}</h3><p id="dash-node-meta" class="text-xs font-medium text-slate-500 mt-2 break-all">{{ App.dashboardView.nodes.meta }}</p><div id="dash-node-badges" class="flex flex-wrap gap-2 mt-3"><span v-for="(badge, badgeIndex) in App.dashboardView.nodes.badges" :key="'node-badge-' + badgeIndex + '-' + badge.label" class="px-2.5 py-1 rounded-full text-[11px] font-medium" :class="App.getDashboardBadgeClass(badge.tone)">{{ badge.label }}</span></div></div>
        </div>
        <div class="glass-card rounded-3xl p-6 shadow-sm">
          <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
            <div>
              <h3 class="font-semibold text-lg">运行状态</h3>
              <p id="dash-runtime-updated" class="text-xs text-slate-500 mt-1">{{ App.dashboardRuntimeView.updatedText }}</p>
            </div>
            <button @click="App.loadDashboard()" class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-xl text-sm text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-800 transition flex items-center justify-center">
              <i data-lucide="refresh-cw" class="w-4 h-4 mr-2"></i>刷新状态
            </button>
          </div>
          <div class="grid grid-cols-1 xl:grid-cols-2 gap-4 mt-4">
            <div id="dash-runtime-log-card" class="rounded-2xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-4"><div class="h-full rounded-2xl border border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/60 p-4"><div class="flex items-start justify-between gap-3"><div class="min-w-0"><div class="flex items-center gap-2"><span class="w-2.5 h-2.5 rounded-full" :class="App.getRuntimeStatusMeta(App.dashboardRuntimeView.logCard.status).dotClass"></span><h4 class="font-semibold text-slate-900 dark:text-white">{{ App.dashboardRuntimeView.logCard.title }}</h4></div><p class="text-xs text-slate-500 mt-1 break-all">{{ App.dashboardRuntimeView.logCard.summary }}</p></div><span class="px-2.5 py-1 rounded-full text-xs font-medium whitespace-nowrap" :class="App.getRuntimeStatusMeta(App.dashboardRuntimeView.logCard.status).badgeClass">{{ App.getRuntimeStatusMeta(App.dashboardRuntimeView.logCard.status).label }}</span></div><ul class="space-y-2 mt-4"><li v-for="(line, lineIndex) in App.dashboardRuntimeView.logCard.lines" :key="'log-card-line-' + lineIndex" class="text-sm text-slate-600 dark:text-slate-300 break-all">{{ line }}</li></ul><p v-if="App.dashboardRuntimeView.logCard.detail" class="text-xs text-slate-400 break-all mt-3">{{ App.dashboardRuntimeView.logCard.detail }}</p></div></div>
            <div id="dash-runtime-scheduled-card" class="rounded-2xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-4"><div class="h-full rounded-2xl border border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/60 p-4"><div class="flex items-start justify-between gap-3"><div class="min-w-0"><div class="flex items-center gap-2"><span class="w-2.5 h-2.5 rounded-full" :class="App.getRuntimeStatusMeta(App.dashboardRuntimeView.scheduledCard.status).dotClass"></span><h4 class="font-semibold text-slate-900 dark:text-white">{{ App.dashboardRuntimeView.scheduledCard.title }}</h4></div><p class="text-xs text-slate-500 mt-1 break-all">{{ App.dashboardRuntimeView.scheduledCard.summary }}</p></div><span class="px-2.5 py-1 rounded-full text-xs font-medium whitespace-nowrap" :class="App.getRuntimeStatusMeta(App.dashboardRuntimeView.scheduledCard.status).badgeClass">{{ App.getRuntimeStatusMeta(App.dashboardRuntimeView.scheduledCard.status).label }}</span></div><ul class="space-y-2 mt-4"><li v-for="(line, lineIndex) in App.dashboardRuntimeView.scheduledCard.lines" :key="'scheduled-card-line-' + lineIndex" class="text-sm text-slate-600 dark:text-slate-300 break-all">{{ line }}</li></ul><p v-if="App.dashboardRuntimeView.scheduledCard.detail" class="text-xs text-slate-400 break-all mt-3">{{ App.dashboardRuntimeView.scheduledCard.detail }}</p></div></div>
          </div>
        </div>
        <div class="glass-card rounded-3xl p-6 shadow-sm flex flex-col">
           <h3 class="font-semibold text-lg mb-4">请求趋势</h3>
           <div class="relative w-full h-64 md:h-80 2xl:h-[40vh] min-h-[250px] 2xl:min-h-[450px]"><canvas id="trafficChart" v-traffic-chart="App.dashboardSeries"></canvas></div>
           <p class="text-xs text-slate-500 mt-4">Y 轴（纵轴）代表：该小时内的“请求总次数”；X 轴（横轴）代表：当前天的“小时”时间刻度（UTC+8）。</p>
        </div>
      </div>

      <div id="view-nodes" class="view-section w-full mx-auto space-y-6" :class="{ active: App.currentHash === '#nodes' }">
        <div class="flex flex-col xl:flex-row justify-between items-center gap-4">
          <div class="flex items-center gap-2 w-full xl:w-auto">
            <button @click="App.showNodeModal()" class="px-4 py-2 bg-brand-600 text-white rounded-xl text-sm font-medium hover:bg-brand-700 flex items-center transition whitespace-nowrap"><i data-lucide="plus" class="w-4 h-4 mr-2"></i> 新建节点</button>
            <input type="text" id="node-search" v-model="App.nodeSearchKeyword" placeholder="搜索节点名称或标签..." class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white w-full sm:w-64 transition">
          </div>
          <div class="flex flex-wrap gap-2 w-full xl:w-auto">
            <label class="flex-1 sm:flex-none px-4 py-2 bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-200 rounded-xl text-sm font-medium hover:bg-slate-300 dark:hover:bg-slate-700 transition flex items-center justify-center"><i data-lucide="upload" class="w-4 h-4 mr-2"></i> 导入配置<input type="file" id="import-nodes-file" class="hidden" accept=".json" @change="App.importNodes"></label>
            <button @click="App.exportNodes()" class="flex-1 sm:flex-none px-4 py-2 bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-200 rounded-xl text-sm font-medium hover:bg-slate-300 dark:hover:bg-slate-700 transition flex items-center justify-center"><i data-lucide="download" class="w-4 h-4 mr-2"></i> 导出配置</button>
            <button @click="App.forceHealthCheck()" :disabled="App.nodesHealthCheckPending" class="w-full sm:w-auto px-4 py-2 bg-emerald-600 text-white rounded-xl text-sm font-medium hover:bg-emerald-700 flex items-center justify-center transition disabled:opacity-60 disabled:cursor-not-allowed"><i :data-lucide="App.nodesHealthCheckPending ? 'loader' : 'activity'" :class="App.nodesHealthCheckPending ? 'w-4 h-4 mr-2 animate-spin' : 'w-4 h-4 mr-2'"></i> {{ App.nodesHealthCheckPending ? '探测中...' : '全局 Ping' }}</button>
            <a v-auto-download="{ href: App.downloadHref, key: App.downloadTriggerKey }" :href="App.downloadHref" :download="App.downloadFilename" class="hidden" aria-hidden="true"></a>
          </div>
        </div>
        <div id="nodes-grid" v-auto-animate="{ duration: 180 }" class="grid gap-6 grid-cols-[repeat(auto-fill,minmax(340px,1fr))]">
          <node-card v-for="(node, index) in App.getFilteredNodes()" :key="node.name || node.displayName || ('node-' + index)" :node="node" :app="App"></node-card>
          <div v-if="!App.getFilteredNodes().length" class="col-span-full py-12 text-center text-slate-500">暂无匹配节点</div>
        </div>
      </div>

      <div id="view-logs" class="view-section w-full mx-auto space-y-6" :class="{ active: App.currentHash === '#logs' }">
        <div class="glass-card rounded-3xl p-6 shadow-sm flex flex-col min-h-[calc(100vh-120px)]">
          <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-4 gap-4">
            <h3 class="font-semibold text-lg flex-shrink-0">日志记录</h3>
            <div class="flex flex-wrap items-center gap-2 w-full md:w-auto">
              <input type="date" id="log-start-date-input" v-model="App.logStartDate" class="px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white">
              <span class="text-xs text-slate-400">至</span>
              <input type="date" id="log-end-date-input" v-model="App.logEndDate" class="px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white">
              <input type="text" id="log-search-input" v-model="App.logSearchKeyword" placeholder="搜索节点、IP、路径或状态码(如200)..." class="px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white flex-1 md:w-56" @keydown.enter="App.loadLogs(1)">
              <button @click="App.loadLogs(1)" class="text-brand-500 text-sm px-2 hover:text-brand-600"><i data-lucide="search" class="w-4 h-4 inline"></i></button>
              <div class="flex flex-wrap items-center gap-1.5">
                <button data-log-playback-filter="" @click="App.setLogsPlaybackModeFilter('')" class="px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 text-xs font-medium transition" :class="App.getLogsPlaybackFilterClass('')">全部模式</button>
                <button data-log-playback-filter="transcode" @click="App.setLogsPlaybackModeFilter('transcode')" class="px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 text-xs font-medium transition" :class="App.getLogsPlaybackFilterClass('transcode')">只看转码</button>
                <button data-log-playback-filter="direct_stream" @click="App.setLogsPlaybackModeFilter('direct_stream')" class="px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 text-xs font-medium transition" :class="App.getLogsPlaybackFilterClass('direct_stream')">只看直串</button>
                <button data-log-playback-filter="direct_play" @click="App.setLogsPlaybackModeFilter('direct_play')" class="px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 text-xs font-medium transition" :class="App.getLogsPlaybackFilterClass('direct_play')">只看直放</button>
              </div>
              
              <div class="w-px h-5 bg-slate-300 dark:bg-slate-700 mx-1 hidden md:block"></div>
              
              <button @click="App.initLogsDbFromUi()" class="text-slate-500 text-sm hover:text-brand-500"><i data-lucide="database" class="w-4 h-4 inline mr-1"></i>初始化 DB</button>
              <button @click="App.clearLogsFromUi()" class="text-red-500 text-sm hover:text-red-600 ml-2"><i data-lucide="trash-2" class="w-4 h-4 inline mr-1"></i>清空日志</button>
              <button @click="App.loadLogs()" class="text-brand-500 text-sm ml-2"><i data-lucide="refresh-cw" class="w-4 h-4 inline mr-1"></i>刷新</button>
            </div>
          </div>
          <div class="overflow-x-auto min-h-0 w-full mb-4">
            <table class="w-full text-left border-collapse table-fixed min-w-[900px]">
              <thead><tr class="text-sm text-slate-500 border-b border-slate-200 dark:border-slate-800"><th class="py-3 px-4 w-24 md:w-28">节点</th><th class="py-3 px-4 w-28 md:w-32">资源类别</th><th class="py-3 px-4 w-16 md:w-20">状态</th><th class="py-3 px-4 w-32">IP</th><th class="py-3 px-4">UA</th><th class="py-3 px-4 w-28">时间锥</th></tr></thead>
              <tbody id="logs-tbody" class="text-sm">
                <tr v-if="!App.logRows.length">
                  <td colspan="6" class="py-6 text-center text-slate-500">暂无匹配日志记录</td>
                </tr>
                <tr v-for="(log, index) in App.logRows" v-else :key="log.id || (String(log.timestamp) + '-' + index)" class="border-b border-slate-100 dark:border-slate-800/50 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition">
                  <td class="py-3 px-4 font-medium truncate" :title="log.node_name">{{ log.node_name }}</td>
                  <td class="py-3 px-4 text-xs cursor-pointer truncate" :title="App.getLogPathTitle(log)">
                    <div class="flex flex-wrap items-center gap-1"><span v-for="(badge, badgeIndex) in App.getLogCategoryBadges(log)" :key="(log.id || index) + '-badge-' + badgeIndex + '-' + badge.label" :class="badge.className">{{ badge.label }}</span></div>
                  </td>
                  <td class="py-3 px-4 font-bold truncate" :class="log.status_code >= 400 ? 'text-red-500' : 'text-emerald-500'"><span :class="App.getLogStatusMeta(log).className" :title="App.getLogStatusMeta(log).title">{{ App.getLogStatusMeta(log).text }}</span></td>
                  <td class="py-3 px-4 font-mono text-xs truncate" :title="log.client_ip">{{ log.client_ip }}</td>
                  <td class="py-3 px-4 text-xs text-slate-400 truncate" :title="log.user_agent || '-'">{{ log.user_agent || '-' }}</td>
                  <td class="py-3 px-4 text-xs font-mono text-slate-500 truncate log-time-cell" :data-timestamp="log.timestamp" :title="App.formatUtc8ExactTime(log.timestamp)" :aria-label="App.formatUtc8ExactTime(log.timestamp)" tabindex="0">{{ App.getLogRelativeTime(log.timestamp, App.logTimeTick) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="flex justify-between items-center mt-auto pt-6 border-t border-slate-200 dark:border-slate-800">
              <button @click="App.changeLogPage(-1)" :disabled="App.logPage <= 1" class="px-4 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 text-sm font-medium text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-800 transition disabled:opacity-40 disabled:pointer-events-none">上一页</button>
              <span id="log-page-info" class="text-sm font-mono text-slate-500">{{ App.logPage }} / {{ App.logTotalPages }}</span>
              <button @click="App.changeLogPage(1)" :disabled="App.logPage >= App.logTotalPages" class="px-4 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 text-sm font-medium text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-800 transition disabled:opacity-40 disabled:pointer-events-none">下一页</button>
          </div>
        </div>
      </div>

      <div id="view-dns" class="view-section w-full mx-auto space-y-6" :class="{ active: App.currentHash === '#dns' }">
        <div class="glass-card rounded-3xl p-6 shadow-sm flex flex-col min-h-[calc(100vh-120px)]">
          <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-4 gap-4">
            <div class="min-w-0">
              <h3 class="font-semibold text-lg flex-shrink-0">DNS编辑</h3>
              <p id="dns-zone-hint" class="text-xs text-slate-500 mt-1 break-all">{{ App.dnsZoneHintText }}</p>
              <p class="text-[11px] text-slate-500 mt-1">提示：当前仅展示当前站点对应记录；名称只读；类型仅允许 A / AAAA / CNAME；修改历史会保存到 KV，最多保留 {{ App.dnsHistoryLimit }} 条且相同值不重复记录。</p>
            </div>
            <div class="flex flex-wrap items-center gap-2 w-full md:w-auto">
              <button @click="App.loadDnsRecords()" class="text-brand-500 text-sm"><i data-lucide="refresh-cw" class="w-4 h-4 inline mr-1"></i>刷新</button>
              <button id="dns-save-all-btn" @click="App.saveAllDnsRecords()" :disabled="App.isDnsSaveAllDisabled()" :title="App.getDnsSaveAllTitle()" class="px-4 py-2 bg-brand-600 text-white rounded-xl text-sm font-medium hover:bg-brand-700 flex items-center transition whitespace-nowrap disabled:opacity-40 disabled:pointer-events-none"><i data-lucide="save" class="w-4 h-4 mr-2"></i>{{ App.getDnsSaveAllButtonText() }}</button>
            </div>
          </div>
          <div class="overflow-x-auto min-h-0 w-full mb-4">
            <table class="w-full text-left border-collapse table-fixed min-w-[900px]">
              <thead>
                <tr class="text-sm text-slate-500 border-b border-slate-200 dark:border-slate-800">
                  <th class="py-3 px-4 w-28">类型</th>
                  <th class="py-3 px-4 w-80">名称</th>
                  <th class="py-3 px-4">内容</th>
                  <th class="py-3 px-4 w-28">操作</th>
                </tr>
              </thead>
              <tbody id="dns-tbody" class="text-sm">
                <tr v-for="record in App.dnsRecords" :key="record.id" class="border-b border-slate-200 dark:border-slate-800 hover:bg-slate-50/60 dark:hover:bg-slate-900/40">
                  <td class="py-3 px-4">
                    <select v-if="record.editable" v-model="record.type" @change="record.type = String(record.type || '').toUpperCase(); App.updateDnsSaveAllButtonState()" class="w-full px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white disabled:opacity-50" :disabled="!!record._saving">
                      <option value="A">A</option>
                      <option value="AAAA">AAAA</option>
                      <option value="CNAME">CNAME</option>
                    </select>
                    <div v-else class="text-xs font-mono text-slate-500 dark:text-slate-400 px-2.5 py-1.5 rounded-lg bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700" title="该类型不在受限编辑范围内">{{ (record.type || '-').toUpperCase() }}</div>
                  </td>
                  <td class="py-3 px-4">
                    <input type="text" :value="record.name || ''" disabled class="w-full px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-100 dark:bg-slate-800 outline-none text-sm text-slate-500 dark:text-slate-400">
                  </td>
                  <td class="py-3 px-4">
                    <input type="text" v-model="record.content" @input="App.updateDnsSaveAllButtonState()" :disabled="!record.editable || !!record._saving" :placeholder="record.editable ? '请输入记录内容' : '只读'" class="w-full px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white disabled:bg-slate-100 disabled:dark:bg-slate-800 disabled:text-slate-500 disabled:dark:text-slate-400 disabled:opacity-70">
                    <div v-if="record.history && record.history.length" v-auto-animate class="mt-2 flex flex-wrap gap-2">
                      <button v-for="(entry, historyIndex) in record.history" :key="App.getDnsHistoryEntryKey(record, entry, historyIndex)" type="button" @click="App.applyDnsHistoryEntry(record.id, entry)" :title="App.getDnsHistoryEntryTitle(entry)" class="inline-flex max-w-full items-center gap-1.5 rounded-lg border px-2 py-1 text-[11px] transition" :class="App.isDnsHistoryEntryCurrent(record, entry) ? 'border-brand-200 bg-brand-50 text-brand-600 dark:border-brand-500/30 dark:bg-brand-500/10 dark:text-brand-300' : 'border-slate-200 bg-slate-50 text-slate-600 hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-800/80 dark:text-slate-300 dark:hover:bg-slate-800'">
                        <span class="font-semibold">{{ entry.type }}</span>
                        <span class="font-mono truncate max-w-[15rem]">{{ entry.content }}</span>
                        <span class="opacity-70 whitespace-nowrap">{{ App.formatDnsHistoryTimestamp(entry.savedAt) }}</span>
                      </button>
                    </div>
                    <p v-else class="mt-2 text-[11px] text-slate-400">KV 历史：暂无记录</p>
                  </td>
                  <td class="py-3 px-4">
                    <button v-if="record.editable" type="button" class="px-3 py-1.5 rounded-lg bg-brand-600 text-white text-sm font-medium hover:bg-brand-700 transition disabled:opacity-40 disabled:pointer-events-none" :disabled="!!record._saving || !App.isDnsRecordDirty(record)" @click="App.saveDnsRecord(record.id)">{{ record._saving ? '保存中...' : '保存' }}</button>
                    <span v-else class="text-xs text-slate-400">只读</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div id="dns-empty" class="text-sm text-slate-500 text-center py-10" :class="{ hidden: App.dnsRecords.length > 0 }">{{ App.dnsEmptyText }}</div>

          <div class="mt-auto pt-6 border-t border-slate-200 dark:border-slate-800">
            <div class="ui-radius-card rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm">
              <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                <div>
                  <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">链接</div>
                  <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">实用链接</div>
                </div>
                <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">快捷入口</span>
              </div>
              <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-2">
                <a href="https://cf.090227.xyz/" target="_blank" rel="noopener noreferrer" class="ui-radius-card group inline-flex items-center justify-between gap-2 rounded-2xl border border-slate-200 dark:border-slate-700 bg-white/70 dark:bg-slate-950/40 px-4 py-3 text-slate-700 dark:text-slate-200 hover:bg-brand-50/80 dark:hover:bg-brand-500/10 transition">
                  <span class="text-sm font-semibold">优选域名</span>
                  <i data-lucide="arrow-up-right" class="w-4 h-4 text-brand-600 dark:text-brand-400"></i>
                </a>
                <a href="https://vps789.com/" target="_blank" rel="noopener noreferrer" class="ui-radius-card group inline-flex items-center justify-between gap-2 rounded-2xl border border-slate-200 dark:border-slate-700 bg-white/70 dark:bg-slate-950/40 px-4 py-3 text-slate-700 dark:text-slate-200 hover:bg-brand-50/80 dark:hover:bg-brand-500/10 transition">
                  <span class="text-sm font-semibold">VPS789</span>
                  <i data-lucide="arrow-up-right" class="w-4 h-4 text-brand-600 dark:text-brand-400"></i>
                </a>
                <a href="https://www.wetest.vip/page/cloudflare/address_v4.html" target="_blank" rel="noopener noreferrer" class="ui-radius-card group inline-flex items-center justify-between gap-2 rounded-2xl border border-slate-200 dark:border-slate-700 bg-white/70 dark:bg-slate-950/40 px-4 py-3 text-slate-700 dark:text-slate-200 hover:bg-brand-50/80 dark:hover:bg-brand-500/10 transition">
                  <span class="text-sm font-semibold">WeTest.Vip</span>
                  <i data-lucide="arrow-up-right" class="w-4 h-4 text-brand-600 dark:text-brand-400"></i>
                </a>
                <a href="https://stock.hostmonit.com/CloudFlareYes" target="_blank" rel="noopener noreferrer" class="ui-radius-card group inline-flex items-center justify-between gap-2 rounded-2xl border border-slate-200 dark:border-slate-700 bg-white/70 dark:bg-slate-950/40 px-4 py-3 text-slate-700 dark:text-slate-200 hover:bg-brand-50/80 dark:hover:bg-brand-500/10 transition">
                  <span class="text-sm font-semibold">CloudFlareYes</span>
                  <i data-lucide="arrow-up-right" class="w-4 h-4 text-brand-600 dark:text-brand-400"></i>
                </a>
                <a href="https://ipdb.api.030101.xyz/" target="_blank" rel="noopener noreferrer" class="ui-radius-card group inline-flex items-center justify-between gap-2 rounded-2xl border border-slate-200 dark:border-slate-700 bg-white/70 dark:bg-slate-950/40 px-4 py-3 text-slate-700 dark:text-slate-200 hover:bg-brand-50/80 dark:hover:bg-brand-500/10 transition">
                  <span class="text-sm font-semibold">IPDB API</span>
                  <i data-lucide="arrow-up-right" class="w-4 h-4 text-brand-600 dark:text-brand-400"></i>
                </a>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div id="view-settings" class="view-section max-w-6xl mx-auto space-y-6" :class="{ active: App.currentHash === '#settings' }">
           <div class="settings-view-layout flex flex-col gap-4 md:flex-row md:items-start md:gap-5">
              <div class="md:w-64 md:flex-shrink-0 md:self-start">
                <div class="settings-nav-shell w-full rounded-[24px] border border-slate-200 dark:border-slate-800 bg-slate-50/90 dark:bg-slate-950/70 p-3 md:p-3.5 shadow-sm shadow-slate-200/60 dark:shadow-none">
                  <div class="px-1 pb-2.5 mb-2.5 border-b border-slate-200/80 dark:border-slate-800">
                    <div class="text-[11px] font-semibold tracking-[0.16em] text-slate-400 dark:text-slate-500 uppercase">Settings</div>
                    <div class="text-[13px] font-semibold text-slate-900 dark:text-white mt-1">全局设置导航</div>
                    <p class="mt-1.5 text-[11px] leading-4 text-slate-500 dark:text-slate-400">PC 端左侧分区导航，移动端可横向滑动切换。</p>
                  </div>
                  <div class="flex flex-row gap-1.5 overflow-x-auto whitespace-nowrap md:flex-col md:overflow-visible md:whitespace-normal" role="tablist" aria-label="全局设置导航">
                    <button class="set-tab min-w-[10rem] md:min-w-0 md:w-full flex-shrink-0 text-left px-3 py-2.5 rounded-xl border text-[13px] transition" :class="App.getSettingsTabClass('ui')" @click="App.switchSetTab('ui')" role="tab" aria-controls="set-ui" :aria-selected="App.activeSettingsTab === 'ui'">
                      <span class="block font-semibold">系统 UI</span>
                      <span class="hidden md:block mt-0.5 text-[11px] leading-4 text-slate-500 dark:text-slate-400">主题与基础界面参数</span>
                    </button>
                    <button class="set-tab min-w-[10rem] md:min-w-0 md:w-full flex-shrink-0 text-left px-3 py-2.5 rounded-xl border text-[13px] transition" :class="App.getSettingsTabClass('proxy')" @click="App.switchSetTab('proxy')" role="tab" aria-controls="set-proxy" :aria-selected="App.activeSettingsTab === 'proxy'">
                      <span class="block font-semibold">代理与网络</span>
                      <span class="hidden md:block mt-0.5 text-[11px] leading-4 text-slate-500 dark:text-slate-400">播放稳定性与链路策略</span>
                    </button>
                    <button class="set-tab min-w-[10rem] md:min-w-0 md:w-full flex-shrink-0 text-left px-3 py-2.5 rounded-xl border text-[13px] transition" :class="App.getSettingsTabClass('security')" @click="App.switchSetTab('security')" role="tab" aria-controls="set-security" :aria-selected="App.activeSettingsTab === 'security'">
                      <span class="block font-semibold">缓存与安全</span>
                      <span class="hidden md:block mt-0.5 text-[11px] leading-4 text-slate-500 dark:text-slate-400">访问控制、限速与跨域</span>
                    </button>
                    <button class="set-tab min-w-[10rem] md:min-w-0 md:w-full flex-shrink-0 text-left px-3 py-2.5 rounded-xl border text-[13px] transition" :class="App.getSettingsTabClass('logs')" @click="App.switchSetTab('logs')" role="tab" aria-controls="set-logs" :aria-selected="App.activeSettingsTab === 'logs'">
                      <span class="block font-semibold">日志与监控</span>
                      <span class="hidden md:block mt-0.5 text-[11px] leading-4 text-slate-500 dark:text-slate-400">日志写入、告警与日报</span>
                    </button>
                    <button class="set-tab min-w-[10rem] md:min-w-0 md:w-full flex-shrink-0 text-left px-3 py-2.5 rounded-xl border text-[13px] transition" :class="App.getSettingsTabClass('account')" @click="App.switchSetTab('account')" role="tab" aria-controls="set-account" :aria-selected="App.activeSettingsTab === 'account'">
                      <span class="block font-semibold">账号与备份</span>
                      <span class="hidden md:block mt-0.5 text-[11px] leading-4 text-slate-500 dark:text-slate-400">Cloudflare 联动与恢复保底</span>
                    </button>
                  </div>
                </div>
              </div>
              <div class="flex-1 min-w-0" id="settings-forms" v-scroll-reset="App.settingsScrollResetKey">
              
              <div id="set-ui" v-show="App.activeSettingsTab === 'ui'" class="space-y-4">
                <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 p-5 shadow-sm settings-panel">
                  <div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                    <div class="max-w-2xl">
                      <span class="inline-flex items-center rounded-full border border-indigo-200 bg-indigo-50 px-2.5 py-1 text-[11px] font-semibold tracking-[0.12em] uppercase text-indigo-600 dark:border-indigo-500/30 dark:bg-indigo-500/10 dark:text-indigo-300">UI</span>
                      <h3 class="mt-3 text-lg font-semibold text-slate-900 dark:text-white">UI 基础设置</h3>
                      <p class="text-sm text-slate-600 dark:text-slate-300 mt-2">深浅模式仍然只保存在当前浏览器；这里仅保留与界面结构直接相关的基础参数，不再提供 Dashboard 自动刷新和液态玻璃协调效果。</p>
                    </div>
                    <div class="flex flex-wrap gap-2 md:max-w-[240px]">
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">本地主题</span>
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">纯净面板</span>
                    </div>
                  </div>
                </div>
                <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-950 p-5 shadow-sm settings-block">
                  <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                    <div>
                      <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Radius</div>
                      <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">UI 圆角弧度</div>
                    </div>
                    <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">0-48 px</span>
                  </div>
                  <p class="text-xs text-slate-500 mb-3 ml-6">控制管理界面主要卡片/面板的圆角弧度；设置为 0 可关闭圆角（更接近矩形 UI）。</p>
                  <label class="block text-sm text-slate-500 mb-1 ml-6">圆角弧度</label>
                  <div class="relative w-[calc(100%-1.5rem)] ml-6">
                    <input type="number" min="0" max="48" step="1" id="cfg-ui-radius-px" v-model="App.settingsForm.uiRadiusPx" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-2 dark:text-white" value="24">
                    <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">px</span>
                  </div>
                  <p class="text-xs text-slate-500 ml-6">推荐 16-24；保存后会立即应用到所有管理员界面（仅 UI，不影响代理业务逻辑）。</p>
                </div>
                <div class="flex flex-wrap gap-2">
                  <button @click="App.saveSettings('ui')" class="px-4 py-2 bg-brand-600 hover:bg-brand-700 text-white rounded-xl text-sm transition">保存 UI 设置</button>
                </div>
              </div>
              
              <div id="set-proxy" v-show="App.activeSettingsTab === 'proxy'" class="space-y-4">
                <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-white/90 dark:bg-slate-950/70 p-5 shadow-sm settings-panel">
                  <div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                    <div class="max-w-2xl">
                      <span class="inline-flex items-center rounded-full border border-sky-200 bg-sky-50 px-2.5 py-1 text-[11px] font-semibold tracking-[0.12em] uppercase text-sky-600 dark:border-sky-500/30 dark:bg-sky-500/10 dark:text-sky-300">Network</span>
                      <h3 class="mt-3 text-lg font-semibold text-slate-900 dark:text-white">网络协议与优化</h3>
                      <p class="text-sm text-slate-600 dark:text-slate-300 mt-2">默认仍以 HTTP/1.1 稳定链路为基线，再按需打开预热、直连与回退策略。这里更适合小步调参，不建议一次改很多项。</p>
                    </div>
                    <div class="flex flex-wrap gap-2 md:max-w-[280px]">
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">H1.1 优先</span>
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">预热拦截</span>
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">307 直连</span>
                    </div>
                  </div>
                </div>

                <div class="grid gap-4">
                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Protocol</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">基础协议策略</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">稳定优先</span>
                    </div>
                    <label class="flex items-center text-sm font-medium mb-2 cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-enable-h2" v-model="App.settingsForm.enableH2" class="mr-2 w-4 h-4 rounded"> 允许开启 HTTP/2 (不建议)</label>
                    <p class="text-xs text-slate-500 mb-3 ml-6">适合少数明确支持多路复用的上游；部分视频源在分片、长连接或头部兼容性上反而更容易出现异常。</p>
                    <label class="flex items-center text-sm font-medium mb-2 cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-enable-h3" v-model="App.settingsForm.enableH3" class="mr-2 w-4 h-4 rounded"> 允许开启 HTTP/3 QUIC (仅网络质量稳定时按需开启)</label>
                    <p class="text-xs text-slate-500 mb-3 ml-6">适合网络质量稳定、丢包率低的环境；弱网或运营商链路复杂时，实际稳定性未必优于 HTTP/1.1。</p>
                    <label class="flex items-center text-sm font-medium mb-2 cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-peak-downgrade" v-model="App.settingsForm.peakDowngrade" class="mr-2 w-4 h-4 rounded" checked> 晚高峰 (20:00 - 24:00) 自动降级为 HTTP/1.1 兜底</label>
                    <p class="text-xs text-slate-500 mb-3 ml-6">高峰时段优先稳态传输，减少握手抖动、异常回源和多路复用放大的兼容性问题。</p>
                    <label class="flex items-center text-sm font-medium cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-protocol-fallback" v-model="App.settingsForm.protocolFallback" class="mr-2 w-4 h-4 rounded" checked> 开启协议回退与 403 重试 (剥离报错头重连，缓解视频报错)</label>
                    <p class="text-xs text-slate-500 mt-2 ml-6">当上游返回 403 或握手异常时，自动剥离可疑报错头并切换到更稳的协议后重试一次。</p>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Metadata Pre-warm</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">轻量级元数据预热</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">海报 / 索引</span>
                    </div>
                    <label class="flex items-center text-sm font-medium mb-2 cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-enable-prewarm" v-model="App.settingsForm.enablePrewarm" class="mr-2 w-4 h-4 rounded" checked> 开启轻量级元数据预热</label>
                    <p class="text-xs text-slate-500 mb-3 ml-6">仅预热索引文件、字幕和海报，大幅提升起播感知速度，同时避免 Worker 参与视频字节流的长时间 I/O。</p>
                    <label class="block text-sm text-slate-500 mb-1 ml-6">元数据预热缓存时长</label>
                    <div class="relative w-[calc(100%-1.5rem)] ml-6">
                      <input type="number" min="0" max="3600" step="1" id="cfg-prewarm-ttl" v-model="App.settingsForm.prewarmCacheTtl" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-2 dark:text-white" value="180">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">秒</span>
                    </div>
                    <p class="text-xs text-slate-500 mb-4 ml-6">该 TTL 只作用于 <code>.m3u8</code>、<code>.vtt/.srt</code> 等轻量元数据；海报仍沿用图片缓存策略。检测到 <code>.mp4</code>、<code>.mkv</code>、<code>.ts</code>、<code>.m4s</code> 等视频字节流时，会立即跳过异步预热。</p>
                    <label class="block text-sm text-slate-500 mb-1 ml-6">预热深度</label>
                    <select id="cfg-prewarm-depth" v-model="App.settingsForm.prewarmDepth" @change="App.syncProxySettingsGuardrails()" class="w-[calc(100%-1.5rem)] ml-6 p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-2 dark:text-white">
                      <option value="poster">仅预热海报</option>
                      <option value="poster_manifest">预热海报+索引</option>
                    </select>
                    <p class="text-xs text-slate-500 mb-3 ml-6">“索引”包含播放列表与字幕等轻量元数据，不包含任何视频分片或大文件 Range。</p>
                    <p id="cfg-prewarm-runtime-hint" class="text-xs text-cyan-700 dark:text-cyan-300 mb-3 ml-6">{{ App.proxySettingsGuardrails.prewarmHint }}</p>
                    <div class="ml-6 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-xs leading-5 text-amber-900 dark:border-amber-500/30 dark:bg-amber-500/10 dark:text-amber-100">
                      <strong>⚠️ 架构师建议：</strong>请确保已在 Cloudflare Cache Rules 中开启“忽略查询字符串（Ignore query string）”，这样视频流缓存才能跨用户共享，真正发挥数据面的缓存能力。
                    </div>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Direct</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">资源直连分流</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">静态 / HLS / DASH</span>
                    </div>
                    <label class="flex items-center text-sm font-medium mb-2 cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-direct-static-assets" v-model="App.settingsForm.directStaticAssets" @change="App.syncProxySettingsGuardrails()" class="mr-2 w-4 h-4 rounded"> 静态文件直连</label>
                    <p class="text-xs text-slate-500 mb-3 ml-6">这里现在只对 JS、CSS、字体、source map、webmanifest 这类前端静态文件生效。海报、封面、字幕继续走 Worker 边缘缓存，因为它们走 307 直连通常会多一次跳转并丢掉缓存，反而更慢。</p>
                    <label class="flex items-center text-sm font-medium mb-2 cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-direct-hls-dash" v-model="App.settingsForm.directHlsDash" @change="App.syncProxySettingsGuardrails()" class="mr-2 w-4 h-4 rounded"> HLS / DASH 直连</label>
                    <p class="text-xs text-slate-500">命中 <code>.m3u8</code>、<code>.mpd</code>、<code>.ts</code>、<code>.m4s</code> 等播放列表或分片时，返回 307 让播放器直接回源；这能明显减少 Worker 中继流量。<code>.vtt</code> 字幕轨默认仍走 Worker 缓存，避免 307 多一跳导致双语字幕更慢。</p>
                    <p id="cfg-direct-mode-hint" class="text-xs text-cyan-700 dark:text-cyan-300 mt-3">{{ App.proxySettingsGuardrails.directHint }}</p>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Relay</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">跳转代理与外链规则</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">同源 / 外链</span>
                    </div>
                    <label class="flex items-center text-sm font-medium mb-2 cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-source-same-origin-proxy" v-model="App.settingsForm.sourceSameOriginProxy" class="mr-2 w-4 h-4 rounded" checked> 默认开启：源站和同源跳转代理</label>
                    <p class="text-xs text-slate-500 mb-3">开启时既包含源站 2xx 的 Worker 透明拉流，也包含同源 30x 的继续代理跳转；仅当节点被显式标记为直连，或启用了“静态文件直连 / HLS-DASH 直连”时，源站 2xx 才会改为 307 直连源站。关闭后，同源 30x 直接下发 Location。</p>
                    <label class="flex items-center text-sm font-medium mb-2 cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-force-external-proxy" v-model="App.settingsForm.forceExternalProxy" class="mr-2 w-4 h-4 rounded" checked> 默认开启：强制反代外部链接</label>
                    <p class="text-xs text-slate-500 mb-3">开启后 Worker 会作为中继站拉流并透明转发；除国内网盘/对象存储外默认不缓存，命中 <code>wangpandirect</code> 列表走直连。关闭后外部链接直接下发直连。</p>
                    <p class="text-xs text-slate-500 mb-2">默认已填入内置关键词；请使用英文逗号分隔自定义内容，例如 <code>baidu,alibaba</code>。</p>
                    <label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">wangpandirect 直连黑名单（关键词模糊匹配，英文逗号分隔）</label>
                    <textarea id="cfg-wangpandirect" v-model="App.settingsForm.wangpandirect" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white resize-y" rows="3" placeholder="例如: baidu,alibaba"></textarea>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Node Direct</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">源站直连名单</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">节点级直连</span>
                    </div>
                    <p class="text-xs text-slate-500 mb-3">这里列出现有节点。勾选后，这些节点在“源站和同源跳转代理”开启时，源站 2xx 会直接下发到源站，不再由 Worker 中继；未勾选节点继续由 Worker 透明拉流。</p>
                    <input type="text" id="cfg-direct-node-search" v-model.trim="App.settingsDirectNodeSearch" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-3 dark:text-white" placeholder="搜索节点名称、标签或备注...">
                    <div id="cfg-source-direct-nodes-summary" class="text-xs text-slate-500 mb-2">{{ App.getSourceDirectNodesSummaryText() }}</div>
                    <div id="cfg-source-direct-nodes-list" class="max-h-64 overflow-y-auto rounded-2xl border border-slate-200 dark:border-slate-700 bg-white/80 dark:bg-slate-950/60 p-2 space-y-2 settings-list-shell">
                      <div v-if="!App.nodes.length" class="text-sm text-slate-500 px-3 py-2">暂无可选节点</div>
                      <div v-else-if="!App.getFilteredSourceDirectNodes().length" class="text-sm text-slate-500 px-3 py-2">没有匹配的节点</div>
                      <label v-for="node in App.getFilteredSourceDirectNodes()" :key="node.name" class="flex items-start gap-3 rounded-xl border border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 px-3 py-2 cursor-pointer">
                        <input type="checkbox" class="mt-1 w-4 h-4 rounded" :checked="App.isSourceDirectNodeSelected(node.name)" @change="App.toggleSourceDirectNode(node.name, $event.target.checked)">
                        <div class="min-w-0 flex-1">
                          <div class="text-sm font-medium text-slate-900 dark:text-white truncate">{{ node.displayName || node.name || '未命名节点' }}</div>
                          <div class="text-xs text-slate-500 mt-1 break-all">{{ [node.tag ? ('标签: ' + node.tag) : '', node.remark ? ('备注: ' + node.remark) : ''].filter(Boolean).join('  ·  ') || '无标签 / 备注' }}</div>
                        </div>
                      </label>
                    </div>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Probe</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">健康检查探测</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">1000-180000 ms</span>
                    </div>
                    <label class="block text-sm text-slate-500 mb-1">Ping 超时时间</label>
                    <div class="relative">
                      <input type="number" min="1000" max="180000" step="500" id="cfg-ping-timeout" v-model="App.settingsForm.pingTimeout" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-2 dark:text-white" value="5000">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">ms</span>
                    </div>
                    <p class="text-xs text-slate-500 mb-3">系统会限制在 1000 到 180000 毫秒之间，避免探测等待时间过长拖住后台操作。</p>
                    <label class="block text-sm text-slate-500 mb-1">Ping 缓存时间</label>
                    <div class="relative">
                      <input type="number" min="0" max="1440" step="1" id="cfg-ping-cache-minutes" v-model="App.settingsForm.pingCacheMinutes" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-2 dark:text-white" value="10">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">分钟</span>
                    </div>
                    <p class="text-xs text-slate-500">缓存只用于自动复用历史测速结果；用户手动触发单点测速、节点测速或全局 Ping 时会直接重测并覆盖旧值。</p>
                    <label class="flex items-start gap-3 text-sm font-medium cursor-pointer text-slate-900 dark:text-white mt-4">
                      <input type="checkbox" id="cfg-node-panel-ping-auto-sort" v-model="App.settingsForm.nodePanelPingAutoSort" class="mt-0.5 w-4 h-4 rounded">
                      <span>节点面板一键测速后自动按延迟排序并切换到最低延迟线路</span>
                    </label>
                    <p class="text-xs text-slate-500 mt-2">默认关闭。仅影响“新建节点 / 编辑节点”面板的一键测试延迟；全局 Ping 与节点卡片 Ping 只测试当前启用线路，不自动排序。</p>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Upstream</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">上游请求防挂死保护</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">最多 3 次重试</span>
                    </div>
                    <label class="block text-sm text-slate-500 mb-1">上游握手超时</label>
                    <div class="relative">
                      <input type="number" min="0" max="180000" step="500" id="cfg-upstream-timeout-ms" v-model="App.settingsForm.upstreamTimeoutMs" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-2 dark:text-white" value="0">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">ms</span>
                    </div>
                    <p class="text-xs text-slate-500 mb-3">系统会限制在 0 到 180000 毫秒之间，避免把超时配置得过大导致失败请求长期占用连接。</p>
                    <label class="block text-sm text-slate-500 mb-1">额外重试轮次（仅 GET / HEAD 等安全请求）</label>
                    <div class="relative">
                      <input type="number" min="0" max="3" step="1" id="cfg-upstream-retry-attempts" v-model="App.settingsForm.upstreamRetryAttempts" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-2 dark:text-white" value="0">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">次</span>
                    </div>
                    <p class="text-xs text-slate-500">每一轮都会重新遍历节点目标地址与可重试状态码。带流式请求体的非幂等请求不会启用额外重试，避免副作用放大；这里上限固定为 3，防止重试过多额外消耗 Worker 子请求预算。</p>
                  </div>
                </div>

                <div class="flex flex-wrap gap-2">
                  <button @click="App.applyRecommendedSettings('proxy')" class="px-4 py-2 border border-emerald-200 text-emerald-600 rounded-xl text-sm transition hover:bg-emerald-50 dark:border-emerald-900/30 dark:text-emerald-400 dark:hover:bg-emerald-900/20">恢复推荐值</button>
                  <button @click="App.saveSettings('proxy')" class="px-4 py-2 bg-brand-600 hover:bg-brand-700 text-white rounded-xl text-sm transition">保存代理网络</button>
                </div>
              </div>
              
              <div id="set-security" v-show="App.activeSettingsTab === 'security'" class="space-y-4">
                <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-white/90 dark:bg-slate-950/70 p-5 shadow-sm settings-panel">
                  <div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                    <div class="max-w-2xl">
                      <span class="inline-flex items-center rounded-full border border-amber-200 bg-amber-50 px-2.5 py-1 text-[11px] font-semibold tracking-[0.12em] uppercase text-amber-600 dark:border-amber-500/30 dark:bg-amber-500/10 dark:text-amber-300">Security</span>
                      <h3 class="mt-3 text-lg font-semibold text-slate-900 dark:text-white">安全防火墙与缓存引擎</h3>
                      <p class="text-sm text-slate-600 dark:text-slate-300 mt-2">这一栏主要决定“谁可以访问”和“图片等静态资源缓存多久”。如果你不确定某条限制会不会误伤正常用户，建议先留空或保持默认值。</p>
                    </div>
                    <div class="flex flex-wrap gap-2 md:max-w-[280px]">
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">Geo / IP</span>
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">海报缓存</span>
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">CORS</span>
                    </div>
                  </div>
                </div>

                <div class="grid gap-4">
                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block">
                    <div class="flex items-start justify-between gap-3 mb-4 pb-3 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Firewall</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">访问控制与限速</div>
                        <p class="text-xs text-slate-500 mt-2">先决定允许谁进来，再决定异常请求多快被压住。</p>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white text-slate-500 border border-slate-200 px-2.5 py-1 text-[10px] font-semibold dark:bg-slate-900 dark:text-slate-300 dark:border-slate-700">Geo + IP + Rate</span>
                    </div>
                    <div class="grid gap-4">
                      <div>
                        <label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">国家/地区访问模式</label>
                        <p class="text-xs text-slate-500 mb-2">在白名单模式和黑名单模式之间二选一，统一使用同一份国家/地区列表，避免同时填两边造成规则冲突。</p>
                        <select id="cfg-geo-mode" v-model="App.settingsForm.geoMode" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white">
                          <option value="allowlist">白名单模式</option>
                          <option value="blocklist">黑名单模式</option>
                        </select>
                      </div>
                      <div>
                        <label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">国家/地区名单 (逗号分隔，如: CN,HK)</label>
                        <p class="text-xs text-slate-500 mb-2">{{ App.settingsForm.geoMode === 'blocklist' ? '当前为黑名单模式：命中的国家/地区会被直接拦截。' : '当前为白名单模式：只有命中的国家/地区允许访问；留空则等同于关闭 Geo 限制。' }}</p>
                        <input type="text" id="cfg-geo-regions" v-model="App.settingsForm.geoRegions" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" :placeholder="App.settingsForm.geoMode === 'blocklist' ? '例如: US,SG' : '例如: CN,HK'">
                      </div>
                      <div class="md:col-span-2">
                        <label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">IP 黑名单 (逗号分隔)</label>
                        <p class="text-xs text-slate-500 mb-2">这里屏蔽的是访问者的公网 IP；命中后会直接拒绝该用户/设备的请求，适合封禁恶意爬虫、攻击源或异常账号。</p>
                        <textarea id="cfg-ip-black" v-model="App.settingsForm.ipBlacklist" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white resize-y" rows="2"></textarea>
                      </div>
                      <div class="md:col-span-2">
                        <label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">全局单 IP 限速</label>
                        <p class="text-xs text-slate-500 mb-2">对单个访客源 IP 生效；超过阈值后可快速压制刷接口、扫库和异常爆发流量。</p>
                        <div class="relative">
                          <input type="number" id="cfg-rate-limit" v-model="App.settingsForm.rateLimitRpm" class="w-full p-2 pr-16 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" placeholder="如: 600">
                          <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">次/分</span>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-sky-500 dark:text-sky-300">Image Cache</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">图片缓存策略</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">0-365 天</span>
                    </div>
                    <p class="text-xs text-slate-500 mt-2 mb-3">主要影响海报、封面等轻资源，缓存得当能显著降低后台浏览时的重复回源。</p>
                    <label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">图片海报缓存时长</label>
                    <div class="relative">
                      <input type="number" min="0" max="365" id="cfg-cache-ttl" v-model="App.settingsForm.cacheTtlImages" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" value="30">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">天</span>
                    </div>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-emerald-500 dark:text-emerald-300">CORS</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">浏览器跨域策略</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">留空为 *</span>
                    </div>
                    <p class="text-xs text-slate-500 mt-2 mb-3">用于限制哪些网页前端可以在浏览器里跨域调用本 Worker API；它主要影响浏览器环境，不影响服务器到服务器的直连请求。</p>
                    <label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">CORS 跨域白名单 (留空为 *，如 https://emby.com)</label>
                    <input type="text" id="cfg-cors" v-model="App.settingsForm.corsOrigins" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white">
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-white/70 dark:bg-slate-900/50 p-5 shadow-sm settings-block">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Checklist</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">建议顺序</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">先通后收紧</span>
                    </div>
                    <div class="text-xs leading-6 text-slate-500">
                      1. 先留空白名单与 CORS，确保基础访问正常。<br>
                      2. 再逐步补充 Geo / IP 黑名单，观察是否误伤。<br>
                      3. 最后再收紧限速和缓存天数，避免一次改太多难排错。
                    </div>
                  </div>
                </div>

                <div class="flex flex-wrap gap-2">
                  <button @click="App.saveSettings('security')" class="px-4 py-2 bg-brand-600 hover:bg-brand-700 text-white rounded-xl text-sm transition">保存安全防护</button>
                </div>
              </div>
              
              <div id="set-logs" v-show="App.activeSettingsTab === 'logs'" class="space-y-4">
                <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-white/90 dark:bg-slate-950/70 p-5 shadow-sm settings-panel">
                  <div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                    <div class="max-w-2xl">
                      <span class="inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-[11px] font-semibold tracking-[0.12em] uppercase text-emerald-600 dark:border-emerald-500/30 dark:bg-emerald-500/10 dark:text-emerald-300">Ops</span>
                      <h3 class="mt-3 text-lg font-semibold text-slate-900 dark:text-white">监控与日志配置</h3>
                      <p class="text-sm text-slate-600 dark:text-slate-300 mt-2">这一栏决定日志如何写入、多久保留，以及 Telegram 如何通知你。小白通常只需要关心“日志保存天数”和“测试通知能不能收到”。</p>
                    </div>
                    <div class="flex flex-wrap gap-2 md:max-w-[280px]">
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">D1 写入</span>
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">Cron</span>
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">Telegram</span>
                    </div>
                  </div>
                </div>
                <div class="grid gap-4">
                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Storage</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">日志队列与落盘</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">Cloudflare 上限已内置</span>
                    </div>
                    <div class="grid gap-3">
                      <div>
                        <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">日志保存</label>
                        <div class="relative">
                          <input type="number" min="1" max="365" step="1" id="cfg-log-days" v-model="App.settingsForm.logRetentionDays" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" value="7">
                          <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">天</span>
                        </div>
                      </div>
                      <div>
                        <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">日志写入延迟</label>
                        <div class="relative">
                          <input type="number" min="0" max="1440" step="0.5" id="cfg-log-delay" v-model="App.settingsForm.logWriteDelayMinutes" class="w-full p-2 pr-16 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" value="20">
                          <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">分钟</span>
                        </div>
                      </div>
                      <div>
                        <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">提前写入阈值</label>
                        <div class="relative">
                          <input type="number" min="1" max="5000" step="1" id="cfg-log-flush-count" v-model="App.settingsForm.logFlushCountThreshold" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" value="50">
                          <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">条</span>
                        </div>
                      </div>
                      <div>
                        <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">D1 切片大小</label>
                        <div class="relative">
                          <input type="number" min="1" max="100" step="1" id="cfg-log-batch-size" v-model="App.settingsForm.logBatchChunkSize" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" value="50">
                          <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">条</span>
                        </div>
                      </div>
                      <div>
                        <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">D1 重试次数</label>
                        <div class="relative">
                          <input type="number" min="0" max="5" step="1" id="cfg-log-retry-count" v-model="App.settingsForm.logBatchRetryCount" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" value="2">
                          <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">次</span>
                        </div>
                      </div>
                      <div>
                        <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">重试退避</label>
                        <div class="relative">
                          <input type="number" min="0" max="5000" step="25" id="cfg-log-retry-backoff" v-model="App.settingsForm.logBatchRetryBackoffMs" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" value="75">
                          <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">ms</span>
                        </div>
                      </div>
                      <div class="md:col-span-2">
                        <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">定时任务租约时长</label>
                        <div class="relative">
                          <input type="number" min="30000" max="900000" step="1000" id="cfg-scheduled-lease-ms" v-model="App.settingsForm.scheduledLeaseMs" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" value="300000">
                          <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">ms</span>
                        </div>
                      </div>
                    </div>
                    <p class="text-xs text-slate-500 mt-3">内存日志队列满足“达到延迟分钟”或“累计达到条数阈值”任一条件即写入 D1。Cloudflare 官方文档说明 Cron Trigger 单次执行最长 15 分钟，因此租约上限固定为 900000 毫秒；D1 单批切片也限制为最多 100 条，避免单次批量过大。</p>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-emerald-500 dark:text-emerald-300">Recommended</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">推荐生产值</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">生产环境</span>
                    </div>
                    <div class="text-xs leading-6 text-slate-600 dark:text-slate-300">
                      日志保存天数：7 到 14 天<br>
                      写入延迟：5 到 20 分钟<br>
                      提前写入阈值：50 到 200 条<br>
                      单批切片：50 到 100 条<br>
                      重试次数：1 到 2 次，退避 75 到 200 毫秒<br>
                      定时任务租约：300000 到 600000 毫秒
                    </div>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-amber-500 dark:text-amber-300">Tuning</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">异常调优指引</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">逐项小步调</span>
                    </div>
                    <div class="text-xs leading-6 text-slate-600 dark:text-slate-300">
                      D1 写入失败增多：先提高重试次数或退避，再观察 lastFlushRetryCount。<br>
                      队列长期堆积：降低写入延迟或下调提前写入阈值。<br>
                      单次刷盘过慢：降低单批切片大小。<br>
                      定时任务频繁重入：适当增大租约时长，但不要超过实际任务耗时太多。<br>
                      只想快速止血：优先保留默认值，再逐项小步调整。
                    </div>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Telegram</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">每日报表与告警机器人</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">先测连通</span>
                    </div>
                    <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">Telegram Bot Token</label>
                    <input type="text" id="cfg-tg-token" v-model="App.settingsForm.tgBotToken" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-3 dark:text-white" placeholder="如: 123456789:ABCdefGHIjklMNOpqrSTUvwxYZ">
                    <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">Telegram Chat ID (接收人ID)</label>
                    <input type="text" id="cfg-tg-chatid" v-model="App.settingsForm.tgChatId" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" placeholder="如: 123456789">
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Alert</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">Telegram 异常告警阈值</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">1-1440 分钟</span>
                    </div>
                    <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">日志丢弃批次阈值</label>
                    <div class="relative">
                      <input type="number" min="0" max="5000" step="1" id="cfg-tg-alert-drop-threshold" v-model="App.settingsForm.tgAlertDroppedBatchThreshold" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-3 dark:text-white" value="0">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">批</span>
                    </div>
                    <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">D1 写入重试阈值</label>
                    <div class="relative">
                      <input type="number" min="0" max="10" step="1" id="cfg-tg-alert-retry-threshold" v-model="App.settingsForm.tgAlertFlushRetryThreshold" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-3 dark:text-white" value="0">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">次</span>
                    </div>
                    <label class="flex items-center text-sm font-medium mb-2 cursor-pointer text-slate-900 dark:text-white"><input type="checkbox" id="cfg-tg-alert-scheduled-failure" v-model="App.settingsForm.tgAlertOnScheduledFailure" class="mr-2 w-4 h-4 rounded"> 定时任务进入 failed / partial_failure 时告警</label>
                    <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">同类告警冷却时间</label>
                    <div class="relative">
                      <input type="number" min="1" max="1440" step="1" id="cfg-tg-alert-cooldown-minutes" v-model="App.settingsForm.tgAlertCooldownMinutes" class="w-full p-2 pr-16 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-2 dark:text-white" value="30">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">分钟</span>
                    </div>
                    <p class="text-xs text-slate-500">告警由定时任务在后台判断并发送。建议先完成 Bot Token 与 Chat ID 测试，再启用阈值；系统会把冷却时间限制在 1 到 1440 分钟之间。</p>
                  </div>
                </div>

                <div class="flex flex-wrap gap-2">
                    <button @click="App.applyRecommendedSettings('logs')" class="px-4 py-2 border border-emerald-200 text-emerald-600 rounded-xl text-sm transition hover:bg-emerald-50 dark:border-emerald-900/30 dark:text-emerald-400 dark:hover:bg-emerald-900/20">恢复推荐值</button>
                    <button @click="App.saveSettings('logs')" class="px-4 py-2 bg-brand-600 hover:bg-brand-700 text-white rounded-xl text-sm transition">保存监控设置</button>
                    <button @click="App.testTelegram()" class="px-4 py-2 border border-blue-200 text-blue-600 rounded-xl text-sm transition hover:bg-blue-50 dark:border-blue-900/30 dark:text-blue-400 dark:hover:bg-blue-900/20 flex items-center justify-center"><i data-lucide="send" class="w-4 h-4 mr-1"></i> 发送测试通知</button>
                    <button @click="App.sendDailyReport()" class="px-4 py-2 border border-emerald-200 text-emerald-600 rounded-xl text-sm transition hover:bg-emerald-50 dark:border-emerald-900/30 dark:text-emerald-400 dark:hover:bg-emerald-900/20 flex items-center justify-center"><i data-lucide="file-bar-chart" class="w-4 h-4 mr-1"></i> 手动发送日报</button>
                </div>
              </div>
              
              <div id="set-account" v-show="App.activeSettingsTab === 'account'" class="space-y-4">
                <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-white/90 dark:bg-slate-950/70 p-5 shadow-sm settings-panel">
                  <div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                    <div class="max-w-2xl">
                      <span class="inline-flex items-center rounded-full border border-sky-200 bg-sky-50 px-2.5 py-1 text-[11px] font-semibold tracking-[0.12em] uppercase text-sky-600 dark:border-sky-500/30 dark:bg-sky-500/10 dark:text-sky-300">Account</span>
                      <h3 class="mt-3 text-lg font-semibold text-slate-900 dark:text-white">系统账号与安全</h3>
                      <p class="text-sm text-slate-600 dark:text-slate-300 mt-2">这一栏主要管理后台登录有效期、Cloudflare 联动参数，以及备份、导入和快照恢复。准备做大改动前，建议先导出一份完整备份。</p>
                    </div>
                    <div class="flex flex-wrap gap-2 md:max-w-[280px]">
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">后台登录</span>
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">Cloudflare</span>
                      <span class="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">快照恢复</span>
                    </div>
                  </div>
                </div>

                <div class="grid gap-4">
                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Login</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">后台登录有效期</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">按天计算</span>
                    </div>
                    <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">免密登录有效期</label>
                    <div class="relative">
                    <input type="number" id="cfg-jwt-days" v-model="App.settingsForm.jwtExpiryDays" class="w-full p-2 pr-12 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none dark:text-white" value="30">
                      <span class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">天</span>
                    </div>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Cloudflare</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">Cloudflare 联动</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">可选增强</span>
                    </div>
                    <p class="text-sm text-slate-500 mb-4">这些参数主要用于仪表盘增强统计和一键清理缓存。没填时基础代理仍可用，只是部分联动能力会缺失。</p>
                    <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">Cloudflare 账号 ID</label>
                    <input type="text" id="cfg-cf-account" v-model="App.settingsForm.cfAccountId" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-3 dark:text-white">
                    <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">Cloudflare Zone ID (区域ID，用于面板数据与清理缓存)</label>
                    <input type="text" id="cfg-cf-zone" v-model="App.settingsForm.cfZoneId" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-3 dark:text-white">
                    <label class="block text-sm font-semibold tracking-[0.01em] text-slate-800 dark:text-slate-200 mb-1">Cloudflare API 令牌</label>
                    <input type="text" id="cfg-cf-token" v-model="App.settingsForm.cfApiToken" class="w-full p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 outline-none mb-4 dark:text-white">
                    <div class="flex flex-wrap gap-2">
                      <button @click="App.saveSettings('account')" class="px-4 py-2 bg-brand-600 hover:bg-brand-700 text-white rounded-xl text-sm transition">保存账号设置</button>
                      <button @click="App.purgeCache()" class="px-4 py-2 border border-red-200 text-red-600 rounded-xl text-sm transition hover:bg-red-50 dark:border-red-900/30 dark:hover:bg-red-900/20">一键清理全站缓存 (Purge)</button>
                    </div>
                  </div>
                </div>

                <div class="grid gap-4">
                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Settings Only</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">全局设置专用迁移</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">不含节点</span>
                    </div>
                    <p class="text-sm text-slate-500 mb-4">只导出 / 导入 settings，不包含节点清单。适合多环境同步代理、监控、账号与 Dashboard 策略。</p>
                    <div class="flex gap-4 flex-wrap">
                      <label class="px-4 py-2 bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-200 rounded-xl text-sm transition font-medium"><i data-lucide="upload-cloud" class="w-4 h-4 inline mr-1"></i> 导入全局设置<input type="file" id="import-settings-file" class="hidden" accept=".json" @change="App.importSettings"></label>
                      <button @click="App.exportSettings()" class="px-4 py-2 bg-brand-600 hover:bg-brand-700 text-white rounded-xl text-sm transition font-medium"><i data-lucide="download-cloud" class="w-4 h-4 inline mr-1"></i> 导出全局设置</button>
                    </div>
                  </div>

                  <div class="rounded-3xl border border-amber-200 dark:border-amber-900/40 bg-amber-50/70 dark:bg-amber-950/20 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-amber-200/80 dark:border-amber-900/40">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-amber-500 dark:text-amber-400">Advanced Repair</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">一键整理 KV 数据</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-amber-700 border border-amber-200 dark:bg-slate-900 dark:border-amber-900/40 dark:text-amber-300">谨慎使用</span>
                    </div>
                    <p class="text-sm text-slate-500 mb-4">用于旧版本升级后出现 <code>sys:theme</code> 脏值、<code>sys:nodes_index</code> 错乱或遗留 Cloudflare 仪表盘缓存键时的整理修复。不会删除 <code>node:*</code> 节点实体；定时任务也会在上次整理成功 1 小时后自动兜底执行一次。</p>
                    <div class="flex gap-4 flex-wrap">
                      <button @click="App.tidyKvData()" class="px-4 py-2 border border-amber-300 text-amber-700 rounded-xl text-sm transition hover:bg-amber-100 dark:border-amber-900/40 dark:text-amber-300 dark:hover:bg-amber-900/20"><i data-lucide="database" class="w-4 h-4 inline mr-1"></i> 一键整理 KV 数据</button>
                    </div>
                  </div>

                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block h-full">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Full Backup</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">备份与恢复 (全量 KV 数据)</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">节点 + 设置</span>
                    </div>
                    <p class="text-sm text-slate-500 mb-4">导出或导入系统内的所有节点以及全局设置数据（单文件）。</p>
                    <div class="flex gap-4 flex-wrap">
                      <label class="px-4 py-2 bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-200 rounded-xl text-sm transition font-medium"><i data-lucide="upload" class="w-4 h-4 inline mr-1"></i> 导入完整备份<input type="file" id="import-full-file" class="hidden" accept=".json" @change="App.importFull"></label>
                      <button @click="App.exportFull()" class="px-4 py-2 bg-brand-600 hover:bg-brand-700 text-white rounded-xl text-sm transition font-medium"><i data-lucide="download" class="w-4 h-4 inline mr-1"></i> 导出完整备份</button>
                    </div>
                  </div>
                  
                  <div class="rounded-3xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-5 shadow-sm settings-block">
                    <div class="flex items-center justify-between gap-3 pb-3 mb-4 border-b border-slate-200/80 dark:border-slate-800">
                      <div>
                        <div class="text-xs font-semibold tracking-[0.12em] uppercase text-slate-400 dark:text-slate-500">Snapshot</div>
                        <div class="text-base font-semibold text-slate-900 dark:text-white mt-1">设置变更快照</div>
                      </div>
                      <span class="inline-flex items-center rounded-full bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-500 border border-slate-200 dark:bg-slate-900 dark:border-slate-700 dark:text-slate-300">最多保留 5 个</span>
                    </div>
                    <p class="text-sm text-slate-500 mb-3">系统会保留最近 5 个全局设置变更快照。恢复快照时，会先把当前配置再记一份快照，确保你始终有回退余地。</p>
                    <div class="flex gap-2 mb-4">
                      <button @click="App.loadConfigSnapshots()" class="px-4 py-2 border border-slate-200 dark:border-slate-700 text-slate-700 dark:text-slate-200 rounded-xl text-sm transition hover:bg-slate-50 dark:hover:bg-slate-800"><i data-lucide="refresh-cw" class="w-4 h-4 inline mr-1"></i> 刷新快照</button>
                      <button @click="App.clearConfigSnapshots()" class="px-4 py-2 border border-red-200 text-red-600 rounded-xl text-sm transition hover:bg-red-50 dark:border-red-900/30 dark:text-red-400 dark:hover:bg-red-900/20"><i data-lucide="trash-2" class="w-4 h-4 inline mr-1"></i> 清理快照</button>
                    </div>
                    <div id="cfg-snapshots-list" v-auto-animate class="space-y-3">
                      <div v-if="!App.configSnapshots.length" class="rounded-2xl border border-dashed border-slate-200 dark:border-slate-700 p-4 text-sm text-slate-500">暂无设置快照。保存、导入或恢复全局设置后，这里会出现最近的历史版本。</div>
                      <div v-for="snapshot in App.configSnapshots" :key="snapshot.id" class="rounded-2xl border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/40 p-4">
                        <div class="flex flex-col md:flex-row md:items-start md:justify-between gap-3">
                          <div class="min-w-0">
                            <div class="text-sm font-semibold text-slate-900 dark:text-white break-all">{{ App.formatSnapshotReason(snapshot) }}</div>
                            <div class="text-xs text-slate-500 mt-1">创建时间：{{ App.formatLocalDateTime(snapshot.createdAt) }}</div>
                            <div class="text-xs text-slate-500 mt-1">变更字段：{{ App.getConfigSnapshotChangedKeysText(snapshot) }}</div>
                          </div>
                          <button @click="App.restoreConfigSnapshot(snapshot.id)" class="px-3 py-2 border border-brand-200 text-brand-600 rounded-xl text-sm transition hover:bg-brand-50 dark:border-brand-900/30 dark:text-brand-400 dark:hover:bg-brand-900/20 whitespace-nowrap">恢复此快照</button>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              
              </div>
           </div>
      </div>

    </div>
  </main>

  <dialog id="node-modal" v-dialog-visible="App.nodeModalOpen" v-lucide-icons @cancel.prevent="App.handleNodeModalCancel" @close="App.handleNodeModalNativeClose" class="backdrop:bg-slate-950/60 bg-transparent w-11/12 md:w-full max-w-6xl m-auto p-0">
    <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-3xl p-6 shadow-2xl">
      <h2 class="text-xl font-bold mb-4 text-slate-900 dark:text-white" id="node-modal-title">{{ App.nodeModalForm.originalName ? '编辑节点' : '新建节点' }}</h2>
	     <form @submit.prevent="App.saveNode" class="space-y-4 max-h-[calc(80vh-env(safe-area-inset-bottom)-env(safe-area-inset-top))] overflow-y-auto pb-[env(safe-area-inset-bottom)] pl-[env(safe-area-inset-left)] pr-[max(0.5rem,env(safe-area-inset-right))]">
	        <input type="hidden" id="form-original-name" :value="App.nodeModalForm.originalName">
	        <input type="hidden" id="form-active-line-id" :value="App.nodeModalForm.activeLineId">
	        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
	          <div><label class="block text-sm text-slate-500 mb-1">节点名称</label><input type="text" id="form-display-name" v-model="App.nodeModalForm.displayName" @input="App.handleNodeModalDisplayNameInput()" class="w-full px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white" required></div>
	          <div><label class="block text-sm text-slate-500 mb-1">节点路径</label><input type="text" id="form-name" v-model="App.nodeModalForm.name" @input="App.handleNodeModalPathInput()" class="w-full px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white" placeholder="不修改默认同左侧"></div>
	        </div>
	        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
		          <div><label class="block text-sm text-slate-500 mb-1">标签</label><div class="flex gap-2"><input type="text" id="form-tag" v-model="App.nodeModalForm.tag" class="flex-1 min-w-0 px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white"><select id="form-tag-color" v-model="App.nodeModalForm.tagColor" class="w-28 px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white"><option value="amber">琥珀</option><option value="emerald">翠绿</option><option value="sky">天蓝</option><option value="violet">紫</option><option value="rose">红</option><option value="slate">灰</option></select></div></div>
		          <div><label class="block text-sm text-slate-500 mb-1">备注</label><input type="text" id="form-remark" v-model="App.nodeModalForm.remark" class="w-full px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white"></div>
		        </div>
	        
	        <div><label class="block text-sm text-slate-500 mb-1">访问鉴权 (Secret, 可留空)</label><input type="text" id="form-secret" v-model="App.nodeModalForm.secret" class="w-full px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white"></div>
	        
	        <div class="rounded-2xl border border-slate-200 dark:border-slate-800 bg-slate-50/80 dark:bg-slate-950/50 p-4">
	          <div class="flex items-center justify-between gap-3 mb-3">
	            <div>
	              <label class="block text-sm text-slate-500">线路列表</label>
		              <p class="text-xs text-slate-400 mt-1">支持单节点多线路、手动启用、桌面端整行拖拽排序和一键延迟测试；是否自动排序可在全局设置中控制。</p>
	            </div>
	            <div class="flex items-center gap-2">
	              <button type="button" @click="App.pingAllNodeLinesInModal()" :disabled="App.nodeModalPingAllPending" class="px-3 py-2 rounded-xl border border-emerald-200 text-emerald-600 hover:bg-emerald-50 dark:border-emerald-900/30 dark:text-emerald-400 dark:hover:bg-emerald-900/20 text-sm font-medium transition disabled:opacity-60 disabled:cursor-not-allowed">{{ App.nodeModalPingAllText }}</button>
	              <button type="button" @click="App.addNodeLine()" class="px-3 py-2 rounded-xl bg-brand-600 hover:bg-brand-700 text-white text-sm font-medium transition">+ 添加线路</button>
	            </div>
	          </div>
	          <div class="hidden md:grid md:grid-cols-[88px_1.15fr_2.1fr_92px_164px] gap-3 px-3 pb-2 text-[11px] font-semibold tracking-[0.1em] uppercase text-slate-400">
	            <span>启用</span>
	            <span>线路名称</span>
	            <span>目标源站</span>
	            <span>延迟</span>
	            <span>拖拽 / 删除</span>
	          </div>
	          <div id="node-lines-container" v-node-lines-drag="{ app: App }" class="space-y-3">
	            <div v-for="(line, index) in App.nodeModalLines" :key="line.id" :class="App.getNodeModalLineRowClass(line.id)" :draggable="App.isDesktopNodeLineDragEnabled()" data-node-line-row="1" :data-line-id="line.id">
	              <div class="md:hidden flex items-center justify-between gap-3 mb-3">
	                <div class="text-xs font-semibold tracking-[0.1em] uppercase text-slate-400">线路 {{ index + 1 }}</div>
	                <span class="inline-flex items-center rounded-full bg-slate-100 dark:bg-slate-800 px-2.5 py-1 text-[11px] font-medium text-slate-500 dark:text-slate-300">{{ line.name || App.buildDefaultLineName(index) }}</span>
	              </div>
	              <div class="grid gap-3 md:grid-cols-[88px_1.15fr_2.1fr_92px_164px] md:items-center">
	                <label data-node-line-interactive="1" class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
	                  <input data-node-line-interactive="1" type="radio" name="node-active-line" class="w-4 h-4" :value="line.id" v-model="App.nodeModalForm.activeLineId">
	                  <span>启用</span>
	                </label>
	                <input data-node-line-interactive="1" type="text" v-model="line.name" :placeholder="App.buildDefaultLineName(index)" class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white">
	                <input data-node-line-interactive="1" type="url" v-model="line.target" placeholder="https://emby.example.com" class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white">
	                <div class="text-sm font-medium text-slate-500 dark:text-slate-300" :title="line.latencyUpdatedAt ? ('最近测速：' + App.formatLocalDateTime(line.latencyUpdatedAt)) : '尚未测速'">{{ App.formatLatency(line.latencyMs) }}</div>
	                <div data-node-line-interactive="1" class="flex items-center gap-2">
	                  <button type="button" title="整行可拖拽排序" disabled class="px-2.5 py-2 rounded-xl border border-slate-200 dark:border-slate-700 text-slate-500 hover:bg-slate-50 dark:hover:bg-slate-800 transition"><i data-lucide="grip-vertical" class="w-4 h-4"></i></button>
	                  <button type="button" :disabled="index === 0" @click="App.moveNodeLine(line.id, -1)" class="px-2.5 py-2 rounded-xl border border-slate-200 dark:border-slate-700 text-slate-500 hover:bg-slate-50 dark:hover:bg-slate-800 transition disabled:opacity-40"><i data-lucide="arrow-up" class="w-4 h-4"></i></button>
	                  <button type="button" :disabled="index === App.nodeModalLines.length - 1" @click="App.moveNodeLine(line.id, 1)" class="px-2.5 py-2 rounded-xl border border-slate-200 dark:border-slate-700 text-slate-500 hover:bg-slate-50 dark:hover:bg-slate-800 transition disabled:opacity-40"><i data-lucide="arrow-down" class="w-4 h-4"></i></button>
	                  <button type="button" :disabled="App.nodeModalLines.length <= 1" @click="App.removeNodeLine(line.id)" class="px-2.5 py-2 rounded-xl border border-red-100 dark:border-red-900/30 text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 transition"><i data-lucide="trash-2" class="w-4 h-4"></i></button>
	                </div>
	              </div>
	            </div>
	          </div>
	        </div>
	        
	        <div class="p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-100 dark:border-slate-800">
	          <label class="block text-sm font-medium mb-2 text-slate-900 dark:text-white">自定义请求头 (覆盖或新增)</label>
          <div id="headers-container" class="space-y-2 mb-3">
            <div v-for="header in App.nodeModalForm.headers" :key="header.id" class="flex gap-2 items-center">
              <input type="text" v-model="header.key" placeholder="Name (e.g. User-Agent)" class="header-key flex-1 min-w-0 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm font-mono text-slate-900 dark:text-white">
              <input type="text" v-model="header.value" placeholder="Value" class="header-val flex-1 min-w-0 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm font-mono text-slate-900 dark:text-white">
              <button type="button" @click="App.removeNodeHeaderRow(header.id)" class="text-red-500 p-1.5 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-lg transition"><i data-lucide="x" class="w-4 h-4"></i></button>
            </div>
          </div>
          <button type="button" @click="App.addHeaderRow()" class="text-xs font-medium text-brand-600 hover:text-brand-700 bg-brand-50 dark:bg-brand-500/10 dark:text-brand-400 px-3 py-1.5 rounded-lg transition">+ 添加请求头</button>
        </div>

        <div class="flex gap-3 mt-6 sticky bottom-0 bg-white dark:bg-slate-900 py-3 border-t border-slate-100 dark:border-slate-800 z-10 shadow-[0_-10px_15px_-3px_rgba(0,0,0,0.05)] dark:shadow-none">
           <button type="button" @click="App.closeNodeModal()" class="flex-1 py-2.5 rounded-xl border border-slate-200 dark:border-slate-700 text-sm font-medium hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-900 dark:text-white transition shadow-sm">取消</button>
           <button type="submit" :disabled="App.nodeModalSubmitting" class="flex-1 py-2.5 rounded-xl bg-brand-600 hover:bg-brand-700 text-white text-sm font-medium transition shadow-sm disabled:opacity-60 disabled:cursor-not-allowed">{{ App.nodeModalSubmitting ? '保存中...' : '保存' }}</button>
        </div>
      </form>
    </div>
  </dialog>

  <div v-if="App.toastState.visible" class="fixed top-4 right-4 z-[90] w-[min(24rem,calc(100vw-2rem))]">
    <div class="rounded-2xl border shadow-2xl backdrop-blur-sm px-4 py-3 text-sm break-words" :class="App.getToastToneClass(App.toastState.tone)">
      {{ App.toastState.message }}
    </div>
  </div>

  <div v-if="App.messageDialog.open" class="fixed inset-0 z-[85] bg-slate-950/60 backdrop-blur-sm flex items-center justify-center p-4" @click.self="App.closeMessageDialog()">
    <div class="w-full max-w-lg rounded-3xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 shadow-2xl p-6">
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0">
          <h3 class="text-lg font-semibold text-slate-900 dark:text-white">{{ App.messageDialog.title }}</h3>
        </div>
        <button type="button" class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 transition" @click="App.closeMessageDialog()">&times;</button>
      </div>
      <pre class="mt-4 whitespace-pre-wrap break-words text-sm text-slate-600 dark:text-slate-300 font-sans">{{ App.messageDialog.message }}</pre>
      <div class="mt-6 flex justify-end">
        <button type="button" class="px-4 py-2 rounded-xl text-sm font-medium transition" :class="App.getDialogConfirmButtonClass(App.messageDialog.tone)" @click="App.closeMessageDialog()">{{ App.messageDialog.confirmText }}</button>
      </div>
    </div>
  </div>

  <div v-if="App.confirmDialog.open" class="fixed inset-0 z-[86] bg-slate-950/60 backdrop-blur-sm flex items-center justify-center p-4" @click.self="App.resolveConfirmDialog(false)">
    <div class="w-full max-w-lg rounded-3xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 shadow-2xl p-6">
      <h3 class="text-lg font-semibold text-slate-900 dark:text-white">{{ App.confirmDialog.title }}</h3>
      <pre class="mt-4 whitespace-pre-wrap break-words text-sm text-slate-600 dark:text-slate-300 font-sans">{{ App.confirmDialog.message }}</pre>
      <div class="mt-6 flex justify-end gap-3">
        <button type="button" class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 text-sm font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-800 transition" @click="App.resolveConfirmDialog(false)">{{ App.confirmDialog.cancelText }}</button>
        <button type="button" class="px-4 py-2 rounded-xl text-sm font-medium transition" :class="App.getDialogConfirmButtonClass(App.confirmDialog.tone)" @click="App.resolveConfirmDialog(true)">{{ App.confirmDialog.confirmText }}</button>
      </div>
    </div>
  </div>

  <div v-if="App.promptDialog.open" class="fixed inset-0 z-[87] bg-slate-950/60 backdrop-blur-sm flex items-center justify-center p-4" @click.self="App.closePromptDialog(null)">
    <form class="w-full max-w-lg rounded-3xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 shadow-2xl p-6" @submit.prevent="App.submitPromptDialog()">
      <h3 class="text-lg font-semibold text-slate-900 dark:text-white">{{ App.promptDialog.title }}</h3>
      <p class="mt-4 whitespace-pre-wrap break-words text-sm text-slate-600 dark:text-slate-300">{{ App.promptDialog.message }}</p>
      <input v-auto-focus-select="App.promptDialog.open" v-model="App.promptDialog.value" :type="App.promptDialog.inputType" :placeholder="App.promptDialog.placeholder" class="mt-4 w-full px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 outline-none text-sm text-slate-900 dark:text-white">
      <div class="mt-6 flex justify-end gap-3">
        <button type="button" class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 text-sm font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-800 transition" @click="App.closePromptDialog(null)">{{ App.promptDialog.cancelText }}</button>
        <button type="submit" class="px-4 py-2 rounded-xl text-sm font-medium transition" :class="App.getDialogConfirmButtonClass(App.promptDialog.tone)">{{ App.promptDialog.confirmText }}</button>
      </div>
    </form>
  </div>
  </div>
  </div>
  </template>

  <script>
    const UI_DEFAULTS = {
      uiRadiusPx: 24,
      directStaticAssets: false,
      directHlsDash: false,
      prewarmDepth: 'poster_manifest',
      prewarmCacheTtl: 180,
      prewarmPrefetchBytes: 4194304,
      pingTimeout: 5000,
      pingCacheMinutes: 10,
      nodePanelPingAutoSort: false,
      upstreamTimeoutMs: 0,
      upstreamRetryAttempts: 0,
      logRetentionDays: 7,
      logWriteDelayMinutes: 20,
      logFlushCountThreshold: 50,
      logBatchChunkSize: 50,
      logBatchRetryCount: 2,
      logBatchRetryBackoffMs: 75,
      scheduledLeaseMs: 300000,
      tgAlertDroppedBatchThreshold: 0,
      tgAlertFlushRetryThreshold: 0,
      tgAlertCooldownMinutes: 30,
      tgAlertOnScheduledFailure: false
    };

    function normalizeRegionCodeCsv(value = '') {
      return [...new Set(String(value || '')
        .split(',')
        .map(item => item.trim().toUpperCase())
        .filter(Boolean))]
        .join(',');
    }

    const CONFIG_PREVIEW_SANITIZE_RULES = ${JSON.stringify(CONFIG_SANITIZE_RULES)};

    const CONFIG_FORM_BINDINGS = {
      ui: [
        { key: 'uiRadiusPx', id: 'cfg-ui-radius-px', kind: 'int-finite', defaultValue: UI_DEFAULTS.uiRadiusPx }
      ],
      proxy: [
        { key: 'enableH2', id: 'cfg-enable-h2', kind: 'checkbox', checkboxMode: 'truthy' },
        { key: 'enableH3', id: 'cfg-enable-h3', kind: 'checkbox', checkboxMode: 'truthy' },
        { key: 'peakDowngrade', id: 'cfg-peak-downgrade', kind: 'checkbox', checkboxMode: 'defaultTrue' },
        { key: 'protocolFallback', id: 'cfg-protocol-fallback', kind: 'checkbox', checkboxMode: 'defaultTrue' },
        { key: 'enablePrewarm', id: 'cfg-enable-prewarm', kind: 'checkbox', checkboxMode: 'defaultTrue' },
        { key: 'prewarmDepth', id: 'cfg-prewarm-depth', kind: 'or-default', defaultValue: UI_DEFAULTS.prewarmDepth },
        { key: 'prewarmCacheTtl', id: 'cfg-prewarm-ttl', kind: 'int-or-default', loadMode: 'number-finite', defaultValue: UI_DEFAULTS.prewarmCacheTtl },
        { key: 'directStaticAssets', id: 'cfg-direct-static-assets', kind: 'checkbox', checkboxMode: 'strictTrue' },
        { key: 'directHlsDash', id: 'cfg-direct-hls-dash', kind: 'checkbox', checkboxMode: 'strictTrue' },
        { key: 'sourceSameOriginProxy', id: 'cfg-source-same-origin-proxy', kind: 'checkbox', checkboxMode: 'defaultTrue' },
        { key: 'forceExternalProxy', id: 'cfg-force-external-proxy', kind: 'checkbox', checkboxMode: 'defaultTrue' },
        { key: 'wangpandirect', id: 'cfg-wangpandirect', kind: 'trim', loadMode: 'or-default', defaultValue: '${DEFAULT_WANGPAN_DIRECT_TEXT}' },
        { key: 'pingTimeout', id: 'cfg-ping-timeout', kind: 'int-or-default', loadMode: 'number-finite', defaultValue: UI_DEFAULTS.pingTimeout },
        { key: 'pingCacheMinutes', id: 'cfg-ping-cache-minutes', kind: 'int-or-default', loadMode: 'number-finite', defaultValue: UI_DEFAULTS.pingCacheMinutes },
        { key: 'nodePanelPingAutoSort', id: 'cfg-node-panel-ping-auto-sort', kind: 'checkbox', checkboxMode: 'strictTrue' },
        { key: 'upstreamTimeoutMs', id: 'cfg-upstream-timeout-ms', kind: 'int-finite', defaultValue: UI_DEFAULTS.upstreamTimeoutMs },
        { key: 'upstreamRetryAttempts', id: 'cfg-upstream-retry-attempts', kind: 'int-finite', defaultValue: UI_DEFAULTS.upstreamRetryAttempts }
      ],
      security: [
        { key: 'geoAllowlist', id: 'cfg-geo-allow', kind: 'text', defaultValue: '' },
        { key: 'geoBlocklist', id: 'cfg-geo-block', kind: 'text', defaultValue: '' },
        { key: 'ipBlacklist', id: 'cfg-ip-black', kind: 'text', defaultValue: '' },
        { key: 'rateLimitRpm', id: 'cfg-rate-limit', kind: 'int-or-default', loadMode: 'or-default', defaultValue: 0, loadDefaultValue: '' },
        { key: 'cacheTtlImages', id: 'cfg-cache-ttl', kind: 'int-or-default', defaultValue: 30 },
        { key: 'corsOrigins', id: 'cfg-cors', kind: 'text', defaultValue: '' }
      ],
      logs: [
        { key: 'logRetentionDays', id: 'cfg-log-days', kind: 'int-finite', defaultValue: UI_DEFAULTS.logRetentionDays },
        { key: 'logWriteDelayMinutes', id: 'cfg-log-delay', kind: 'float-finite', defaultValue: UI_DEFAULTS.logWriteDelayMinutes },
        { key: 'logFlushCountThreshold', id: 'cfg-log-flush-count', kind: 'int-finite', defaultValue: UI_DEFAULTS.logFlushCountThreshold },
        { key: 'logBatchChunkSize', id: 'cfg-log-batch-size', kind: 'int-finite', defaultValue: UI_DEFAULTS.logBatchChunkSize },
        { key: 'logBatchRetryCount', id: 'cfg-log-retry-count', kind: 'int-finite', defaultValue: UI_DEFAULTS.logBatchRetryCount },
        { key: 'logBatchRetryBackoffMs', id: 'cfg-log-retry-backoff', kind: 'int-finite', defaultValue: UI_DEFAULTS.logBatchRetryBackoffMs },
        { key: 'scheduledLeaseMs', id: 'cfg-scheduled-lease-ms', kind: 'int-finite', defaultValue: UI_DEFAULTS.scheduledLeaseMs },
        { key: 'tgBotToken', id: 'cfg-tg-token', kind: 'trim', defaultValue: '' },
        { key: 'tgChatId', id: 'cfg-tg-chatid', kind: 'trim', defaultValue: '' },
        { key: 'tgAlertDroppedBatchThreshold', id: 'cfg-tg-alert-drop-threshold', kind: 'int-finite', defaultValue: UI_DEFAULTS.tgAlertDroppedBatchThreshold },
        { key: 'tgAlertFlushRetryThreshold', id: 'cfg-tg-alert-retry-threshold', kind: 'int-finite', defaultValue: UI_DEFAULTS.tgAlertFlushRetryThreshold },
        { key: 'tgAlertOnScheduledFailure', id: 'cfg-tg-alert-scheduled-failure', kind: 'checkbox', checkboxMode: 'strictTrue' },
        { key: 'tgAlertCooldownMinutes', id: 'cfg-tg-alert-cooldown-minutes', kind: 'int-finite', defaultValue: UI_DEFAULTS.tgAlertCooldownMinutes }
      ],
      account: [
        { key: 'jwtExpiryDays', id: 'cfg-jwt-days', kind: 'int-or-default', defaultValue: 30 },
        { key: 'cfAccountId', id: 'cfg-cf-account', kind: 'trim', defaultValue: '' },
        { key: 'cfZoneId', id: 'cfg-cf-zone', kind: 'trim', defaultValue: '' },
        { key: 'cfApiToken', id: 'cfg-cf-token', kind: 'trim', defaultValue: '' }
      ]
    };

    const CONFIG_SECTION_FIELDS = {
      ui: CONFIG_FORM_BINDINGS.ui.map(item => item.key),
      proxy: [...CONFIG_FORM_BINDINGS.proxy.map(item => item.key), 'sourceDirectNodes'],
      security: CONFIG_FORM_BINDINGS.security.map(item => item.key),
      logs: CONFIG_FORM_BINDINGS.logs.map(item => item.key),
      account: CONFIG_FORM_BINDINGS.account.map(item => item.key)
    };

    const CONFIG_FIELD_LABELS = {
      uiRadiusPx: 'UI 圆角弧度（px）',
      enableH2: 'HTTP/2',
      enableH3: 'HTTP/3',
      peakDowngrade: '晚高峰降级兜底',
      protocolFallback: '协议回退与 403 重试',
      enablePrewarm: '轻量级元数据预热',
      prewarmDepth: '预热深度',
      prewarmCacheTtl: '元数据预热缓存时长',
      directStaticAssets: '静态文件直连',
      directHlsDash: 'HLS / DASH 直连',
      sourceSameOriginProxy: '源站同源代理',
      forceExternalProxy: '外链强制反代',
      wangpandirect: 'wangpandirect 关键词',
      sourceDirectNodes: '源站直连节点名单',
      pingTimeout: 'Ping 超时',
      pingCacheMinutes: 'Ping 缓存时间',
      nodePanelPingAutoSort: '节点面板 Ping 自动排序',
      upstreamTimeoutMs: '上游握手超时',
      upstreamRetryAttempts: '额外重试轮次',
      geoAllowlist: '国家/地区白名单',
      geoBlocklist: '国家/地区黑名单',
      ipBlacklist: 'IP 黑名单',
      rateLimitRpm: '单 IP 限速',
      cacheTtlImages: '图片缓存时长',
      corsOrigins: 'CORS 白名单',
      logRetentionDays: '日志保存天数',
      logWriteDelayMinutes: '日志写入延迟',
      logFlushCountThreshold: '日志提前写入阈值',
      logBatchChunkSize: 'D1 切片大小',
      logBatchRetryCount: 'D1 重试次数',
      logBatchRetryBackoffMs: 'D1 退避毫秒',
      scheduledLeaseMs: '定时任务租约时长',
      tgBotToken: 'Telegram Bot Token',
      tgChatId: 'Telegram Chat ID',
      tgAlertDroppedBatchThreshold: '日志丢弃批次阈值',
      tgAlertFlushRetryThreshold: '日志写入重试阈值',
      tgAlertOnScheduledFailure: '定时任务失败告警',
      tgAlertCooldownMinutes: '告警冷却时间',
      jwtExpiryDays: 'JWT 有效天数',
      cfAccountId: 'Cloudflare 账号 ID',
      cfZoneId: 'Cloudflare Zone ID',
      cfApiToken: 'Cloudflare API 令牌'
    };

    const SNAPSHOT_REASON_LABELS = {
      save_config: '手动保存设置',
      import_settings: '导入全局设置',
      import_full: '导入完整备份',
      restore_snapshot: '恢复历史快照',
      tidy_kv_data: '整理 KV 数据'
    };

    const RECOMMENDED_SECTION_VALUES = {
      proxy: {
        enableH2: false,
        enableH3: false,
        peakDowngrade: true,
        protocolFallback: true,
        enablePrewarm: true,
        prewarmDepth: 'poster_manifest',
        prewarmCacheTtl: 180,
        directStaticAssets: true,
        directHlsDash: true,
        sourceSameOriginProxy: true,
        forceExternalProxy: true,
        pingTimeout: 5000,
        pingCacheMinutes: 10,
        nodePanelPingAutoSort: false,
        upstreamTimeoutMs: 30000,
        upstreamRetryAttempts: 1
      },
      logs: {
        logRetentionDays: 7,
        logWriteDelayMinutes: 20,
        logFlushCountThreshold: 50,
        logBatchChunkSize: 50,
        logBatchRetryCount: 2,
        logBatchRetryBackoffMs: 75,
        scheduledLeaseMs: 300000,
        tgAlertDroppedBatchThreshold: 1,
        tgAlertFlushRetryThreshold: 2,
        tgAlertOnScheduledFailure: true,
        tgAlertCooldownMinutes: 30
      }
    };

    const VIEW_TITLES = {
      '#dashboard': '仪表盘',
      '#nodes': '节点列表',
      '#logs': '日志记录',
      '#dns': 'DNS编辑',
      '#settings': '全局设置'
    };

    const NAV_ITEMS = [
      { hash: '#dashboard', icon: 'layout-dashboard', label: '仪表盘' },
      { hash: '#nodes', icon: 'server', label: '节点列表' },
      { hash: '#logs', icon: 'activity', label: '日志记录' },
      { hash: '#dns', icon: 'globe', label: 'DNS编辑' },
      { hash: '#settings', icon: 'settings', label: '全局设置' }
    ];

    const CONFIG_BINDING_LIST = Object.values(CONFIG_FORM_BINDINGS).flat();
    const CONFIG_BINDING_BY_KEY = CONFIG_BINDING_LIST.reduce((acc, binding) => {
      acc[binding.key] = binding;
      return acc;
    }, {});
    const DEFAULT_PROXY_GUARDRAILS = {
      directHint: '当前未启用 307 直连分流；如后续开启静态文件直连或 HLS / DASH 直连，命中的资源会自动走数据面直传。',
      prewarmHint: '当前会预热海报、播放列表与字幕等轻量元数据；检测到 mp4 / mkv / ts / m4s 等视频字节流时会立即跳过。',
      prefetchDisabled: false
    };

    function createNodeModalHeaderRow(key = '', value = '') {
      return {
        id: 'hdr-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8),
        key,
        value
      };
    }

    function createEmptyNodeModalForm() {
      return {
        originalName: '',
        displayName: '',
        name: '',
        tag: '',
        tagColor: 'amber',
        remark: '',
        secret: '',
        activeLineId: '',
        headers: []
      };
    }

    function createToastState() {
      return {
        visible: false,
        message: '',
        tone: 'info'
      };
    }

    function createMessageDialogState() {
      return {
        open: false,
        title: '提示',
        message: '',
        tone: 'info',
        confirmText: '知道了'
      };
    }

    function createConfirmDialogState() {
      return {
        open: false,
        title: '请确认',
        message: '',
        tone: 'warning',
        confirmText: '确认',
        cancelText: '取消'
      };
    }

    function createPromptDialogState() {
      return {
        open: false,
        title: '请输入',
        message: '',
        value: '',
        placeholder: '',
        tone: 'info',
        inputType: 'text',
        confirmText: '确认',
        cancelText: '取消',
        required: false
      };
    }

    const UiBridge = {
      navItems: NAV_ITEMS,
      currentHash: '#dashboard',
      pageTitle: '加载中...',
      sidebarOpen: false,
      isDarkTheme: false,
      isDesktopViewport: false,
      isDesktopSettingsLayout: false,
      contentScrollResetKey: 0,
      settingsScrollResetKey: 0,
      uiRadiusCssValue: '24px',
      activeSettingsTab: 'ui',
      nodeSearchKeyword: '',
      nodes: [],
      settingsForm: {},
      settingsDirectNodeSearch: '',
      settingsSourceDirectNodes: [],
      proxySettingsGuardrails: { ...DEFAULT_PROXY_GUARDRAILS },
      nodeHealth: {},
      nodesHealthCheckPending: false,
      nodePingPending: {},
      nodeMutationSeq: 0,
      nodeMutationVersion: {},
      logPage: 1,
      logTotalPages: 1,
      logRows: [],
      logSearchKeyword: '',
      logStartDate: '',
      logEndDate: '',
      logTimeTick: 0,
      logsPlaybackModeFilter: '',
      dnsRecords: [],
      dnsZone: null,
      dnsZoneHintText: '当前站点：加载中...',
      dnsCurrentHost: '',
      dnsTotalRecordCount: 0,
      dnsEmptyText: '暂无 DNS 记录',
      dnsHistoryLimit: 10,
      dnsBatchSaving: false,
      dnsLoadSeq: 0,
      dashboardSeries: [],
      dashboardLoadSeq: 0,
      dashboardView: {
        requests: {
          count: '0',
          hint: ' ',
          title: '',
          badges: [{ label: '待加载', tone: 'slate' }],
          embyMetrics: '请求: 播放请求 0 次 | 获取播放信息 0 次'
        },
        traffic: {
          count: '0 B',
          hint: ' ',
          title: '',
          badges: [{ label: '待加载', tone: 'slate' }],
          detail: ' '
        },
        nodes: {
          count: '0',
          meta: ' ',
          badges: [{ label: '待加载', tone: 'slate' }]
        }
      },
      dashboardRuntimeView: {
        updatedText: '最近同步：未加载',
        logCard: {
          title: '日志写入',
          status: 'idle',
          summary: '日志写入状态加载中...',
          lines: [],
          detail: ''
        },
        scheduledCard: {
          title: '定时任务',
          status: 'idle',
          summary: '定时任务状态加载中...',
          lines: [],
          detail: ''
        }
      },
      runtimeConfig: {},
      configSnapshots: [],
      runtimeStatus: {},
      loginPromise: null,
      downloadHref: '',
      downloadFilename: '',
      downloadTriggerKey: 0,
      downloadCleanupTimer: null,
      nodeModalOpen: false,
      nodeModalSubmitting: false,
      nodeModalPingAllPending: false,
      nodeModalPingAllText: '一键测试延迟',
      nodeModalForm: createEmptyNodeModalForm(),
      nodeModalPathManual: false,
      nodeModalLastDisplayName: '',
      nodeModalLines: [],
      nodeLineDragId: '',
      nodeLineDropHint: null,
      toastState: createToastState(),
      toastTimer: null,
      messageDialog: createMessageDialogState(),
      messageDialogResolver: null,
      confirmDialog: createConfirmDialogState(),
      confirmDialogResolver: null,
      promptDialog: createPromptDialogState(),
      promptDialogResolver: null,

      normalizeUiMessage(message) {
        return String(message == null ? '' : message).trim();
      },

      getToastToneClass(tone = 'info') {
        const palette = {
          info: 'border-slate-200 bg-white/95 text-slate-700 dark:border-slate-700 dark:bg-slate-900/95 dark:text-slate-100',
          success: 'border-emerald-200 bg-emerald-50/95 text-emerald-700 dark:border-emerald-900/40 dark:bg-emerald-950/90 dark:text-emerald-200',
          warning: 'border-amber-200 bg-amber-50/95 text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/90 dark:text-amber-200',
          error: 'border-red-200 bg-red-50/95 text-red-700 dark:border-red-900/40 dark:bg-red-950/90 dark:text-red-200'
        };
        return palette[tone] || palette.info;
      },

      getDialogConfirmButtonClass(tone = 'info') {
        const palette = {
          info: 'bg-brand-600 hover:bg-brand-700 text-white',
          success: 'bg-emerald-600 hover:bg-emerald-700 text-white',
          warning: 'bg-amber-600 hover:bg-amber-700 text-white',
          error: 'bg-red-600 hover:bg-red-700 text-white',
          danger: 'bg-red-600 hover:bg-red-700 text-white'
        };
        return palette[tone] || palette.info;
      },

      clearToastTimer() {
        if (this.toastTimer) {
          uiBrowserBridge.clearTimer(this.toastTimer);
          this.toastTimer = null;
        }
      },

      dismissToast() {
        this.clearToastTimer();
        this.toastState = createToastState();
      },

      showToast(message, tone = 'info', duration = 2600) {
        const text = this.normalizeUiMessage(message);
        if (!text) return Promise.resolve(false);
        this.clearToastTimer();
        this.toastState = {
          visible: true,
          message: text,
          tone: String(tone || 'info') || 'info'
        };
        this.toastTimer = uiBrowserBridge.startTimer(() => this.dismissToast(), Math.max(1200, Number(duration) || 2600));
        return Promise.resolve(true);
      },

      closeMessageDialog(result = true) {
        const resolve = this.messageDialogResolver;
        this.messageDialogResolver = null;
        this.messageDialog = createMessageDialogState();
        if (typeof resolve === 'function') resolve(result);
        return result;
      },

      openMessageDialog(message, options = {}) {
        const text = this.normalizeUiMessage(message);
        if (!text) return Promise.resolve(false);
        if (typeof this.messageDialogResolver === 'function') this.messageDialogResolver(true);
        this.messageDialog = {
          ...createMessageDialogState(),
          open: true,
          title: this.normalizeUiMessage(options.title) || '提示',
          message: text,
          tone: String(options.tone || 'info') || 'info',
          confirmText: this.normalizeUiMessage(options.confirmText) || '知道了'
        };
        return new Promise(resolve => {
          this.messageDialogResolver = resolve;
        });
      },

      showMessage(message, options = {}) {
        const text = this.normalizeUiMessage(message);
        if (!text) return Promise.resolve(false);
        const useModal = options.modal === true || text.includes('\\n') || text.length > 96;
        if (!useModal) return this.showToast(text, options.tone || 'info', options.duration);
        return this.openMessageDialog(text, options);
      },

      resolveConfirmDialog(result = false) {
        const resolve = this.confirmDialogResolver;
        this.confirmDialogResolver = null;
        this.confirmDialog = createConfirmDialogState();
        if (typeof resolve === 'function') resolve(result === true);
        return result === true;
      },

      askConfirm(message, options = {}) {
        const text = this.normalizeUiMessage(message);
        if (!text) return Promise.resolve(false);
        if (typeof this.confirmDialogResolver === 'function') this.confirmDialogResolver(false);
        this.confirmDialog = {
          ...createConfirmDialogState(),
          open: true,
          title: this.normalizeUiMessage(options.title) || '请确认',
          message: text,
          tone: String(options.tone || 'warning') || 'warning',
          confirmText: this.normalizeUiMessage(options.confirmText) || '确认',
          cancelText: this.normalizeUiMessage(options.cancelText) || '取消'
        };
        return new Promise(resolve => {
          this.confirmDialogResolver = resolve;
        });
      },

      closePromptDialog(result = null) {
        const resolve = this.promptDialogResolver;
        this.promptDialogResolver = null;
        const finalValue = result == null ? null : String(result);
        this.promptDialog = createPromptDialogState();
        if (typeof resolve === 'function') resolve(finalValue);
        return finalValue;
      },

      askPrompt(options = {}) {
        if (typeof this.promptDialogResolver === 'function') this.promptDialogResolver(null);
        this.promptDialog = {
          ...createPromptDialogState(),
          open: true,
          title: this.normalizeUiMessage(options.title) || '请输入',
          message: this.normalizeUiMessage(options.message) || '请输入内容',
          value: String(options.defaultValue || ''),
          placeholder: String(options.placeholder || ''),
          tone: String(options.tone || 'info') || 'info',
          inputType: options.inputType === 'password' ? 'password' : 'text',
          confirmText: this.normalizeUiMessage(options.confirmText) || '确认',
          cancelText: this.normalizeUiMessage(options.cancelText) || '取消',
          required: options.required === true
        };
        return new Promise(resolve => {
          this.promptDialogResolver = resolve;
        });
      },

      submitPromptDialog() {
        const value = String(this.promptDialog.value || '');
        if (this.promptDialog.required && !value.trim()) {
          this.showToast('请输入必填内容', 'warning');
          return;
        }
        this.closePromptDialog(value);
      },

      revokeDownloadUrl() {
        if (this.downloadCleanupTimer) {
          uiBrowserBridge.clearTimer(this.downloadCleanupTimer);
          this.downloadCleanupTimer = null;
        }
        const currentHref = String(this.downloadHref || '');
        if (currentHref.startsWith('blob:')) {
          uiBrowserBridge.revokeObjectUrl(currentHref);
        }
        this.downloadHref = '';
        this.downloadFilename = '';
      },

      triggerDownload(url, filename) {
        this.revokeDownloadUrl();
        this.downloadHref = String(url || '');
        this.downloadFilename = String(filename || '');
        this.downloadTriggerKey += 1;
        this.downloadCleanupTimer = uiBrowserBridge.startTimer(() => this.revokeDownloadUrl(), 1000);
        return Promise.resolve({
          href: this.downloadHref,
          filename: this.downloadFilename
        });
      },

      getConfigBindingByKey(key) {
        return CONFIG_BINDING_BY_KEY[key] || null;
      },

      getCurrentRouteHash(fallback = '#dashboard') {
        return String(this.currentHash || uiBrowserBridge.readHash(fallback) || fallback || '#dashboard');
      },

      hasSettingsFieldValue(key) {
        return Object.prototype.hasOwnProperty.call(this.settingsForm || {}, key);
      },

      getEffectiveSettingValue(key) {
        const binding = this.getConfigBindingByKey(key);
        if (!binding) return undefined;
        if (this.hasSettingsFieldValue(key)) return this.readConfigBindingFromState(binding);
        return this.resolveConfigBindingInputValue(binding, this.runtimeConfig || {});
      },

      clampSettingsNumberInput(element) {
        if (!element) return;
        const raw = String(element.value || '').trim();
        if (!raw) return;
        let next = Number(raw);
        if (!Number.isFinite(next)) {
          element.value = '';
          return;
        }
        const min = Number(element.min);
        const max = Number(element.max);
        if (Number.isFinite(min)) next = Math.max(min, next);
        if (Number.isFinite(max)) next = Math.min(max, next);
        const step = String(element.step || '').trim();
        if (step && step !== 'any') {
          const stepValue = Number(step);
          if (Number.isFinite(stepValue) && stepValue > 0) {
            const base = Number.isFinite(min) ? min : 0;
            const steps = Math.round((next - base) / stepValue);
            next = base + (steps * stepValue);
            if (Number.isFinite(min)) next = Math.max(min, next);
            if (Number.isFinite(max)) next = Math.min(max, next);
          }
        }
        element.value = step.includes('.') ? String(next) : String(Math.trunc(next));
      },

      normalizeSettingsNumberInputs() {
        this.settingsForm = { ...this.settingsForm };
      },

      syncProxySettingsGuardrails() {
        const directStatic = this.readConfigBindingFromState(this.getConfigBindingByKey('directStaticAssets')) === true;
        const directHlsDash = this.readConfigBindingFromState(this.getConfigBindingByKey('directHlsDash')) === true;
        const prewarmDepthBinding = this.getConfigBindingByKey('prewarmDepth');
        const rawPrewarmDepth = this.readConfigBindingFromState(prewarmDepthBinding);
        const prewarmDepth = String(rawPrewarmDepth || UI_DEFAULTS.prewarmDepth).trim().toLowerCase() === 'poster' ? 'poster' : 'poster_manifest';
        const direct307Enabled = directStatic || directHlsDash;

        if (direct307Enabled) {
          this.proxySettingsGuardrails = {
            directHint: '已启用 307 直连分流。命中的静态 / HLS / DASH 资源会自动下沉到数据面直传，减少 Worker 长连接负担。',
            prewarmHint: prewarmDepth === 'poster'
              ? '当前只预热海报；由于已启用 HLS / DASH 直连，播放列表会直接走 307 分流，不再进入 Worker 元数据缓存。'
              : '已启用 HLS / DASH 直连。海报与字幕仍可按需预热，但命中的播放列表会直接走 307 分流，不再占用 Worker 缓存通道。',
            prefetchDisabled: false
          };
          return;
        }
        this.proxySettingsGuardrails = {
          directHint: DEFAULT_PROXY_GUARDRAILS.directHint,
          prewarmHint: prewarmDepth === 'poster'
            ? '当前只预热海报，不会额外拉取 m3u8 或字幕索引，更适合极度克制的 Worker 负载策略。'
            : DEFAULT_PROXY_GUARDRAILS.prewarmHint,
          prefetchDisabled: false
        };
      },

      applyRuntimeConfig(cfg) {
        this.runtimeConfig = cfg && typeof cfg === 'object' ? { ...cfg } : {};
        this.applyUiRadius();
      },

      applyUiRadius() {
        const raw = Number(this.runtimeConfig?.uiRadiusPx);
        const fallback = Number(UI_DEFAULTS.uiRadiusPx);
        let next = Number.isFinite(raw) ? Math.trunc(raw) : fallback;
        if (!Number.isFinite(next)) next = 24;
        next = Math.max(0, Math.min(48, next));
        this.uiRadiusCssValue = String(next) + 'px';
      },

      getSettingsSectionLabel(section) {
        const labels = {
          ui: '系统 UI',
          proxy: '代理与网络',
          security: '缓存与安全',
          logs: '日志与监控',
          account: '账号与备份',
          all: '全部分区'
        };
        return labels[section] || section || '未知分区';
      },

      getConfigFieldLabel(key) {
        return CONFIG_FIELD_LABELS[key] || key;
      },

      getConfigFormBindings(section) {
        return CONFIG_FORM_BINDINGS[section] || [];
      },

      getConfigBindingDefaultValue(binding, phase = 'save') {
        if (phase === 'load' && Object.prototype.hasOwnProperty.call(binding || {}, 'loadDefaultValue')) {
          return binding.loadDefaultValue;
        }
        return Object.prototype.hasOwnProperty.call(binding || {}, 'defaultValue') ? binding.defaultValue : '';
      },

      getConfigBindingMode(binding, phase = 'save') {
        if (phase === 'load' && binding?.loadMode) return binding.loadMode;
        if (phase === 'save' && binding?.saveMode) return binding.saveMode;
        return binding?.kind || 'text';
      },

      resolveConfigBindingInputValue(binding, source = {}) {
        const rawValue = source?.[binding.key];
        const mode = this.getConfigBindingMode(binding, 'load');
        const fallback = this.getConfigBindingDefaultValue(binding, 'load');
        if (mode === 'checkbox') {
          if (binding.checkboxMode === 'defaultTrue') return rawValue !== false;
          if (binding.checkboxMode === 'truthy') return !!rawValue;
          return rawValue === true;
        }
        if (mode === 'or-default') return rawValue || fallback;
        if (mode === 'int-or-default') {
          const num = parseInt(rawValue, 10);
          return num || fallback;
        }
        if (mode === 'int-finite' || mode === 'number-finite') {
          const num = Number(rawValue);
          return Number.isFinite(num) ? num : fallback;
        }
        if (mode === 'float-finite') {
          const num = Number(rawValue);
          return Number.isFinite(num) ? num : fallback;
        }
        if (rawValue === undefined || rawValue === null) return fallback;
        return String(rawValue);
      },

      resolveGeoFirewallFormState(source = {}) {
        const geoAllowlist = normalizeRegionCodeCsv(source?.geoAllowlist || '');
        const geoBlocklist = normalizeRegionCodeCsv(source?.geoBlocklist || '');
        if (geoAllowlist) return { geoMode: 'allowlist', geoRegions: geoAllowlist };
        if (geoBlocklist) return { geoMode: 'blocklist', geoRegions: geoBlocklist };
        return {
          geoMode: String(this.settingsForm?.geoMode || 'allowlist') === 'blocklist' ? 'blocklist' : 'allowlist',
          geoRegions: ''
        };
      },

      applyConfigSectionToForm(section, source = {}, options = {}) {
        const onlyPresent = options.onlyPresent === true;
        const nextSettingsForm = { ...this.settingsForm };
        this.getConfigFormBindings(section).forEach(binding => {
          if (onlyPresent && !Object.prototype.hasOwnProperty.call(source || {}, binding.key)) return;
          nextSettingsForm[binding.key] = this.resolveConfigBindingInputValue(binding, source);
        });
        if (section === 'security') Object.assign(nextSettingsForm, this.resolveGeoFirewallFormState(source));
        this.settingsForm = nextSettingsForm;
      },

      readConfigBindingFromState(binding) {
        if (!binding) return undefined;
        const rawValue = this.settingsForm?.[binding.key];
        const mode = this.getConfigBindingMode(binding, 'save');
        const fallback = this.getConfigBindingDefaultValue(binding, 'save');
        if (mode === 'checkbox') return rawValue === true;
        if (mode === 'int-or-default') {
          const num = parseInt(rawValue, 10);
          return num || fallback;
        }
        if (mode === 'int-finite' || mode === 'number-finite') {
          const num = parseInt(rawValue, 10);
          return Number.isFinite(num) ? num : fallback;
        }
        if (mode === 'float-finite') {
          const num = parseFloat(rawValue);
          return Number.isFinite(num) ? num : fallback;
        }
        if (mode === 'trim') return String(rawValue || '').trim();
        if (rawValue === undefined || rawValue === null) return '';
        return rawValue;
      },

      collectConfigSectionFromForm(section) {
        const collected = this.getConfigFormBindings(section).reduce((acc, binding) => {
          const value = this.readConfigBindingFromState(binding);
          if (value !== undefined) acc[binding.key] = value;
          return acc;
        }, {});
        if (section === 'security') {
          const geoMode = String(this.settingsForm?.geoMode || 'allowlist').trim().toLowerCase() === 'blocklist' ? 'blocklist' : 'allowlist';
          const geoRegions = normalizeRegionCodeCsv(this.settingsForm?.geoRegions || '');
          collected.geoAllowlist = geoMode === 'allowlist' ? geoRegions : '';
          collected.geoBlocklist = geoMode === 'blocklist' ? geoRegions : '';
        }
        return collected;
      },

      formatConfigPreviewValue(key, value) {
        if (Array.isArray(value)) return value.length ? value.join(', ') : '空';
        if (typeof value === 'boolean') return value ? '开启' : '关闭';
        if (value === undefined || value === null || value === '') return '空';
        return String(value);
      },

      getSettingsRiskHints(section, nextConfig) {
        const hints = [];
        if ((section === 'proxy' || section === 'all') && nextConfig.enableH2 === true && nextConfig.enableH3 === true && nextConfig.peakDowngrade === false) {
          hints.push('H2/H3 同时开启且关闭晚高峰降级，在复杂链路下更容易放大协议抖动。');
        }
        if ((section === 'proxy' || section === 'all') && Number(nextConfig.upstreamTimeoutMs) > 0 && Number(nextConfig.upstreamTimeoutMs) < 5000) {
          hints.push('上游握手超时低于 5000 毫秒，慢源或弱网容易被过早判定失败。');
        }
        if ((section === 'logs' || section === 'all') && Number(nextConfig.logBatchRetryCount) === 0) {
          hints.push('D1 重试次数为 0，瞬时抖动时会直接丢弃日志批次。');
        }
        if ((section === 'logs' || section === 'all') && Number(nextConfig.scheduledLeaseMs) > 0 && Number(nextConfig.scheduledLeaseMs) < 60000) {
          hints.push('定时任务租约低于 60 秒，慢清理或网络抖动时更容易出现并发重入。');
        }
        if ((section === 'logs' || section === 'all') && nextConfig.tgAlertOnScheduledFailure === true && (!String(nextConfig.tgBotToken || '').trim() || !String(nextConfig.tgChatId || '').trim())) {
          hints.push('已启用 Telegram 异常告警，但 Bot Token / Chat ID 还未完整配置。');
        }
        return hints;
      },

      buildConfigChangePreview(section, prevConfig, nextConfig) {
        const fields = CONFIG_SECTION_FIELDS[section] || [...new Set([...Object.keys(prevConfig || {}), ...Object.keys(nextConfig || {})])];
        const diffLines = [];
        fields.forEach(key => {
          const before = JSON.stringify(prevConfig?.[key]);
          const after = JSON.stringify(nextConfig?.[key]);
          if (before === after) return;
          diffLines.push('• ' + this.getConfigFieldLabel(key) + ': ' + this.formatConfigPreviewValue(key, prevConfig?.[key]) + ' -> ' + this.formatConfigPreviewValue(key, nextConfig?.[key]));
        });
        if (!diffLines.length) {
          return {
            hasChanges: false,
            message: '当前分区没有检测到变更，无需保存。'
          };
        }
        const riskHints = this.getSettingsRiskHints(section, nextConfig);
        let message = '即将保存「' + this.getSettingsSectionLabel(section) + '」以下变更：\\n\\n' + diffLines.join('\\n');
        if (riskHints.length) {
          message += '\\n\\n风险提示：\\n' + riskHints.map(item => '• ' + item).join('\\n');
        }
        message += '\\n\\n是否继续？';
        return { hasChanges: true, message, riskHints };
      },

      clampPreviewValue(value, fallback, min, max, integer = false) {
        let next = Number.isFinite(Number(value)) ? Number(value) : Number(fallback);
        if (integer) next = Math.trunc(next);
        if (Number.isFinite(min)) next = Math.max(min, next);
        if (Number.isFinite(max)) next = Math.min(max, next);
        return next;
      },

      sanitizeConfigByRules(input, rules) {
        const config = input && typeof input === 'object' && !Array.isArray(input) ? { ...input } : {};
        (rules?.trimFields || []).forEach(key => {
          if (config[key] === undefined || config[key] === null) return;
          config[key] = String(config[key]).trim();
        });
        Object.entries(rules?.arrayNormalizers || {}).forEach(([key, normalizerName]) => {
          if (!Array.isArray(config[key])) return;
          if (normalizerName === 'nodeNameList') config[key] = this.normalizeNodeNameList(config[key]);
        });
        Object.entries(rules?.integerFields || {}).forEach(([key, rule]) => {
          config[key] = this.clampPreviewValue(config[key], rule.fallback, rule.min, rule.max, true);
        });
        Object.entries(rules?.numberFields || {}).forEach(([key, rule]) => {
          config[key] = this.clampPreviewValue(config[key], rule.fallback, rule.min, rule.max, false);
        });
        (rules?.booleanTrueFields || []).forEach(key => {
          config[key] = config[key] !== false;
        });
        (rules?.booleanFalseFields || []).forEach(key => {
          config[key] = config[key] === true;
        });
        return config;
      },

      sanitizeConfigPreviewCompat(input) {
        return this.sanitizeConfigByRules(input, CONFIG_PREVIEW_SANITIZE_RULES);
      },

      async finalizePersistedSettings(savedConfig, options = {}) {
        const appliedConfig = savedConfig && typeof savedConfig === 'object' && !Array.isArray(savedConfig) ? savedConfig : {};
        this.applyRuntimeConfig(appliedConfig);
        try {
          await this.loadSettings();
          this.showMessage(options.successMessage || '设置已保存，立即生效', { tone: 'success' });
        } catch (err) {
          console.error(options.refreshErrorLog || 'reload settings after persist failed', err);
          this.showMessage((options.partialSuccessPrefix || '设置已保存，但设置面板刷新失败: ') + (err?.message || '未知错误'), { tone: 'warning', modal: true });
        }
      },

      async prepareConfigChangePreview(section, prevConfig, rawNextConfig) {
        let sanitizedConfig;
        try {
          const previewRes = await this.apiCall('previewConfig', { config: rawNextConfig });
          if (!previewRes?.config || typeof previewRes.config !== 'object' || Array.isArray(previewRes.config)) {
            throw new Error('配置预览返回格式无效');
          }
          sanitizedConfig = previewRes.config;
        } catch (err) {
          if (err?.code === 'INVALID_ACTION' && err?.status === 400) {
            sanitizedConfig = this.sanitizeConfigPreviewCompat(rawNextConfig);
          } else {
            const detail = String(err?.message || '未知错误');
            throw new Error(detail.startsWith('配置预览失败') ? detail : ('配置预览失败: ' + detail));
          }
        }
        return {
          sanitizedConfig,
          preview: this.buildConfigChangePreview(section, prevConfig, sanitizedConfig)
        };
      },

      formatSnapshotReason(snapshot) {
        const reasonLabel = SNAPSHOT_REASON_LABELS[snapshot?.reason] || (snapshot?.reason || '未知来源');
        const section = String(snapshot?.section || 'all');
        return section && section !== 'all'
          ? (reasonLabel + ' · ' + this.getSettingsSectionLabel(section))
          : reasonLabel;
      },

      getConfigSnapshotChangedKeysText(snapshot) {
        const changedKeys = Array.isArray(snapshot?.changedKeys)
          ? snapshot.changedKeys.slice(0, 4).map(key => this.getConfigFieldLabel(key)).join(' / ')
          : '';
        const overflow = Array.isArray(snapshot?.changedKeys) && snapshot.changedKeys.length > 4
          ? (' +' + (snapshot.changedKeys.length - 4) + ' 项')
          : '';
        return (changedKeys || '未记录') + overflow;
      },

      applyConfigSnapshotsState(snapshots) {
        this.configSnapshots = Array.isArray(snapshots) ? snapshots : [];
        return this.configSnapshots;
      },

      async loadConfigSnapshots() {
        const res = await this.apiCall('getConfigSnapshots');
        this.applyConfigSnapshotsState(res.snapshots || []);
      },

      async clearConfigSnapshots() {
        if (!await this.askConfirm('清理后将删除当前保存的全部设置快照，且不能恢复。是否继续？', { title: '清理设置快照', tone: 'danger', confirmText: '继续' })) return;
        const res = await this.apiCall('clearConfigSnapshots');
        this.applyConfigSnapshotsState(res.snapshots || []);
        this.showMessage('设置快照已清理。', { tone: 'success' });
      },

      async restoreConfigSnapshot(snapshotId) {
        if (!snapshotId) return;
        if (!await this.askConfirm('恢复该快照后，当前全局设置会立即被替换。系统会先自动记录当前配置，是否继续？', { title: '恢复设置快照', tone: 'warning', confirmText: '恢复' })) return;
        const res = await this.apiCall('restoreConfigSnapshot', { id: snapshotId });
        this.applyRuntimeConfig(res.config || {});
        await this.loadSettings();
        this.showMessage('配置快照已恢复并立即生效。', { tone: 'success' });
      },

      simpleHash(str) {
        const input = String(str || "");
        let hash = 0;
        for (let i = 0; i < input.length; i++) {
          hash = ((hash << 5) - hash + input.charCodeAt(i)) | 0;
        }
        return String(hash >>> 0).toString(36);
      },

      safeDomId(prefix, value) {
        const base = String(value || "").toLowerCase().replace(/[^a-z0-9_-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 24) || "node";
        return prefix + "-" + base + "-" + this.simpleHash(value);
      },

      buildNodeLink(node) {
        const encodedName = encodeURIComponent(String(node.name || ""));
        const encodedSecret = node.secret ? "/" + encodeURIComponent(String(node.secret)) : "";
        return uiBrowserBridge.readLocationOrigin() + "/" + encodedName + encodedSecret;
      },
      normalizeNodeKey(value) {
        return String(value || '').trim().toLowerCase();
      },
      normalizeNodeNameList(value) {
        const rawList = Array.isArray(value) ? value : String(value || '').split(/[\\r\\n,，;；|]+/);
        const seen = new Set();
        const result = [];
        rawList.forEach(item => {
          const name = String(item || '').trim();
          if (!name) return;
          const key = name.toLowerCase();
          if (seen.has(key)) return;
          seen.add(key);
          result.push(name);
        });
        return result;
      },
      markNodeMutation(names) {
        const mutationId = ++this.nodeMutationSeq;
        this.normalizeNodeNameList(names).forEach(name => {
          const key = this.normalizeNodeKey(name);
          if (key) this.nodeMutationVersion[key] = mutationId;
        });
        return mutationId;
      },
      isNodeMutationCurrent(names, mutationId) {
        const keys = this.normalizeNodeNameList(names)
          .map(name => this.normalizeNodeKey(name))
          .filter(Boolean);
        return keys.length > 0 && keys.every(key => this.nodeMutationVersion[key] === mutationId);
      },
      async rollbackNodesState(message) {
        try {
          await this.loadNodes();
        } catch (rollbackErr) {
          console.error('loadNodes rollback failed', rollbackErr);
          this.showMessage(message + '；自动回滚失败，请检查网络后手动刷新页面', { tone: 'error', modal: true });
          return;
        }
        this.showMessage(message, { tone: 'error', modal: true });
      },
      createLineId() {
        return 'line-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
      },
      buildDefaultLineName(index) {
        return '线路' + (Number(index) + 1);
      },
      getNextDefaultLineName(lines = []) {
        const usedNames = new Set((Array.isArray(lines) ? lines : []).map(line => String(line?.name || '').trim()));
        let cursor = 1;
        while (usedNames.has('线路' + cursor)) cursor += 1;
        return '线路' + cursor;
      },
      normalizeSingleTarget(value) {
        const raw = String(value || '').trim();
        if (!raw) return '';
        try {
          const url = new URL(raw);
          if (url.protocol !== 'http:' && url.protocol !== 'https:') return '';
          return url.toString().replace(/\\/$/, '');
        } catch {
          return '';
        }
      },
      validateSingleTarget(value) {
        return !!this.normalizeSingleTarget(value);
      },
      normalizeNodeLines(lines, fallbackTarget = '') {
        const sourceLines = Array.isArray(lines) && lines.length
          ? lines
          : String(fallbackTarget || '')
              .split(',')
              .map(item => item.trim())
              .filter(Boolean)
              .map((target, index) => ({
                id: 'line-' + (index + 1),
                name: this.buildDefaultLineName(index),
                target
              }));
        if (!sourceLines.length) return [];

        const result = [];
        const usedIds = new Set();
        sourceLines.forEach((item, index) => {
          const line = item && typeof item === 'object' && !Array.isArray(item) ? item : { target: item };
          const target = this.normalizeSingleTarget(line?.target);
          if (!target) return;
          let nextId = this.normalizeNodeKey(line?.id) || ('line-' + (index + 1));
          let suffix = 2;
          while (usedIds.has(nextId)) {
            nextId = (this.normalizeNodeKey(line?.id) || ('line-' + (index + 1))) + '-' + suffix;
            suffix += 1;
          }
          usedIds.add(nextId);
          const latencyValue = Number(line?.latencyMs);
          const checkedAt = line?.latencyUpdatedAt ? new Date(line.latencyUpdatedAt) : null;
          result.push({
            id: nextId,
            name: String(line?.name || '').trim() || this.buildDefaultLineName(index),
            target,
            latencyMs: Number.isFinite(latencyValue) && latencyValue >= 0 ? Math.round(latencyValue) : null,
            latencyUpdatedAt: checkedAt && Number.isFinite(checkedAt.getTime()) ? checkedAt.toISOString() : ''
          });
        });
        return result;
      },
      buildLegacyTargetFromLines(lines = []) {
        return (Array.isArray(lines) ? lines : [])
          .map(line => String(line?.target || '').trim())
          .filter(Boolean)
          .join(',');
      },
      resolveActiveLineId(activeLineId, lines = []) {
        const normalizedId = this.normalizeNodeKey(activeLineId);
        if (normalizedId && lines.some(line => String(line?.id || '') === normalizedId)) return normalizedId;
        return lines[0]?.id || '';
      },
      getNodeLines(node) {
        return this.normalizeNodeLines(node?.lines, node?.target || '');
      },
      getActiveNodeLine(node) {
        const lines = this.getNodeLines(node);
        if (!lines.length) return null;
        const activeLineId = this.resolveActiveLineId(node?.activeLineId, lines);
        return lines.find(line => line.id === activeLineId) || lines[0];
      },
      hydrateNode(node) {
        if (!node || typeof node !== 'object') return node;
        const lines = this.getNodeLines(node);
        const activeLineId = this.resolveActiveLineId(node.activeLineId, lines);
        return {
          ...node,
          lines,
          activeLineId,
          target: this.buildLegacyTargetFromLines(lines)
        };
      },
      upsertNode(nextNode) {
        if (!nextNode?.name) return;
        const hydratedNode = this.hydrateNode(nextNode);
        const nextKey = this.normalizeNodeKey(hydratedNode.name);
        const index = this.nodes.findIndex(node => this.normalizeNodeKey(node?.name) === nextKey);
        if (index > -1) this.nodes[index] = hydratedNode;
        else this.nodes.push(hydratedNode);
      },
      formatLatency(ms) {
        const latency = Number(ms);
        if (!Number.isFinite(latency)) return '--';
        return latency > 5000 ? 'Timeout' : (Math.round(latency) + ' ms');
      },
      getNodeLatencyMeta(ms, healthCount = 0) {
        const latency = Number(ms);
        const overloaded = Number(healthCount) > 3;
        if (!Number.isFinite(latency)) {
          return {
            dotClass: 'bg-slate-200 dark:bg-slate-700',
            textClass: 'text-slate-500 dark:text-slate-400 font-medium',
            text: '--',
            titleClass: overloaded ? 'text-red-600 dark:text-red-400' : ''
          };
        }
        if (latency <= 150) {
          return {
            dotClass: 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.6)] dark:shadow-[0_0_8px_rgba(52,211,153,0.4)]',
            textClass: 'text-emerald-600 dark:text-emerald-400 font-medium',
            text: this.formatLatency(latency),
            titleClass: overloaded ? 'text-red-600 dark:text-red-400' : ''
          };
        }
        if (latency <= 200) {
          return {
            dotClass: 'bg-amber-500 shadow-[0_0_8px_rgba(245,158,11,0.6)] dark:shadow-[0_0_8px_rgba(251,191,36,0.4)]',
            textClass: 'text-amber-600 dark:text-amber-400 font-medium',
            text: this.formatLatency(latency),
            titleClass: overloaded ? 'text-red-600 dark:text-red-400' : ''
          };
        }
        if (latency <= 300) {
          return {
            dotClass: 'bg-orange-500 shadow-[0_0_8px_rgba(249,115,22,0.6)] dark:shadow-[0_0_8px_rgba(251,146,60,0.4)]',
            textClass: 'text-orange-600 dark:text-orange-400 font-medium',
            text: this.formatLatency(latency),
            titleClass: overloaded ? 'text-red-600 dark:text-red-400' : ''
          };
        }
        return {
          dotClass: 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)] dark:shadow-[0_0_8px_rgba(248,113,113,0.4)]',
          textClass: 'text-red-600 dark:text-red-400 font-medium',
          text: this.formatLatency(latency),
          titleClass: overloaded ? 'text-red-600 dark:text-red-400' : ''
        };
      },
      getFilteredNodes() {
        const keyword = String(this.nodeSearchKeyword || '').trim().toLowerCase();
        return (Array.isArray(this.nodes) ? this.nodes : [])
          .map(node => this.hydrateNode(node))
          .filter(node => node && typeof node === 'object')
          .filter(n => {
            const nodeName = String(n.name || '').trim();
            const displayName = String(n.displayName || n.name || '').trim();
            const tagText = String(n.tag || '').trim();
            const remarkText = String(n.remark || '').trim();
            if (!nodeName && !displayName) return false;
            if (!keyword) return true;
            const lineNames = this.getNodeLines(n).map(line => String(line?.name || '')).join(' ').toLowerCase();
            return nodeName.toLowerCase().includes(keyword)
              || displayName.toLowerCase().includes(keyword)
              || tagText.toLowerCase().includes(keyword)
              || remarkText.toLowerCase().includes(keyword)
              || lineNames.includes(keyword);
          });
      },
      sortLinesByLatency(lines = []) {
        return (Array.isArray(lines) ? lines : [])
          .map((line, index) => ({ line, index }))
          .sort((left, right) => {
            const leftMs = Number.isFinite(left.line?.latencyMs) ? left.line.latencyMs : Number.POSITIVE_INFINITY;
            const rightMs = Number.isFinite(right.line?.latencyMs) ? right.line.latencyMs : Number.POSITIVE_INFINITY;
            if (leftMs !== rightMs) return leftMs - rightMs;
            return left.index - right.index;
          })
          .map(item => item.line);
      },
      isNodePanelPingAutoSortEnabled() {
        return this.getEffectiveSettingValue('nodePanelPingAutoSort') === true;
      },
      buildActiveLinePingPayload(nodeOrName) {
        const node = typeof nodeOrName === 'string'
          ? this.nodes.find(item => this.normalizeNodeKey(item?.name) === this.normalizeNodeKey(nodeOrName))
          : nodeOrName;
        const payload = { name: typeof nodeOrName === 'string' ? nodeOrName : String(node?.name || '') };
        const activeLineId = this.getActiveNodeLine(node)?.id || '';
        if (activeLineId) {
          payload.lineId = activeLineId;
          payload.silent = true;
        }
        return payload;
      },
      clearNodeLineDragState() {
        this.nodeLineDragId = '';
        this.nodeLineDropHint = null;
      },
      readDesktopViewportMatch() {
        return uiBrowserBridge.readDesktopViewportMatch();
      },
      syncViewportState(hash = '', forceDesktopMatch = null) {
        const nextDesktop = typeof forceDesktopMatch === 'boolean'
          ? forceDesktopMatch
          : this.readDesktopViewportMatch();
        this.isDesktopViewport = nextDesktop === true;
        const targetHash = String(hash || this.getCurrentRouteHash());
        this.syncSettingsSplitLayout(targetHash);
        if (!this.isDesktopViewport) this.sidebarOpen = false;
        return this.isDesktopViewport;
      },
      isDesktopNodeLineDragEnabled() {
        return this.isDesktopViewport === true;
      },
      getNodeModalLineRowClass(lineId) {
        const isDragging = this.nodeLineDragId === lineId;
        const dropPlacement = this.nodeLineDropHint?.lineId === lineId ? this.nodeLineDropHint.placement : '';
        return [
          'rounded-2xl border border-slate-200 dark:border-slate-800 bg-white/90 dark:bg-slate-900/90 p-3 transition',
          isDragging ? 'opacity-60 ring-2 ring-brand-200 dark:ring-brand-500/20' : '',
          dropPlacement === 'before' ? 'border-t-brand-500 border-t-4 pt-[10px]' : '',
          dropPlacement === 'after' ? 'border-b-brand-500 border-b-4 pb-[10px]' : '',
          this.isDesktopNodeLineDragEnabled() ? 'md:cursor-grab' : ''
        ].filter(Boolean).join(' ');
      },
      moveNodeLineTo(lineId, targetLineId, placement = 'before') {
        const fromIndex = this.nodeModalLines.findIndex(line => line.id === lineId);
        const targetIndex = this.nodeModalLines.findIndex(line => line.id === targetLineId);
        if (fromIndex < 0 || targetIndex < 0 || fromIndex === targetIndex) return;
        const [line] = this.nodeModalLines.splice(fromIndex, 1);
        const adjustedTargetIndex = fromIndex < targetIndex ? targetIndex - 1 : targetIndex;
        const insertIndex = placement === 'after' ? adjustedTargetIndex + 1 : adjustedTargetIndex;
        this.nodeModalLines.splice(insertIndex, 0, line);
      },
      async pingAllNodeLinesInModal(event) {
        const validLines = this.nodeModalLines.filter(line => this.validateSingleTarget(line?.target));
        const autoSortEnabled = this.isNodePanelPingAutoSortEnabled();
        if (!validLines.length) {
          this.showMessage('请先至少填写一条有效的 http/https 目标源站', { tone: 'warning' });
          return;
        }
        this.nodeModalPingAllPending = true;
        this.nodeModalPingAllText = '测试中...';
        try {
          const timeout = Number(this.getEffectiveSettingValue('pingTimeout')) || UI_DEFAULTS.pingTimeout;
          for (let index = 0; index < validLines.length; index++) {
            const line = validLines[index];
            this.nodeModalPingAllText = '测试中 ' + (index + 1) + '/' + validLines.length;
            try {
              const normalizedTarget = this.normalizeSingleTarget(line.target);
              const res = await this.apiCall('pingNode', { target: normalizedTarget, timeout, forceRefresh: true });
              line.target = normalizedTarget;
              line.latencyMs = Number(res?.ms);
              line.latencyUpdatedAt = new Date().toISOString();
            } catch {
              line.latencyMs = 9999;
              line.latencyUpdatedAt = new Date().toISOString();
            }
          }
          if (autoSortEnabled) {
            this.nodeModalLines = this.sortLinesByLatency(this.nodeModalLines);
            this.syncNodeModalLinesState(this.nodeModalLines[0]?.id || '');
          } else {
            this.syncNodeModalLinesState();
          }
        } finally {
          this.nodeModalPingAllPending = false;
          this.nodeModalPingAllText = '一键测试延迟';
        }
      },

      getSourceDirectNodesSummaryText() {
        const total = Array.isArray(this.nodes) ? this.nodes.length : 0;
        const selectedCount = this.normalizeNodeNameList(this.settingsSourceDirectNodes).length;
        return total ? ('已选 ' + selectedCount + ' / ' + total + ' 个节点作为源站直连') : ('已选 ' + selectedCount + ' 个节点');
      },

      getFilteredSourceDirectNodes() {
        const keyword = String(this.settingsDirectNodeSearch || '').trim().toLowerCase();
        const nodes = Array.isArray(this.nodes) ? this.nodes.slice() : [];
        return nodes
          .filter(node => {
            if (!keyword) return true;
            const haystack = (String(node?.displayName || '') + ' ' + String(node?.name || '') + ' ' + String(node?.tag || '') + ' ' + String(node?.remark || '')).toLowerCase();
            return haystack.includes(keyword);
          })
          .sort((a, b) => String(a?.name || '').localeCompare(String(b?.name || ''), 'zh-Hans-CN'));
      },

      isSourceDirectNodeSelected(nodeName) {
        const normalized = String(nodeName || '').trim().toLowerCase();
        return this.normalizeNodeNameList(this.settingsSourceDirectNodes).some(name => String(name || '').trim().toLowerCase() === normalized);
      },

      toggleSourceDirectNode(nodeName, checked) {
        const currentSet = new Set(this.normalizeNodeNameList(this.settingsSourceDirectNodes).map(name => String(name || '').trim().toLowerCase()));
        const originalNames = new Map(this.normalizeNodeNameList(this.settingsSourceDirectNodes).map(name => [String(name || '').trim().toLowerCase(), name]));
        const normalized = String(nodeName || '').trim().toLowerCase();
        if (!normalized) return;
        if (checked) {
          currentSet.add(normalized);
          originalNames.set(normalized, String(nodeName || '').trim());
        } else {
          currentSet.delete(normalized);
          originalNames.delete(normalized);
        }
        this.settingsSourceDirectNodes = Array.from(currentSet).map(key => originalNames.get(key) || key);
      },

      syncSourceDirectNodesSelection(selectedNames) {
        if (selectedNames !== undefined) {
          this.settingsSourceDirectNodes = this.normalizeNodeNameList(selectedNames);
        } else {
          this.settingsSourceDirectNodes = this.normalizeNodeNameList(this.settingsSourceDirectNodes);
        }
      },

      validateTargets(targetValue) {
        const targets = String(targetValue || "").split(",").map(function (item) { return item.trim(); }).filter(Boolean);
        if (!targets.length) return false;
        return targets.every(item => this.validateSingleTarget(item));
      },
      ensureNodeModalLines(lines = [], fallbackTarget = '') {
        const normalized = this.normalizeNodeLines(lines, fallbackTarget);
        this.nodeModalLines = normalized.length
          ? normalized
          : [{
              id: this.createLineId(),
              name: this.buildDefaultLineName(0),
              target: '',
              latencyMs: null,
              latencyUpdatedAt: ''
            }];
        return this.nodeModalLines;
      },
      syncNodeModalActiveLine(preferredId = '') {
        const nextId = this.resolveActiveLineId(preferredId || this.nodeModalForm.activeLineId, this.nodeModalLines);
        this.nodeModalForm.activeLineId = nextId;
        return nextId;
      },
      syncNodeModalLinesState(preferredId = '') {
        if (!Array.isArray(this.nodeModalLines) || !this.nodeModalLines.length) this.ensureNodeModalLines();
        this.syncNodeModalActiveLine(preferredId);
      },
      addNodeLine() {
        if (!Array.isArray(this.nodeModalLines)) this.nodeModalLines = [];
        this.nodeModalLines.push({
          id: this.createLineId(),
          name: this.getNextDefaultLineName(this.nodeModalLines),
          target: '',
          latencyMs: null,
          latencyUpdatedAt: ''
        });
        this.syncNodeModalLinesState();
      },
      moveNodeLine(lineId, delta) {
        const index = this.nodeModalLines.findIndex(line => line.id === lineId);
        const nextIndex = index + delta;
        if (index < 0 || nextIndex < 0 || nextIndex >= this.nodeModalLines.length) return;
        const [line] = this.nodeModalLines.splice(index, 1);
        this.nodeModalLines.splice(nextIndex, 0, line);
      },
      removeNodeLine(lineId) {
        this.nodeModalLines = this.nodeModalLines.filter(line => line.id !== lineId);
        if (!this.nodeModalLines.length) {
          this.ensureNodeModalLines();
        }
        if (this.nodeModalForm.activeLineId === lineId) {
          this.nodeModalForm.activeLineId = this.nodeModalLines[0]?.id || '';
        }
        this.syncNodeModalLinesState();
      },
      async promptLogin() {
        if (this.loginPromise) return this.loginPromise;
        this.loginPromise = (async () => {
          const pass = await this.askPrompt({
            title: '管理员登录',
            message: '请输入管理员密码：',
            placeholder: '请输入管理员密码',
            inputType: 'password',
            confirmText: '登录',
            cancelText: '取消',
            required: true,
            tone: 'info'
          });
          if (!pass) throw new Error("LOGIN_CANCELLED");
          const res = await fetch(ADMIN_LOGIN_PATH, {
            method: "POST",
            credentials: "same-origin",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ password: pass })
          });
          const data = await res.json().catch(function () { return {}; });
          if (!res.ok || (!data.ok && !data.token)) throw new Error((data.error && data.error.message) || "登录失败");
          return true;
        })();
        try { return await this.loginPromise; } finally { this.loginPromise = null; }
      },

      handleNodeModalDisplayNameInput() {
        const previousDisplayName = String(this.nodeModalLastDisplayName || '');
        const nextDisplayName = String(this.nodeModalForm.displayName || '');
        const currentPath = String(this.nodeModalForm.name || '');
        if (!this.nodeModalPathManual && (!currentPath || currentPath === previousDisplayName)) {
          this.nodeModalForm.name = nextDisplayName;
        }
        this.nodeModalLastDisplayName = nextDisplayName;
      },

      handleNodeModalPathInput() {
        const currentPath = String(this.nodeModalForm.name || '');
        const currentDisplayName = String(this.nodeModalForm.displayName || '');
        this.nodeModalPathManual = !!currentPath && currentPath !== currentDisplayName;
      },

      handleNodeModalCancel(event) {
        event?.preventDefault?.();
        this.closeNodeModal();
      },

      handleNodeModalNativeClose() {
        this.nodeModalOpen = false;
      },
      
      init() {
        const defaultLogRange = getDefaultLogDateRange();
        if (!this.logStartDate) this.logStartDate = defaultLogRange.startDate;
        if (!this.logEndDate) this.logEndDate = defaultLogRange.endDate;
        this.isDarkTheme = uiBrowserBridge.resolveDarkTheme();
        this.route(this.getCurrentRouteHash());
      },

      toggleTheme() {
        const nextTheme = !this.isDarkTheme;
        this.isDarkTheme = nextTheme;
        uiBrowserBridge.persistTheme(nextTheme);
      },

      navigate(hash) {
        const nextHash = String(hash || '').trim() || '#dashboard';
        if (this.getCurrentRouteHash() === nextHash) {
          this.route(nextHash);
          return;
        }
        uiBrowserBridge.writeHash(nextHash);
      },

      getNavItemClass(hash) {
        if (String(hash || '') === String(this.currentHash || '#dashboard')) {
          return 'bg-brand-50 text-brand-600 dark:bg-brand-500/10 dark:text-brand-400';
        }
        return '';
      },

      escapeHtml(value) {
        return String(value == null ? '' : value)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      },

      formatLocalDateTime(value) {
        if (!value) return '未记录';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return String(value);
        return date.toLocaleString('zh-CN', {
          hour12: false,
          timeZone: 'Asia/Shanghai'
        });
      },

      summarizeRuntimeTimestamp(value, prefix) {
        if (!value) return '';
        return prefix + this.formatLocalDateTime(value);
      },

      getDashboardBadgeClass(tone = 'slate') {
        const palette = {
          emerald: 'bg-emerald-50 text-emerald-600 dark:bg-emerald-500/10 dark:text-emerald-400',
          blue: 'bg-blue-50 text-blue-600 dark:bg-blue-500/10 dark:text-blue-400',
          amber: 'bg-amber-50 text-amber-600 dark:bg-amber-500/10 dark:text-amber-400',
          red: 'bg-red-50 text-red-600 dark:bg-red-500/10 dark:text-red-400',
          slate: 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300'
        };
        return palette[tone] || palette.slate;
      },

      buildDashboardBadge(label, tone = 'slate') {
        return { label: String(label || '').trim(), tone: String(tone || 'slate') || 'slate' };
      },

      buildDashboardBadges(items) {
        const badges = (Array.isArray(items) ? items : [])
          .filter(item => item && item.label)
          .map(item => this.buildDashboardBadge(item.label, item.tone));
        return badges.length ? badges : [this.buildDashboardBadge('待加载', 'slate')];
      },

      getRequestSourceBadge(data) {
        const source = String(data?.requestSource || '').toLowerCase();
        if (source === 'workers_usage') return { label: '请求口径: Workers Usage', tone: 'emerald' };
        if (source === 'zone_analytics') return { label: '请求口径: Zone Analytics', tone: 'blue' };
        if (source === 'd1_logs') return { label: '请求口径: D1 兜底', tone: 'amber' };
        return { label: '请求口径: 待确认', tone: 'slate' };
      },

      getTrafficStatusBadge(data) {
        if (data?.cfAnalyticsLoaded) return { label: '流量状态: Cloudflare 正常', tone: 'emerald' };
        const status = String(data?.cfAnalyticsStatus || '');
        if (status.includes('未配置')) return { label: '流量状态: 未配置', tone: 'amber' };
        if (status.includes('失败') || data?.cfAnalyticsError) return { label: '流量状态: 查询失败', tone: 'red' };
        return { label: '流量状态: 降级/未知', tone: 'slate' };
      },

      getStatsFreshnessBadge(data) {
        const cacheStatus = String(data?.cacheStatus || 'live').toLowerCase();
        if (cacheStatus === 'cache') return { label: '统计快照: 缓存命中', tone: 'blue' };
        return { label: '统计快照: 实时汇总', tone: 'emerald' };
      },

      getRuntimeStatusMeta(status) {
        const key = String(status || 'idle').toLowerCase();
        if (key === 'success') return { label: '正常', badgeClass: 'bg-emerald-50 text-emerald-600 dark:bg-emerald-500/10 dark:text-emerald-400', dotClass: 'bg-emerald-500' };
        if (key === 'running') return { label: '运行中', badgeClass: 'bg-blue-50 text-blue-600 dark:bg-blue-500/10 dark:text-blue-400', dotClass: 'bg-blue-500' };
        if (key === 'partial_failure') return { label: '部分失败', badgeClass: 'bg-amber-50 text-amber-600 dark:bg-amber-500/10 dark:text-amber-400', dotClass: 'bg-amber-500' };
        if (key === 'failed') return { label: '失败', badgeClass: 'bg-red-50 text-red-600 dark:bg-red-500/10 dark:text-red-400', dotClass: 'bg-red-500' };
        if (key === 'skipped') return { label: '已跳过', badgeClass: 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300', dotClass: 'bg-slate-400' };
        return { label: '待记录', badgeClass: 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300', dotClass: 'bg-slate-400' };
      },

      formatRuntimeStateText(status) {
        return this.getRuntimeStatusMeta(status).label;
      },

      buildRuntimeStatusCard(title, status, summary, lines = [], detail = '') {
        return {
          title: String(title || '').trim() || '运行状态',
          status: String(status || 'idle').trim() || 'idle',
          summary: String(summary || '暂无运行记录'),
          lines: (Array.isArray(lines) ? lines : []).filter(Boolean).map(line => String(line)),
          detail: String(detail || '')
        };
      },

      applyRuntimeStatusState(statusPayload) {
        const status = statusPayload && typeof statusPayload === 'object' ? statusPayload : {};
        this.runtimeStatus = status;
        this.dashboardRuntimeView.updatedText = '最近同步：' + this.formatLocalDateTime(status.updatedAt);

        const log = status.log && typeof status.log === 'object' ? status.log : {};
        const logSummary = this.summarizeRuntimeTimestamp(log.lastFlushAt || log.lastFlushErrorAt || log.lastOverflowAt, '最近日志事件：');
        const logLines = [
          log.lastFlushAt ? ('最近成功写入：' + this.formatLocalDateTime(log.lastFlushAt)) : '',
          Number.isFinite(Number(log.lastFlushCount)) ? ('最近写入批次：' + Number(log.lastFlushCount) + ' 条') : '',
          Number.isFinite(Number(log.queueLengthAfterFlush)) ? ('写入后队列长度：' + Number(log.queueLengthAfterFlush)) : '',
          log.lastOverflowAt ? ('最近队列溢出：' + this.formatLocalDateTime(log.lastOverflowAt) + '，丢弃 ' + (Number(log.lastOverflowDropCount) || 0) + ' 条') : ''
        ].filter(Boolean);
        const logDetail = log.lastFlushError ? ('最近写入错误：' + log.lastFlushError) : '';
        this.dashboardRuntimeView.logCard = this.buildRuntimeStatusCard('日志写入', log.lastFlushStatus || (log.lastOverflowAt ? 'partial_failure' : 'idle'), logSummary, logLines, logDetail);

        const scheduled = status.scheduled && typeof status.scheduled === 'object' ? status.scheduled : {};
        const cleanup = scheduled.cleanup && typeof scheduled.cleanup === 'object' ? scheduled.cleanup : {};
        const report = scheduled.report && typeof scheduled.report === 'object' ? scheduled.report : {};
        const alerts = scheduled.alerts && typeof scheduled.alerts === 'object' ? scheduled.alerts : {};
        const scheduledSummary = this.summarizeRuntimeTimestamp(scheduled.lastFinishedAt || scheduled.lastStartedAt || scheduled.lastErrorAt, '最近调度：');
        const scheduledLines = [
          scheduled.lastStartedAt ? ('最近开始：' + this.formatLocalDateTime(scheduled.lastStartedAt)) : '',
          scheduled.lastFinishedAt ? ('最近结束：' + this.formatLocalDateTime(scheduled.lastFinishedAt)) : '',
          cleanup.status ? ('日志清理：' + this.formatRuntimeStateText(cleanup.status) + (cleanup.lastSuccessAt ? '（' + this.formatLocalDateTime(cleanup.lastSuccessAt) + '）' : cleanup.lastSkippedAt ? '（' + this.formatLocalDateTime(cleanup.lastSkippedAt) + '）' : cleanup.lastErrorAt ? '（' + this.formatLocalDateTime(cleanup.lastErrorAt) + '）' : '')) : '',
          report.status ? ('日报发送：' + this.formatRuntimeStateText(report.status) + (report.lastSuccessAt ? '（' + this.formatLocalDateTime(report.lastSuccessAt) + '）' : report.lastSkippedAt ? '（' + this.formatLocalDateTime(report.lastSkippedAt) + '）' : report.lastErrorAt ? '（' + this.formatLocalDateTime(report.lastErrorAt) + '）' : '')) : '',
          alerts.status ? ('异常告警：' + this.formatRuntimeStateText(alerts.status) + (alerts.lastSuccessAt ? '（' + this.formatLocalDateTime(alerts.lastSuccessAt) + '）' : alerts.lastSkippedAt ? '（' + this.formatLocalDateTime(alerts.lastSkippedAt) + '）' : alerts.lastErrorAt ? '（' + this.formatLocalDateTime(alerts.lastErrorAt) + '）' : '')) : ''
        ].filter(Boolean);
        const scheduledDetail = scheduled.lastError || cleanup.lastError || report.lastError || alerts.lastError
          ? ('最近调度错误：' + (scheduled.lastError || cleanup.lastError || report.lastError || alerts.lastError))
          : '';
        this.dashboardRuntimeView.scheduledCard = this.buildRuntimeStatusCard('定时任务', scheduled.status || 'idle', scheduledSummary, scheduledLines, scheduledDetail);
      },

      applyRuntimeStatusErrorState(message) {
        this.dashboardRuntimeView.updatedText = '最近同步：运行状态加载失败';
        const errorMessage = message || '未知错误';
        this.dashboardRuntimeView.logCard = this.buildRuntimeStatusCard('日志写入', 'failed', '运行状态接口暂时不可用', [], errorMessage);
        this.dashboardRuntimeView.scheduledCard = this.buildRuntimeStatusCard('定时任务', 'failed', '运行状态接口暂时不可用', [], errorMessage);
      },
      
      async apiCall(action, payload={}) {
          const headers = {'Content-Type': 'application/json'};
          if (String(action || '') === 'updateDnsRecord') headers['X-Admin-Confirm'] = 'updateDnsRecord';
          const requestInit = {
              method: 'POST',
              credentials: 'same-origin',
              headers,
              body: JSON.stringify({action, ...payload})
          };
          let res = await fetch(ADMIN_PATH, requestInit);
          if (res.status === 401) {
              await this.promptLogin();
              res = await fetch(ADMIN_PATH, requestInit);
          }
          const data = await res.json().catch(() => ({}));
          if (!res.ok) {
              const error = new Error(data.error?.message || ('HTTP ' + res.status));
              error.code = data.error?.code || null;
              error.status = res.status;
              throw error;
          }
          return data;
      },

      toggleSidebar() {
        this.sidebarOpen = !this.sidebarOpen;
      },
      
      route(forcedHash = '') {
        const hash = forcedHash || this.getCurrentRouteHash();
        this.currentHash = hash;
        this.pageTitle = VIEW_TITLES[hash] || 'Emby Proxy';
        this.syncViewportState(hash);

        if (hash === '#dashboard') this.loadDashboard();
        if (hash === '#nodes') this.loadNodes();
        if (hash === '#logs') this.loadLogs(1);
        if (hash === '#dns') this.loadDnsRecords();
        if (hash === '#settings') this.loadSettings();
      },

      syncSettingsSplitLayout(hash) {
        const isDesktopSettings = hash === '#settings' && this.isDesktopViewport === true;
        this.isDesktopSettingsLayout = isDesktopSettings;
        if (!isDesktopSettings) return;
        this.contentScrollResetKey += 1;
        this.settingsScrollResetKey += 1;
      },

      getSettingsTabClass(id) {
        return this.activeSettingsTab === id
          ? 'border-brand-200/80 bg-brand-50 text-brand-600 dark:border-brand-500/20 dark:bg-brand-500/10 dark:text-brand-400'
          : 'border-transparent bg-transparent text-slate-500 dark:text-slate-400 hover:bg-slate-100 hover:border-slate-200 hover:text-slate-900 dark:hover:bg-slate-900 dark:hover:border-slate-700 dark:hover:text-white';
      },

      switchSetTab(id) {
        this.activeSettingsTab = id;
        this.settingsScrollResetKey += 1;
      },

      applyDashboardStatsState(data) {
         const requestTitle = [data.requestSourceText || '', data.cfAnalyticsDetail || ''].filter(Boolean).join(' | ');
         this.dashboardView.requests.count = String(data.todayRequests || 0);
         this.dashboardView.requests.hint = data.requestSourceText || '今日请求量口径：未知';
         this.dashboardView.requests.title = requestTitle;
         this.dashboardView.requests.badges = this.buildDashboardBadges([
           this.getRequestSourceBadge(data),
           this.getStatsFreshnessBadge(data)
         ]);
         this.dashboardView.requests.embyMetrics = '请求: 播放请求 ' + (data.playCount || 0) + ' 次 | 获取播放信息 ' + (data.infoCount || 0) + ' 次';

         const trafficTitle = [data.trafficSourceText || '', data.cfAnalyticsStatus || '', data.cfAnalyticsError || '', data.cfAnalyticsDetail || ''].filter(Boolean).join(' | ');
         this.dashboardView.traffic.count = data.todayTraffic || '0 B';
         this.dashboardView.traffic.hint = data.trafficSourceText || data.cfAnalyticsStatus || data.cfAnalyticsError || ' ';
         this.dashboardView.traffic.title = trafficTitle;
         this.dashboardView.traffic.detail = [data.cfAnalyticsStatus, data.cfAnalyticsError, data.cfAnalyticsDetail].filter(Boolean).join('\\n') || ' ';
         this.dashboardView.traffic.badges = this.buildDashboardBadges([
           this.getTrafficStatusBadge(data),
           this.getStatsFreshnessBadge(data)
         ]);

         this.dashboardView.nodes.count = String(data.nodeCount || 0);
         this.dashboardView.nodes.meta = '统计时间：' + this.formatLocalDateTime(data.generatedAt);
         this.dashboardView.nodes.badges = this.buildDashboardBadges([
           { label: '节点索引: 已加载', tone: 'emerald' },
           this.getStatsFreshnessBadge(data)
         ]);
         this.dashboardSeries = Array.isArray(data.hourlySeries) ? data.hourlySeries : [];
      },

      applyDashboardErrorState(message) {
         this.dashboardView.requests.count = '0';
         this.dashboardView.requests.hint = '加载仪表盘失败';
         this.dashboardView.requests.title = '';
         this.dashboardView.requests.badges = this.buildDashboardBadges([{ label: '请求口径: 加载失败', tone: 'red' }]);
         this.dashboardView.requests.embyMetrics = '请求: 播放请求 0 次 | 获取播放信息 0 次';
         this.dashboardView.traffic.count = '0 B';
         this.dashboardView.traffic.hint = '加载仪表盘失败';
         this.dashboardView.traffic.title = '';
         this.dashboardView.traffic.detail = message || '未知错误';
         this.dashboardView.traffic.badges = this.buildDashboardBadges([{ label: '流量状态: 加载失败', tone: 'red' }]);
         this.dashboardView.nodes.count = '0';
         this.dashboardView.nodes.meta = '统计时间：不可用';
         this.dashboardView.nodes.badges = this.buildDashboardBadges([{ label: '节点索引: 未确认', tone: 'red' }]);
         this.dashboardSeries = [];
      },

      async loadDashboard() {
         const loadSeq = ++this.dashboardLoadSeq;
         const [statsResult, runtimeResult] = await Promise.allSettled([
           this.apiCall('getDashboardStats'),
           this.apiCall('getRuntimeStatus')
         ]);
         if (loadSeq !== this.dashboardLoadSeq) return;

         if (statsResult.status === 'fulfilled') {
           this.applyDashboardStatsState(statsResult.value);
         } else {
           this.applyDashboardErrorState(statsResult.reason?.message || '未知错误');
         }

         if (runtimeResult.status === 'fulfilled') {
           this.applyRuntimeStatusState(runtimeResult.value.status || {});
         } else {
           this.applyRuntimeStatusErrorState(runtimeResult.reason?.message || '未知错误');
         }
      },

      async loadSettings() {
          const [configRes, nodesRes, snapshotRes] = await Promise.all([
              this.apiCall('loadConfig'),
              this.apiCall('list').catch(() => ({ nodes: this.nodes || [] })),
              this.apiCall('getConfigSnapshots').catch(() => ({ snapshots: this.configSnapshots || [] }))
          ]);
          const cfg = configRes.config || { enableH2: false, enableH3: false, peakDowngrade: true, protocolFallback: true, sourceSameOriginProxy: true, forceExternalProxy: true };
          this.applyRuntimeConfig(cfg);
          if (Array.isArray(nodesRes.nodes)) this.nodes = nodesRes.nodes.map(node => this.hydrateNode(node));
          this.applyConfigSnapshotsState(snapshotRes.snapshots || []);

          this.applyConfigSectionToForm('ui', cfg);
          this.applyConfigSectionToForm('proxy', cfg);
          this.normalizeSettingsNumberInputs();
          this.syncProxySettingsGuardrails();
          this.settingsDirectNodeSearch = '';
          this.syncSourceDirectNodesSelection(cfg.sourceDirectNodes || cfg.directSourceNodes || cfg.nodeDirectList || []);
          this.applyConfigSectionToForm('security', cfg);
          this.applyConfigSectionToForm('logs', cfg);
          this.applyConfigSectionToForm('account', cfg);
          return cfg;
      },

      applyRecommendedSettings(section) {
          const recommended = RECOMMENDED_SECTION_VALUES[section];
          if (!recommended) return;
          this.applyConfigSectionToForm(section, recommended, { onlyPresent: true });
          this.normalizeSettingsNumberInputs();
          if (section === 'proxy') this.syncProxySettingsGuardrails();
          this.showMessage('推荐生产值已回填到表单，请确认后再点击保存。', { tone: 'success' });
      },

      async saveSettings(section) {
          try {
              const res = await this.apiCall('loadConfig');
              const currentConfig = res.config || {};
              let newConfig = { ...currentConfig };
              
              if (CONFIG_FORM_BINDINGS[section]) {
                  newConfig = { ...newConfig, ...this.collectConfigSectionFromForm(section) };
                  if (section === 'proxy') {
                      newConfig.sourceDirectNodes = this.normalizeNodeNameList(this.settingsSourceDirectNodes);
                  }
              }

              const { sanitizedConfig, preview } = await this.prepareConfigChangePreview(section, currentConfig, newConfig);
              if (!preview.hasChanges) {
                  this.showMessage(preview.message, { tone: 'info', modal: true });
                  return;
              }
              if (!await this.askConfirm(preview.message, { title: '保存设置', tone: 'warning', confirmText: '保存' })) return;

              const saveRes = await this.apiCall('saveConfig', { config: sanitizedConfig, meta: { section, source: 'ui' } });
              await this.finalizePersistedSettings(saveRes.config || sanitizedConfig, {
                  successMessage: '设置已保存，立即生效',
                  partialSuccessPrefix: '设置已保存，但设置面板刷新失败: ',
                  refreshErrorLog: 'loadSettings after saveConfig failed'
              });
          } catch (err) {
              console.error('saveSettings failed', err);
              this.showMessage('设置保存失败: ' + (err?.message || '未知错误'), { tone: 'error', modal: true });
          }
      },
      
      async testTelegram() {
          const botToken = String(this.getEffectiveSettingValue('tgBotToken') || '').trim();
          const chatId = String(this.getEffectiveSettingValue('tgChatId') || '').trim();
          
          if (!botToken || !chatId) {
              this.showMessage("请先填写完整的 Telegram Bot Token 和 Chat ID！", { tone: 'warning' });
              return;
          }
          
          const res = await this.apiCall('testTelegram', { tgBotToken: botToken, tgChatId: chatId });
          if (res.success) {
              this.showMessage("测试通知已发送！请查看您的 Telegram 客户端。", { tone: 'success' });
          } else {
              this.showMessage("发送失败: " + (res.error?.message || "未知网络错误"), { tone: 'error', modal: true });
          }
      },
      
      async sendDailyReport() {
          try {
              const res = await this.apiCall('sendDailyReport');
              if (res.success) {
                  this.showMessage("日报已成功生成并发送到 Telegram！", { tone: 'success' });
              } else {
                  this.showMessage("发送失败: " + (res.error?.message || "未知网络错误"), { tone: 'error', modal: true });
              }
          } catch(e) {
              this.showMessage("发送失败: " + e.message, { tone: 'error', modal: true });
          }
      },

      async purgeCache() {
          const res = await this.apiCall('purgeCache');
          if (res.success) this.showMessage("边缘缓存已成功清空！", { tone: 'success' });
          else this.showMessage("清空失败: " + (res.error?.message || "请检查 Zone ID 和 Token"), { tone: 'error', modal: true });
      },

      async tidyKvData() {
          if (!await this.askConfirm('这会重建 sys:nodes_index、清洗 sys:theme，并删除遗留的 Cloudflare 仪表盘缓存与过期租约键。不会删除 node:* 节点实体，是否继续？', { title: '整理 KV 数据', tone: 'warning', confirmText: '开始整理' })) return;
          const res = await this.apiCall('tidyKvData');
          if (!res.success) {
              this.showMessage('整理失败: ' + (res.error?.message || '未知错误'), { tone: 'error', modal: true });
              return;
          }
          try {
              await this.loadSettings();
          } catch (refreshErr) {
              console.warn('loadSettings after tidyKvData failed', refreshErr);
          }
          const summary = res.summary || {};
          const extraLockText = summary.deletedExpiredScheduledLock ? '，并移除了 1 个过期租约' : '';
          const malformedText = summary.themeWasMalformed ? '；已重写异常的 sys:theme。' : '。';
          this.showMessage('KV 整理完成：重建 ' + (summary.rebuiltNodeCount || 0) + ' 个节点索引，清理 ' + (summary.deletedCacheKeyCount || 0) + ' 个缓存键' + extraLockText + malformedText, { tone: 'success', modal: true });
      },

      async initLogsDbFromUi() {
          await this.apiCall('initLogsDb');
          this.showMessage('初始化完成', { tone: 'success' });
      },

      async clearLogsFromUi() {
          if (!await this.askConfirm('确定清空所有日志?', { title: '清空日志', tone: 'danger', confirmText: '清空' })) return;
          await this.apiCall('clearLogs');
          await this.loadLogs(1);
      },

      async loadNodes() {
          const res = await this.apiCall('list');
          if(res.nodes) { this.nodes = res.nodes.map(node => this.hydrateNode(node)); }
      },

      isNodePingPending(name) {
          const key = this.normalizeNodeKey(name);
          return this.nodePingPending?.[key] === true;
      },

      setNodePingPending(name, pending) {
          const key = this.normalizeNodeKey(name);
          if (!key) return;
          this.nodePingPending = {
            ...(this.nodePingPending || {}),
            [key]: pending === true
          };
      },

      async forceHealthCheck() {
          if (this.nodesHealthCheckPending) return;
          this.nodesHealthCheckPending = true;
          try {
            await this.checkAllNodesHealth();
          } finally {
            this.nodesHealthCheckPending = false;
          }
      },

      async checkSingleNodeHealth(name) {
          if (this.isNodePingPending(name)) return;
          this.setNodePingPending(name, true);
          try {
             const timeout = Number(this.getEffectiveSettingValue('pingTimeout')) || UI_DEFAULTS.pingTimeout;
             const res = await this.apiCall('pingNode', { ...this.buildActiveLinePingPayload(name), timeout, forceRefresh: true });
             if (res?.node) this.upsertNode(res.node);
          } catch(e) {
             this.updateNodeCardStatus(name, 9999);
          } finally {
             this.setNodePingPending(name, false);
          }
      },

      async checkAllNodesHealth() {
          const timeout = Number(this.getEffectiveSettingValue('pingTimeout')) || UI_DEFAULTS.pingTimeout;
          for(let n of this.nodes.slice()) {
             try {
                const res = await this.apiCall('pingNode', { ...this.buildActiveLinePingPayload(n), timeout, forceRefresh: true });
                if (res?.node) this.upsertNode(res.node);
             } catch(e) {
                this.updateNodeCardStatus(n.name, 9999);
             }
          }
      },
      
      updateNodeCardStatus(name, ms) {
          const normalizedName = this.normalizeNodeKey(name);
          const targetNode = this.nodes.find(node => this.normalizeNodeKey(node?.name) === normalizedName);
          if (!targetNode) return;
          const hydratedNode = this.hydrateNode(targetNode);
          const activeLine = this.getActiveNodeLine(hydratedNode);
          if (!activeLine) return;

          const nextLines = this.getNodeLines(hydratedNode).map(line => {
            if (line.id !== activeLine.id) return line;
            return {
              ...line,
              latencyMs: Number.isFinite(Number(ms)) ? Math.round(Number(ms)) : null,
              latencyUpdatedAt: new Date().toISOString()
            };
          });
          this.upsertNode({ ...hydratedNode, lines: nextLines, target: this.buildLegacyTargetFromLines(nextLines) });

          if (ms > 300) this.nodeHealth[name] = (this.nodeHealth[name] || 0) + 1;
          else this.nodeHealth[name] = 0;
      },

      addHeaderRow(key = '', val = '') {
          const nextHeaders = Array.isArray(this.nodeModalForm.headers) ? this.nodeModalForm.headers.slice() : [];
          nextHeaders.push(createNodeModalHeaderRow(key, val));
          this.nodeModalForm = {
            ...this.nodeModalForm,
            headers: nextHeaders
          };
      },

      removeNodeHeaderRow(headerId) {
          this.nodeModalForm = {
            ...this.nodeModalForm,
            headers: (Array.isArray(this.nodeModalForm.headers) ? this.nodeModalForm.headers : []).filter(header => header.id !== headerId)
          };
      },

      showNodeModal(name='') {
        this.nodeModalSubmitting = false;
        this.nodeModalPingAllPending = false;
        this.nodeModalPingAllText = '一键测试延迟';
        this.clearNodeLineDragState();

        let nextForm = createEmptyNodeModalForm();
        if (name) {
            const foundNode = this.nodes.find(x => String(x.name) === String(name));
            if (foundNode) {
                const hydratedNode = this.hydrateNode(foundNode);
                const displayName = String(foundNode.displayName || foundNode.name || '');
                this.ensureNodeModalLines(hydratedNode.lines, hydratedNode.target);
                nextForm = {
                  originalName: String(foundNode.name || ''),
                  displayName,
                  name: String(foundNode.name || ''),
                  tag: String(foundNode.tag || ''),
                  tagColor: String(foundNode.tagColor || 'amber') || 'amber',
                  remark: String(foundNode.remark || ''),
                  secret: String(foundNode.secret || ''),
                  activeLineId: hydratedNode.activeLineId || this.nodeModalLines[0]?.id || '',
                  headers: foundNode.headers && typeof foundNode.headers === 'object'
                    ? Object.entries(foundNode.headers).map(([headerKey, headerValue]) => createNodeModalHeaderRow(headerKey, String(headerValue || '')))
                    : []
                };
            } else {
                this.ensureNodeModalLines();
                nextForm.activeLineId = this.nodeModalLines[0]?.id || '';
            }
        } else {
            this.ensureNodeModalLines();
            nextForm.activeLineId = this.nodeModalLines[0]?.id || '';
            nextForm.headers = [createNodeModalHeaderRow()];
        }

        this.nodeModalForm = nextForm;
        this.nodeModalLastDisplayName = String(nextForm.displayName || '');
        this.nodeModalPathManual = !!String(nextForm.name || '') && String(nextForm.name || '') !== String(nextForm.displayName || '');
        this.syncNodeModalLinesState(nextForm.activeLineId);
        this.nodeModalOpen = true;
      },

      closeNodeModal() {
        this.nodeModalOpen = false;
        this.nodeModalSubmitting = false;
        this.nodeModalPingAllPending = false;
        this.nodeModalPingAllText = '一键测试延迟';
        this.clearNodeLineDragState();
      },
      
      async saveNode() {
          let headersObj = {};
          const headerRows = Array.isArray(this.nodeModalForm.headers) ? this.nodeModalForm.headers : [];
          for (let i = 0; i < headerRows.length; i++) {
              const row = headerRows[i] || {};
              const k = String(row.key || '').trim();
              const v = String(row.value || '').trim();
              if(k) headersObj[k] = v;
          }

          const displayName = String(this.nodeModalForm.displayName || '').trim();
          const nodePath = String(this.nodeModalForm.name || '').trim() || displayName;
          const tagColor = String(this.nodeModalForm.tagColor || 'amber') || 'amber';
          
          const payload = {
              originalName: String(this.nodeModalForm.originalName || ''),
              name: nodePath,
              displayName,
              secret: String(this.nodeModalForm.secret || '').trim(),
              tag: String(this.nodeModalForm.tag || '').trim(),
              tagColor,
              remark: String(this.nodeModalForm.remark || '').trim(),
              headers: headersObj
          };
          if (!payload.name) {
              this.showMessage('节点路径不能为空', { tone: 'warning' });
              return;
          }

          const normalizedLines = [];
          for (let index = 0; index < this.nodeModalLines.length; index++) {
              const rawLine = this.nodeModalLines[index] || {};
              const hasAnyValue = String(rawLine.name || '').trim() || String(rawLine.target || '').trim() || Number.isFinite(Number(rawLine.latencyMs));
              if (!hasAnyValue) continue;
              const target = this.normalizeSingleTarget(rawLine.target);
              if (!target) {
                  this.showMessage('每条线路都必须填写有效的 http/https 目标源站', { tone: 'warning' });
                  return;
              }
              normalizedLines.push({
                  id: this.normalizeNodeKey(rawLine.id) || this.createLineId(),
                  name: String(rawLine.name || '').trim() || this.buildDefaultLineName(index),
                  target,
                  latencyMs: Number.isFinite(Number(rawLine.latencyMs)) ? Math.round(Number(rawLine.latencyMs)) : null,
                  latencyUpdatedAt: rawLine.latencyUpdatedAt || ''
              });
          }

          if (!normalizedLines.length) {
              this.showMessage('至少需要保留一条有效线路', { tone: 'warning' });
              return;
          }
          payload.lines = normalizedLines;
          payload.activeLineId = this.resolveActiveLineId(this.nodeModalForm.activeLineId, normalizedLines);
          payload.target = this.buildLegacyTargetFromLines(normalizedLines);
          this.nodeModalSubmitting = true;

          const originalNameKey = this.normalizeNodeKey(payload.originalName);
          const optimisticNameKey = this.normalizeNodeKey(payload.name);
          const mutationNames = [payload.originalName, payload.name];
          const mutationId = this.markNodeMutation(mutationNames);
          const optimisticIdx = this.nodes.findIndex(n => {
              const currentName = this.normalizeNodeKey(n?.name);
              return currentName === originalNameKey || currentName === optimisticNameKey;
          });
          const previousNode = optimisticIdx > -1 ? this.nodes[optimisticIdx] : null;
          const optimisticNode = {
              ...(previousNode || {}),
              name: payload.name,
              displayName: payload.displayName,
              target: payload.target,
              lines: payload.lines,
              activeLineId: payload.activeLineId,
              secret: payload.secret,
              tag: payload.tag,
              tagColor: payload.tagColor,
              remark: payload.remark,
              headers: payload.headers
          };

          if (optimisticIdx > -1) {
              this.nodes[optimisticIdx] = optimisticNode;
          } else {
              this.nodes.push(optimisticNode);
          }

          this.closeNodeModal();

          this.apiCall('save', payload).then(res => {
              if (!this.isNodeMutationCurrent([...mutationNames, res?.node?.name], mutationId)) return;
              if (res && res.node) {
                  this.upsertNode(res.node);
              }
          }).catch(err => {
              if (!this.isNodeMutationCurrent(mutationNames, mutationId)) return;
              return this.rollbackNodesState('后台同步到 KV 数据库失败: ' + err.message);
          }).finally(() => {
              this.nodeModalSubmitting = false;
          });
      },
      
      async deleteNode(name) {
          if (!await this.askConfirm('删除节点后将立即同步到 KV，是否继续？', { title: '删除节点', tone: 'danger', confirmText: '删除' })) return;
          const normalizedName = this.normalizeNodeKey(name);
          const mutationId = this.markNodeMutation([name]);
          this.nodes = this.nodes.filter(n => String(n?.name || '').trim().toLowerCase() !== normalizedName);

          try {
              await this.apiCall('delete', {name});
          } catch(err) {
              if (!this.isNodeMutationCurrent([name], mutationId)) return;
              await this.rollbackNodesState('后台删除节点失败: ' + err.message);
          }
      },
      
      formatRelativeTime(ts) {
          const diff = Math.floor((Date.now() - ts) / 60000);
          if (diff <= 0) return '刚刚';
          if (diff < 60) return diff + ' 分钟前';
          if (diff < 1440) return Math.floor(diff / 60) + ' 小时前';
          return Math.floor(diff / 1440) + ' 天前';
      },

      formatUtc8ExactTime(ts) {
          const time = Number(ts);
          if (!time) return '-';
          const date = new Date(time + 8 * 3600 * 1000);
          if (Number.isNaN(date.getTime())) return '-';
          const yyyy = date.getUTCFullYear();
          const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
          const dd = String(date.getUTCDate()).padStart(2, '0');
          const hh = String(date.getUTCHours()).padStart(2, '0');
          const mi = String(date.getUTCMinutes()).padStart(2, '0');
          return 'UTC+8 ' + yyyy + '-' + mm + '-' + dd + ' ' + hh + ':' + mi;
      },
      
      updateTimeCones() {
          this.logTimeTick += 1;
      },

      getLogRelativeTime(ts) {
          return this.formatRelativeTime(ts);
      },

      getResourceCategoryBadge(path, category) {
          const p = String(path || "").toLowerCase();
          if (category === 'error') return { label: '请求报错', className: 'text-red-500 bg-red-50 dark:bg-red-500/10 px-2 py-1.5 rounded-lg font-medium' };
          if (category === 'segment' || p.includes('.ts') || p.includes('.m4s')) return { label: '视频流分片', className: 'text-blue-600 bg-blue-50 dark:text-blue-400 dark:bg-blue-500/10 px-2 py-1.5 rounded-lg font-medium' };
          if (category === 'manifest' || p.includes('.m3u8') || p.includes('.mpd')) return { label: '播放列表', className: 'text-purple-600 bg-purple-50 dark:text-purple-400 dark:bg-purple-500/10 px-2 py-1.5 rounded-lg font-medium' };
          if (category === 'stream' || p.includes('.mp4') || p.includes('.mkv') || p.includes('/stream') || p.includes('download=true')) return { label: '视频数据', className: 'text-emerald-600 bg-emerald-50 dark:text-emerald-400 dark:bg-emerald-500/10 px-2 py-1.5 rounded-lg font-medium' };
          if (category === 'image' || p.includes('/images/') || p.includes('/emby/covers/') || p.includes('.jpg') || p.includes('.png')) return { label: '图片海报', className: 'text-amber-600 bg-amber-50 dark:text-amber-400 dark:bg-amber-500/10 px-2 py-1.5 rounded-lg font-medium' };
          if (category === 'subtitle' || p.includes('.srt') || p.includes('.vtt') || p.includes('.ass')) return { label: '字幕文件', className: 'text-indigo-600 bg-indigo-50 dark:text-indigo-400 dark:bg-indigo-500/10 px-2 py-1.5 rounded-lg font-medium' };
          if (category === 'prewarm') return { label: '连接预热', className: 'text-cyan-600 bg-cyan-50 dark:text-cyan-400 dark:bg-cyan-500/10 px-2 py-1.5 rounded-lg font-medium' };
          if (category === 'websocket' || p.includes('websocket')) return { label: '长连接通讯', className: 'text-rose-600 bg-rose-50 dark:text-rose-400 dark:bg-rose-500/10 px-2 py-1.5 rounded-lg font-medium' };
          
          if (p.includes('/sessions/playing')) return { label: '播放状态同步', className: 'text-slate-600 bg-slate-100 dark:text-slate-300 dark:bg-slate-800 px-2 py-1.5 rounded-lg font-medium' };
          if (p.includes('/playbackinfo')) return { label: '播放信息获取', className: 'text-slate-600 bg-slate-100 dark:text-slate-300 dark:bg-slate-800 px-2 py-1.5 rounded-lg font-medium' };
          if (p.includes('/users/authenticate')) return { label: '用户认证', className: 'text-pink-600 bg-pink-50 dark:text-pink-400 dark:bg-pink-500/10 px-2 py-1.5 rounded-lg font-medium' };
          if (p.includes('/items/') || p.includes('/shows/') || p.includes('/movies/') || p.includes('/users/')) return { label: '媒体元数据', className: 'text-slate-600 bg-slate-100 dark:text-slate-300 dark:bg-slate-800 px-2 py-1.5 rounded-lg font-medium' };
          
          return { label: '常规 API', className: 'text-slate-500 bg-slate-50 dark:text-slate-400 dark:bg-slate-800/50 px-2 py-1.5 rounded-lg font-medium' };
      },
      getPlaybackModeBadge(errorDetail) {
          const detail = String(errorDetail || '');
          const match = /Playback=(direct_play|direct_stream|transcode|unknown)/i.exec(detail);
          if (!match) return null;
          const mode = match[1].toLowerCase();
          const badgeMap = {
              direct_play: {
                  label: '直放',
                  className: 'text-emerald-700 bg-emerald-100 dark:text-emerald-300 dark:bg-emerald-500/15'
              },
              direct_stream: {
                  label: '直串',
                  className: 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-500/15'
              },
              transcode: {
                  label: '转码',
                  className: 'text-rose-700 bg-rose-100 dark:text-rose-300 dark:bg-rose-500/15'
              },
              unknown: {
                  label: '未知',
                  className: 'text-slate-600 bg-slate-100 dark:text-slate-300 dark:bg-slate-700/60'
              }
          };
          const meta = badgeMap[mode] || badgeMap.unknown;
          return {
              label: 'Playback · ' + meta.label,
              className: 'px-2 py-1 rounded-lg text-[11px] font-semibold ' + meta.className
          };
      },
      getLogCategoryBadges(log) {
          return [
              this.getResourceCategoryBadge(log?.request_path, log?.category),
              this.getPlaybackModeBadge(log?.error_detail)
          ].filter(Boolean);
      },
      getLogStatusMeta(log) {
          const statusCode = Number(log?.status_code) || 0;
          if (statusCode < 400) {
              return {
                  text: String(statusCode || ''),
                  title: '',
                  className: ''
              };
          }
          const errMap = {
              400: 'Bad Request (请求无效或参数错误)',
              401: 'Unauthorized (未授权，客户端登录失败或缺少凭证)',
              403: 'Forbidden (拒绝访问：命中防火墙、IP黑名单或源站拒绝)',
              404: 'Not Found (目标不存在：节点未找到或上游路径错误)',
              405: 'Method Not Allowed (不允许的请求方法)',
              429: 'Too Many Requests (限流拦截：单 IP 请求过频)',
              500: 'Internal Server Error (源站或代理内部执行报错)',
              502: 'Bad Gateway (网关错误：源站宕机、地址无效或无法连通)',
              503: 'Service Unavailable (服务不可用：源站超载或维护)',
              504: 'Gateway Timeout (网关超时：目标源站无响应)',
              522: 'Connection Timed Out (CF 无法与您的源站建立 TCP 连接)'
          };
          let hint = errMap[statusCode] || ('HTTP 异常码: ' + statusCode);
          if (log?.error_detail) hint += '\\n[抓取详情] ' + log.error_detail;
          return {
              text: String(statusCode),
              title: hint,
              className: 'cursor-help border-b border-dashed border-red-400/70 pb-[1px]'
          };
      },
      getLogPathTitle(log) {
          return log?.error_detail ? (String(log.request_path || '') + '\\n[诊断] ' + log.error_detail) : String(log?.request_path || '');
      },
      getLogsPlaybackFilterClass(mode = '') {
          const active = String(mode || '') === String(this.logsPlaybackModeFilter || '');
          return active
            ? 'bg-brand-50 text-brand-600 dark:bg-brand-500/10 dark:text-brand-400'
            : 'text-slate-500 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800';
      },
      updateLogsPlaybackFilterButtons() {
          return this.logsPlaybackModeFilter;
      },
      setLogsPlaybackModeFilter(mode = '') {
          this.logsPlaybackModeFilter = String(mode || '').trim();
          this.loadLogs(1);
      },

      normalizeLogDateInputValue(value, fallbackValue = '') {
          const text = String(value || '').trim();
          return /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(text) ? text : String(fallbackValue || '');
      },

      ensureLogDateRange() {
          const fallbackRange = getDefaultLogDateRange();
          let startDate = this.normalizeLogDateInputValue(this.logStartDate, fallbackRange.startDate);
          let endDate = this.normalizeLogDateInputValue(this.logEndDate, fallbackRange.endDate);
          if (!startDate) startDate = fallbackRange.startDate;
          if (!endDate) endDate = fallbackRange.endDate;
          if (startDate > endDate) startDate = endDate;
          this.logStartDate = startDate;
          this.logEndDate = endDate;
          return { startDate, endDate };
      },

      // ============================================================================
      // DNS 编辑：读取 / 受限修改（不支持增删）
      // ============================================================================
      isDnsTypeAllowed(type) {
          const upper = String(type || '').toUpperCase();
          return upper === 'A' || upper === 'AAAA' || upper === 'CNAME';
      },

      isDnsRecordDirty(record) {
          if (!record) return false;
          const type = String(record.type || '').toUpperCase();
          const content = String(record.content || '');
          const originalType = String(record._originalType || '').toUpperCase();
          const originalContent = String(record._originalContent || '');
          return type !== originalType || content !== originalContent;
      },

      normalizeDnsHistoryEntries(entries = []) {
          const normalized = [];
          const seen = new Set();
          for (const rawEntry of Array.isArray(entries) ? entries : []) {
              const type = String(rawEntry?.type || '').trim().toUpperCase();
              const content = String(rawEntry?.content || '').trim();
              if (!type || !content) continue;
              const dedupeKey = type + '::' + content.toLowerCase();
              if (seen.has(dedupeKey)) continue;
              seen.add(dedupeKey);
              normalized.push({
                  id: String(rawEntry?.id || ''),
                  name: String(rawEntry?.name || '').trim(),
                  type,
                  content,
                  savedAt: String(rawEntry?.savedAt || rawEntry?.updatedAt || rawEntry?.createdAt || ''),
                  actor: String(rawEntry?.actor || 'admin').trim() || 'admin',
                  source: String(rawEntry?.source || 'ui').trim() || 'ui',
                  requestHost: String(rawEntry?.requestHost || '').trim().toLowerCase()
              });
              if (normalized.length >= this.dnsHistoryLimit) break;
          }
          return normalized;
      },

      formatDnsHistoryTimestamp(value) {
          if (!value) return '未记录';
          const date = new Date(value);
          if (Number.isNaN(date.getTime())) return String(value);
          return date.toLocaleString('zh-CN', {
              month: '2-digit',
              day: '2-digit',
              hour: '2-digit',
              minute: '2-digit',
              hour12: false,
              timeZone: 'Asia/Shanghai'
          }).replace(',', '');
      },

      getDnsHistoryEntryKey(record, entry, index = 0) {
          return String(entry?.id || (
            String(record?.id || 'dns')
            + ':' + String(entry?.type || '')
            + ':' + String(entry?.content || '')
            + ':' + String(entry?.savedAt || '')
            + ':' + String(index)
          ));
      },

      getDnsHistoryEntryTitle(entry) {
          if (!entry) return '';
          const titleLines = [
              String(entry.type || '').toUpperCase() + ' → ' + String(entry.content || ''),
              '保存时间：' + this.formatLocalDateTime(entry.savedAt),
              entry.requestHost ? ('站点：' + entry.requestHost) : '',
              entry.source ? ('来源：' + entry.source) : ''
          ].filter(Boolean);
          return titleLines.join('\\n');
      },

      isDnsHistoryEntryCurrent(record, entry) {
          if (!record || !entry) return false;
          return String(record._originalType || '').toUpperCase() === String(entry.type || '').toUpperCase()
            && String(record._originalContent || '').trim() === String(entry.content || '').trim();
      },

      applyDnsHistoryEntry(recordId, entry) {
          const records = Array.isArray(this.dnsRecords) ? this.dnsRecords : [];
          const record = records.find(item => String(item?.id || '') === String(recordId || ''));
          if (!record || !record.editable || !entry) return;
          record.type = String(entry.type || record.type || '').toUpperCase();
          record.content = String(entry.content || record.content || '');
          this.updateDnsSaveAllButtonState();
          this.showToast('已回填历史值，点击保存即可生效', 'info', 1800);
      },

      inferZoneNameFromRecordNames(names = []) {
          const normalized = (Array.isArray(names) ? names : [])
            .map(name => String(name || '').trim().toLowerCase())
            .filter(Boolean);
          if (!normalized.length) return '';

          const reversedPartsList = normalized
            .map(name => name.split('.').map(part => part.trim()).filter(Boolean).reverse())
            .filter(parts => parts.length > 0);
          if (!reversedPartsList.length) return '';

          let common = reversedPartsList[0].slice();
          for (let i = 1; i < reversedPartsList.length && common.length; i++) {
              const parts = reversedPartsList[i];
              let j = 0;
              while (j < common.length && j < parts.length && common[j] === parts[j]) j++;
              common = common.slice(0, j);
          }

          const zoneParts = common.slice().reverse();
          if (zoneParts.length < 2) return '';
          return zoneParts.join('.');
      },

      updateDnsSaveAllButtonState() {
          const records = Array.isArray(this.dnsRecords) ? this.dnsRecords : [];
          const dirtyCount = records.filter(r => r && r.editable && this.isDnsRecordDirty(r) && !r._saving).length;
          const anySaving = records.some(r => r && r._saving) || this.dnsBatchSaving;
          return {
            dirtyCount,
            anySaving,
            disabled: anySaving || dirtyCount === 0,
            title: anySaving ? '正在保存中...' : (dirtyCount ? ('将保存 ' + dirtyCount + ' 条变更') : '没有可保存的变更')
          };
      },
      isDnsSaveAllDisabled() {
          return this.updateDnsSaveAllButtonState().disabled;
      },
      getDnsSaveAllTitle() {
          return this.updateDnsSaveAllButtonState().title;
      },
      getDnsSaveAllButtonText() {
          const state = this.updateDnsSaveAllButtonState();
          return state.anySaving ? '保存中...' : '保存全部';
      },

      isValidIpv4(value) {
          const v = String(value || '').trim();
          const parts = v.split('.');
          if (parts.length !== 4) return false;
          for (const part of parts) {
              if (!/^[0-9]{1,3}$/.test(part)) return false;
              const num = Number(part);
              if (!Number.isFinite(num) || num < 0 || num > 255) return false;
          }
          return true;
      },

      isValidIpv6(value) {
          const v = String(value || '').trim();
          if (!v) return false;
          if (!v.includes(':')) return false;
          if (/[\\s]/.test(v)) return false;
          try {
              // 利用 URL 解析做一个轻量校验（浏览器原生）
              new URL('http://[' + v + ']/');
              return true;
          } catch {
              return false;
          }
      },

      validateDnsRecordForSave(record) {
          const type = String(record?.type || '').toUpperCase();
          const content = String(record?.content || '').trim();
          if (!this.isDnsTypeAllowed(type)) return 'Type 仅允许 A / AAAA / CNAME';
          if (!content) return 'Content 不能为空';
          if (type === 'A' && !this.isValidIpv4(content)) return 'A 记录 Content 必须是合法 IPv4 地址';
          if (type === 'AAAA' && !this.isValidIpv6(content)) return 'AAAA 记录 Content 必须是合法 IPv6 地址';
          if (type === 'CNAME') {
              if (/[\\s]/.test(content)) return 'CNAME 记录 Content 不能包含空格';
              if (content.length > 255) return 'CNAME 记录 Content 过长';
          }
          return '';
      },

      async loadDnsRecords() {
          const loadSeq = ++this.dnsLoadSeq;
          this.dnsZoneHintText = '当前站点：加载中...';
          this.dnsEmptyText = '正在加载当前站点 DNS 记录...';
          this.dnsCurrentHost = '';
          this.dnsTotalRecordCount = 0;

          try {
              const res = await this.apiCall('listDnsRecords');
              if (loadSeq !== this.dnsLoadSeq) return;

              const zoneName = res.zoneName || res.zone?.name || '';
              const zoneId = res.zoneId || res.zone?.id || '';
              this.dnsCurrentHost = String(res.currentHost || '').trim().toLowerCase();
              this.dnsTotalRecordCount = Math.max(0, Number(res.totalRecords) || 0);

              const rawRecords = Array.isArray(res.records) ? res.records : [];
              const inferredZoneName = zoneName ? String(zoneName || '') : this.inferZoneNameFromRecordNames(rawRecords.map(item => item?.name));
              const displayZoneName = String(inferredZoneName || zoneName || '').trim();
              const zoneText = displayZoneName ? displayZoneName : '未知域名';
              const visibleCount = rawRecords.length;
              const totalCount = this.dnsTotalRecordCount || visibleCount;
              this.dnsZoneHintText = '当前站点：' + (this.dnsCurrentHost || '未识别') + ' · Zone：' + zoneText + ' · 显示 ' + visibleCount + ' / ' + totalCount + ' 条';
              this.dnsEmptyText = this.dnsCurrentHost
                ? ('当前站点 ' + this.dnsCurrentHost + ' 暂无 DNS 记录')
                : '暂无 DNS 记录';
              const records = rawRecords.map((item) => {
                  const type = String(item?.type || '').toUpperCase();
                  const name = String(item?.name || '');
                  const content = String(item?.content || '');
                  return {
                      id: String(item?.id || ''),
                      type,
                      name,
                      content,
                      ttl: Number(item?.ttl) || 1,
                      proxied: item?.proxied === true,
                      editable: this.isDnsTypeAllowed(type),
                      history: this.normalizeDnsHistoryEntries(item?.history),
                      _originalType: type,
                      _originalContent: content,
                      _saving: false
                  };
              }).filter(r => r.id && r.name);

              records.sort((a, b) => (a.name.localeCompare(b.name) || a.type.localeCompare(b.type) || a.id.localeCompare(b.id)));
              this.dnsRecords = records;
              this.dnsZone = zoneId || displayZoneName ? { id: zoneId, name: displayZoneName, currentHost: this.dnsCurrentHost } : null;
          } catch (e) {
              if (loadSeq !== this.dnsLoadSeq) return;
              console.error('loadDnsRecords failed', e);
              this.dnsZoneHintText = '当前站点：加载失败（请检查 CF Zone ID、API 令牌权限）';
              this.dnsRecords = [];
              this.dnsZone = null;
              this.dnsCurrentHost = '';
              this.dnsTotalRecordCount = 0;
              this.dnsEmptyText = 'DNS 记录加载失败';
              const message = e && e.message ? e.message : '未知错误';
              this.showMessage('DNS 记录加载失败: ' + message, { tone: 'error', modal: true });
          }
      },

      async saveDnsRecord(recordId, opts = {}) {
          const records = Array.isArray(this.dnsRecords) ? this.dnsRecords : [];
          const record = records.find(r => String(r?.id || '') === String(recordId || ''));
          if (!record) throw new Error('记录不存在');

          if (!record.editable) throw new Error('该记录类型不支持编辑');
          const dirty = this.isDnsRecordDirty(record);
          if (!dirty) return;

          const validationError = this.validateDnsRecordForSave(record);
          if (validationError) throw new Error(validationError);

          record._saving = true;
          this.updateDnsSaveAllButtonState();

          try {
              const res = await this.apiCall('updateDnsRecord', { recordId: record.id, type: record.type, content: record.content });
              const savedRecord = res?.record || {};
              record.name = String(savedRecord.name || record.name || '');
              record.type = String(savedRecord.type || record.type || '').toUpperCase();
              record.content = String(savedRecord.content || record.content || '').trim();
              record.history = this.normalizeDnsHistoryEntries(res?.history || record.history);
              record._originalType = String(record.type || '').toUpperCase();
              record._originalContent = String(record.content || '');
              if (!opts.silent) this.showMessage('保存成功', { tone: 'success' });
          } finally {
              record._saving = false;
              this.updateDnsSaveAllButtonState();
          }
      },

      async saveAllDnsRecords() {
          const records = Array.isArray(this.dnsRecords) ? this.dnsRecords : [];
          const dirtyRecords = records.filter(r => r && r.editable && this.isDnsRecordDirty(r) && !r._saving);
          if (!dirtyRecords.length) {
              this.showMessage('没有需要保存的变更', { tone: 'info' });
              return;
          }

          if (!await this.askConfirm('确定保存 ' + dirtyRecords.length + ' 条 DNS 记录变更？', { title: '保存 DNS 变更', tone: 'warning', confirmText: '保存' })) return;

          this.dnsBatchSaving = true;
          const errors = [];
          let okCount = 0;
          try {
              for (let i = 0; i < dirtyRecords.length; i++) {
                  const record = dirtyRecords[i];
                  try {
                      await this.saveDnsRecord(record.id, { silent: true });
                      okCount += 1;
                  } catch (e) {
                      errors.push((record.name || record.id) + ': ' + (e && e.message ? e.message : '未知错误'));
                  }
              }
          } finally {
              this.dnsBatchSaving = false;
              this.updateDnsSaveAllButtonState();
          }

          if (errors.length) {
              const head = '已保存 ' + okCount + '/' + dirtyRecords.length + '，失败 ' + errors.length + ' 条。';
              const detail = errors.slice(0, 6).join('\\n') + (errors.length > 6 ? '\\n...' : '');
              this.showMessage(head + '\\n' + detail, { tone: 'warning', modal: true });
          } else {
              this.showMessage('已保存 ' + okCount + ' 条 DNS 记录变更', { tone: 'success' });
          }
      },

      async loadLogs(page = this.logPage) {
          const keyword = this.logSearchKeyword || '';
          const { startDate, endDate } = this.ensureLogDateRange();
          try {
              const res = await this.apiCall('getLogs', {
                page: page,
                pageSize: 50,
                filters: {
                  keyword,
                  playbackMode: this.logsPlaybackModeFilter || '',
                  startDate,
                  endDate
                }
              });
              if (res.logs) {
                  this.logPage = res.page;
                  this.logTotalPages = res.totalPages || 1;
                  this.logRows = res.logs;
                  return;
              }
          } catch (err) {
              this.logRows = [];
              this.logPage = 1;
              this.logTotalPages = 1;
              this.showMessage('日志加载失败: ' + (err?.message || '未知错误'), { tone: 'error', modal: true });
              return;
          }
          this.logRows = [];
      },

      changeLogPage(delta) {
          const newPage = this.logPage + delta;
          if(newPage >= 1 && newPage <= this.logTotalPages) {
              this.loadLogs(newPage);
          }
      },

      downloadJson(data, filename) {
          const blob = new Blob([JSON.stringify(data, null, 2)], {type: 'application/json'});
          const url = uiBrowserBridge.createObjectUrl(blob);
          return this.triggerDownload(url, filename);
      },

      async readJsonFileFromInputEvent(event) {
          const input = event?.target && typeof event.target === 'object' ? event.target : null;
          const file = input?.files?.[0];
          if (!file) return null;
          try {
              const text = await file.text();
              return JSON.parse(text);
          } finally {
              if (input) input.value = '';
          }
      },

      async exportNodes() {
          this.downloadJson(this.nodes, \`emby_nodes_\${new Date().getTime()}.json\`);
      },

      async importNodes(event) {
          try {
              const data = await this.readJsonFileFromInputEvent(event);
              if (!data) return;
              const nodes = Array.isArray(data) ? data : (data.nodes || []);
              if(!nodes.length) {
                  this.showMessage('未找到有效的节点数据', { tone: 'warning' });
                  return;
              }
              await this.apiCall('import', {nodes});
              await this.loadNodes();
              this.showMessage('节点导入成功', { tone: 'success' });
          } catch(err) {
              console.error('importNodes failed', err);
              if (err instanceof SyntaxError) this.showMessage('文件解析失败', { tone: 'error' });
              else this.showMessage('节点导入失败: ' + (err?.message || '未知错误'), { tone: 'error', modal: true });
          }
      },

      async exportFull() {
          const res = await this.apiCall('exportConfig');
          if(res) this.downloadJson(res, \`emby_proxy_full_backup_\${new Date().getTime()}.json\`);
      },

      async exportSettings() {
          const res = await this.apiCall('exportSettings');
          if (res) this.downloadJson(res, \`emby_proxy_settings_\${new Date().getTime()}.json\`);
      },

      async importSettings(event) {
          try {
              const data = await this.readJsonFileFromInputEvent(event);
              if (!data) return;
              const importedConfig = data && typeof data === 'object' && !Array.isArray(data)
                ? ((data.config && typeof data.config === 'object' && !Array.isArray(data.config)) ? data.config : (data.settings && typeof data.settings === 'object' && !Array.isArray(data.settings) ? data.settings : data))
                : null;
              if(!importedConfig || Array.isArray(importedConfig)) {
                this.showMessage('无效的设置备份文件', { tone: 'warning' });
                return;
              }
              const currentRes = await this.apiCall('loadConfig');
              const currentConfig = currentRes.config || {};
              const mergedConfig = { ...currentConfig, ...importedConfig };
              const { sanitizedConfig, preview } = await this.prepareConfigChangePreview('all', currentConfig, mergedConfig);
              if (!preview.hasChanges) {
                this.showMessage('导入文件与当前全局设置一致，无需导入。', { tone: 'info' });
                return;
              }
              const importMessage = preview.message.replace('即将保存「全部分区」以下变更：', '即将导入以下全局设置变更：');
              if (!await this.askConfirm(importMessage, { title: '导入全局设置', tone: 'warning', confirmText: '导入' })) return;
              const res = await this.apiCall('importSettings', { config: sanitizedConfig, meta: { source: 'settings_file' } });
              await this.finalizePersistedSettings(res.config || sanitizedConfig, {
                successMessage: '全局设置导入成功，已立即生效。',
                partialSuccessPrefix: '全局设置已导入，但设置面板刷新失败: ',
                refreshErrorLog: 'loadSettings after importSettings failed'
              });
          } catch(err) {
              console.error('importSettings failed', err);
              if (err instanceof SyntaxError) this.showMessage('文件解析失败', { tone: 'error' });
              else this.showMessage('全局设置导入失败: ' + (err?.message || '未知错误'), { tone: 'error', modal: true });
          }
      },

      async importFull(event) {
          try {
              const data = await this.readJsonFileFromInputEvent(event);
              if (!data) return;
              if(!data.config && !data.nodes) {
                this.showMessage('无效的备份文件', { tone: 'warning' });
                return;
              }
              const res = await this.apiCall('importFull', {config: data.config, nodes: data.nodes});
              this.applyRuntimeConfig(res.config || {});
              await Promise.all([
                this.loadNodes(),
                this.loadSettings()
              ]);
              this.showMessage('完整数据导入成功，已立即生效。', { tone: 'success' });
          } catch(err) {
              console.error('importFull failed', err);
              if (err instanceof SyntaxError) this.showMessage('文件解析失败', { tone: 'error' });
              else this.showMessage('完整数据导入失败: ' + (err?.message || '未知错误'), { tone: 'error', modal: true });
          }
      }
    };

    const ADMIN_UI_BOOTSTRAP = globalThis.__ADMIN_BOOTSTRAP__ && typeof globalThis.__ADMIN_BOOTSTRAP__ === 'object'
      ? globalThis.__ADMIN_BOOTSTRAP__
      : {};
    const ADMIN_PATH = String(ADMIN_UI_BOOTSTRAP.adminPath || (typeof window !== 'undefined' && window.location?.pathname) || '/admin');
    const ADMIN_LOGIN_PATH = String(ADMIN_UI_BOOTSTRAP.loginPath || (ADMIN_PATH === '/' ? '/login' : (ADMIN_PATH + '/login')));
    function renderUiBootstrapError(message) {
      if (typeof document === 'undefined') return;
      const target = document.getElementById('app') || document.body;
      if (!target) return;
      target.innerHTML = '<div class="min-h-screen flex items-center justify-center px-6 py-10"><div class="max-w-lg w-full rounded-[28px] border border-red-200 bg-white p-6 shadow-xl"><h1 class="text-xl font-bold text-slate-900">管理台初始化失败</h1><p class="mt-3 text-sm leading-6 text-slate-600">' + String(message || '未知错误') + '</p></div></div>';
    }
    if (typeof Vue === 'undefined') {
      renderUiBootstrapError('Vue 资源未加载完成，请检查当前网络到 CDN 的连通性。');
      throw new Error('Vue dependency missing');
    }

    const { createApp, defineComponent, reactive, onMounted, onBeforeUnmount, nextTick } = Vue;
    const AUTO_ANIMATE_CDN_URL = 'https://cdn.jsdelivr.net/npm/@formkit/auto-animate@0.9.0/index.mjs';
    function formatDateInputValue(date = new Date()) {
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      return year + '-' + month + '-' + day;
    }
    function getDefaultLogDateRange() {
      const end = new Date();
      const start = new Date(end.getTime() - 24 * 60 * 60 * 1000);
      return {
        startDate: formatDateInputValue(start),
        endDate: formatDateInputValue(end)
      };
    }
    const DEFAULT_LOG_DATE_RANGE = getDefaultLogDateRange();
    let autoAnimateLoader = null;
    const autoAnimateControllers = new WeakMap();
    const lucideDirectiveTokens = new WeakMap();
    const trafficChartInstances = new WeakMap();
    const trafficChartSignatures = new WeakMap();
    const nodeLinesDragDirectiveStates = new WeakMap();

    function normalizeAutoAnimateOptions(value) {
      if (value === false) return false;
      if (value && typeof value === 'object') return value;
      return {};
    }

    async function ensureAutoAnimateFunction() {
      if (autoAnimateLoader) return autoAnimateLoader;
      autoAnimateLoader = import(AUTO_ANIMATE_CDN_URL)
        .then(module => module?.default || module?.autoAnimate || null)
        .catch(error => {
          console.error('autoAnimate import failed', error);
          return null;
        });
      return autoAnimateLoader;
    }

    async function bindAutoAnimate(element, value) {
      const options = normalizeAutoAnimateOptions(value);
      if (options === false || !element || autoAnimateControllers.has(element)) return;
      const autoAnimate = await ensureAutoAnimateFunction();
      if (typeof autoAnimate !== 'function') return;
      try {
        const controller = autoAnimate(element, options);
        autoAnimateControllers.set(element, controller || true);
      } catch (error) {
        console.error('autoAnimate init failed', error);
      }
    }

    function scheduleLucideIconsRender(element) {
      if (!element) return;
      const token = (lucideDirectiveTokens.get(element) || 0) + 1;
      lucideDirectiveTokens.set(element, token);
      uiBrowserBridge.queueTask(() => {
        if (lucideDirectiveTokens.get(element) !== token) return;
        uiBrowserBridge.renderLucideIcons({ root: element });
      });
    }

    function normalizeTrafficChartSeries(series) {
      if (Array.isArray(series) && series.length) {
        return series.map(item => ({
          label: String(item?.label || ''),
          total: Number.isFinite(Number(item?.total)) ? Number(item.total) : 0
        }));
      }
      return Array.from({ length: 24 }, (_, hour) => ({
        label: String(hour).padStart(2, '0') + ':00',
        total: 0
      }));
    }

    function getTrafficChartTheme(element) {
      return element?.closest('.dark') ? 'dark' : 'light';
    }

    function buildTrafficChartSignature(series, theme) {
      return JSON.stringify({
        theme,
        series: normalizeTrafficChartSeries(series)
      });
    }

    function buildTrafficChartConfig(series, theme) {
      const normalizedSeries = normalizeTrafficChartSeries(series);
      const isDarkTheme = theme === 'dark';
      const labelColor = isDarkTheme ? '#e2e8f0' : '#0f172a';
      const axisColor = isDarkTheme ? '#94a3b8' : '#64748b';
      const gridColor = isDarkTheme ? 'rgba(71, 85, 105, 0.35)' : 'rgba(148, 163, 184, 0.22)';
      return {
        type: 'line',
        data: {
          labels: normalizedSeries.map(item => item.label),
          datasets: [{
            label: '请求趋势',
            data: normalizedSeries.map(item => item.total),
            borderColor: '#3b82f6',
            backgroundColor: 'rgba(59, 130, 246, 0.12)',
            fill: true,
            tension: 0.35,
            pointRadius: 0,
            pointHoverRadius: 3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: {
            duration: 180
          },
          plugins: {
            legend: { display: false },
            tooltip: { displayColors: false }
          },
          scales: {
            y: {
              min: 0,
              suggestedMax: 10,
              ticks: {
                precision: 0,
                color: axisColor
              },
              title: {
                display: true,
                text: '请求总次数',
                color: labelColor
              },
              grid: {
                color: gridColor
              },
              border: {
                color: gridColor
              }
            },
            x: {
              ticks: {
                color: axisColor
              },
              title: {
                display: true,
                text: '小时（UTC+8）',
                color: labelColor
              },
              grid: {
                color: gridColor
              },
              border: {
                color: gridColor
              }
            }
          }
        }
      };
    }

    function syncTrafficChart(element, series) {
      if (!element) return;
      const ChartCtor = uiBrowserBridge.resolveChartConstructor();
      if (!ChartCtor) return;
      const theme = getTrafficChartTheme(element);
      const signature = buildTrafficChartSignature(series, theme);
      if (trafficChartSignatures.get(element) === signature) return;
      const context = element.getContext?.('2d');
      if (!context) return;
      const currentChart = trafficChartInstances.get(element);
      if (currentChart && typeof currentChart.destroy === 'function') {
        currentChart.destroy();
      }
      try {
        const chartInstance = new ChartCtor(context, buildTrafficChartConfig(series, theme));
        trafficChartInstances.set(element, chartInstance);
        trafficChartSignatures.set(element, signature);
      } catch (error) {
        console.error('traffic chart render failed', error);
      }
    }

    const uiBrowserBridge = {
      renderLucideIcons(opts = {}) {
        if (typeof window?.lucide === 'undefined') return;
        try {
          window.lucide.createIcons(opts);
        } catch (error) {
          console.error('lucide.createIcons failed', error);
        }
      },
      queueTask(callback) {
        if (typeof callback !== 'function') return;
        if (typeof queueMicrotask === 'function') {
          queueMicrotask(callback);
          return;
        }
        Promise.resolve().then(callback);
      },
      startTimer(callback, delay = 0) {
        return setTimeout(callback, Math.max(0, Number(delay) || 0));
      },
      clearTimer(timerId) {
        if (!timerId) return;
        clearTimeout(timerId);
      },
      startIntervalTimer(callback, delay = 0) {
        return setInterval(callback, Math.max(0, Number(delay) || 0));
      },
      clearIntervalTimer(timerId) {
        if (!timerId) return;
        clearInterval(timerId);
      },
      syncDialogVisibility(element, shouldOpen) {
        if (!element) return;
        if (shouldOpen) {
          if (element.open) return;
          if (typeof element.showModal === 'function') {
            try {
              element.showModal();
              return;
            } catch {}
          }
          element.open = true;
          return;
        }
        if (!element.open) return;
        if (typeof element.close === 'function') {
          try {
            element.close();
            return;
          } catch {}
        }
        element.open = false;
      },
      resetScrollPosition(element) {
        if (!element) return;
        this.queueTask(() => {
          element.scrollTop = 0;
        });
      },
      triggerDownload(element) {
        if (!element) return;
        this.queueTask(() => {
          element.click?.();
        });
      },
      async writeClipboard(text) {
        if (typeof navigator?.clipboard?.writeText === 'function') {
          await navigator.clipboard.writeText(String(text || ''));
          return;
        }
        throw new Error('CLIPBOARD_UNAVAILABLE');
      },
      createObjectUrl(blob) {
        return URL.createObjectURL(blob);
      },
      revokeObjectUrl(url) {
        try {
          URL.revokeObjectURL(String(url || ''));
        } catch {}
      },
      resolveChartConstructor() {
        return typeof window?.Chart === 'function'
          ? window.Chart
          : (typeof Chart === 'function' ? Chart : null);
      },
      readStoredTheme() {
        try {
          return localStorage.getItem('theme');
        } catch {
          return '';
        }
      },
      resolveDarkTheme() {
        const savedTheme = this.readStoredTheme();
        if (savedTheme === 'light') return false;
        if (savedTheme === 'dark') return true;
        try {
          return !!window.matchMedia?.('(prefers-color-scheme: dark)').matches;
        } catch {
          return false;
        }
      },
      persistTheme(isDarkTheme) {
        try {
          localStorage.setItem('theme', isDarkTheme ? 'dark' : 'light');
        } catch {}
      },
      readLocationOrigin() {
        return String(window?.location?.origin || '');
      },
      createMediaQueryList(query) {
        try {
          return window?.matchMedia?.(String(query || '')) || null;
        } catch {
          return null;
        }
      },
      bindMediaQueryChange(mediaQueryList, handler) {
        if (!mediaQueryList || typeof handler !== 'function') return () => {};
        if (typeof mediaQueryList.addEventListener === 'function') {
          mediaQueryList.addEventListener('change', handler);
          return () => mediaQueryList.removeEventListener('change', handler);
        }
        if (typeof mediaQueryList.addListener === 'function') {
          mediaQueryList.addListener(handler);
          return () => mediaQueryList.removeListener(handler);
        }
        return () => {};
      },
      readDesktopViewportMatch() {
        const mediaQueryList = this.createMediaQueryList('(min-width: 768px)');
        if (mediaQueryList) return mediaQueryList.matches === true;
        return Number(window?.innerWidth || 0) >= 768;
      },
      readHash(fallback = '#dashboard') {
        return String(window?.location?.hash || fallback || '#dashboard');
      },
      writeHash(hash) {
        if (!window?.location) return;
        window.location.hash = String(hash || '').trim() || '#dashboard';
      },
      bindHashChange(handler) {
        if (typeof window?.addEventListener !== 'function' || typeof handler !== 'function') {
          return () => {};
        }
        window.addEventListener('hashchange', handler);
        return () => window.removeEventListener('hashchange', handler);
      },
      attachDebugApp(appState) {
        if (!window) return;
        window.App = appState;
      },
      detachDebugApp(appState) {
        if (!window || window.App !== appState) return;
        try {
          delete window.App;
        } catch {
          window.App = undefined;
        }
      },
      bindElementEvents(element, listeners = {}) {
        if (!element || !listeners || typeof listeners !== 'object') return () => {};
        const entries = Object.entries(listeners).filter(([, handler]) => typeof handler === 'function');
        for (const [eventName, handler] of entries) {
          element.addEventListener(eventName, handler);
        }
        return () => {
          for (const [eventName, handler] of entries) {
            element.removeEventListener(eventName, handler);
          }
        };
      }
    };

    const nodeLineDragAdapter = {
      normalizeValue(value) {
        return value && typeof value === 'object' ? value : {};
      },
      isInteractiveTarget(target) {
        return !!target?.closest?.('[data-node-line-interactive="1"]');
      },
      resolveRowElement(target) {
        return target?.closest?.('[data-node-line-row="1"]') || null;
      },
      resolveLineId(element) {
        return String(element?.dataset?.lineId || '').trim();
      },
      resolveDropPlacement(element, clientY) {
        if (!element || !Number.isFinite(clientY) || typeof element.getBoundingClientRect !== 'function') return 'before';
        const rect = element.getBoundingClientRect();
        return clientY >= rect.top + (rect.height / 2) ? 'after' : 'before';
      },
      bind(element, value) {
        const state = {
          value: this.normalizeValue(value),
          dragBlocked: false,
          cleanup: null
        };

        const handlers = {
          mousedown: (event) => {
            state.dragBlocked = this.isInteractiveTarget(event?.target);
          },
          dragstart: (event) => {
            const app = state.value?.app;
            const row = this.resolveRowElement(event?.target);
            const lineId = this.resolveLineId(row);
            if (!app || !row || !lineId || app.isDesktopNodeLineDragEnabled?.() !== true) return;
            if (state.dragBlocked) {
              state.dragBlocked = false;
              event?.preventDefault?.();
              return;
            }
            state.dragBlocked = false;
            app.nodeLineDragId = lineId;
            app.nodeLineDropHint = null;
            if (event?.dataTransfer) {
              event.dataTransfer.effectAllowed = 'move';
              try { event.dataTransfer.setData('text/plain', lineId); } catch {}
            }
          },
          dragover: (event) => {
            const app = state.value?.app;
            const row = this.resolveRowElement(event?.target);
            const lineId = this.resolveLineId(row);
            if (!app || !row || !lineId || !app.nodeLineDragId || app.nodeLineDragId === lineId) return;
            event?.preventDefault?.();
            if (event?.dataTransfer) event.dataTransfer.dropEffect = 'move';
            const placement = this.resolveDropPlacement(row, event?.clientY);
            const prevHint = app.nodeLineDropHint;
            if (!prevHint || prevHint.lineId !== lineId || prevHint.placement !== placement) {
              app.nodeLineDropHint = { lineId, placement };
            }
          },
          drop: (event) => {
            const app = state.value?.app;
            const row = this.resolveRowElement(event?.target);
            const lineId = this.resolveLineId(row);
            if (!app || !row || !lineId) return;
            event?.preventDefault?.();
            if (!app.nodeLineDragId || app.nodeLineDragId === lineId) {
              app.clearNodeLineDragState?.();
              return;
            }
            const placement = app.nodeLineDropHint?.lineId === lineId
              ? app.nodeLineDropHint.placement
              : this.resolveDropPlacement(row, event?.clientY);
            app.moveNodeLineTo?.(app.nodeLineDragId, lineId, placement);
            app.clearNodeLineDragState?.();
          },
          dragend: () => {
            const app = state.value?.app;
            state.dragBlocked = false;
            if (!app?.nodeLineDragId && !app?.nodeLineDropHint) return;
            app.clearNodeLineDragState?.();
          }
        };

        state.cleanup = uiBrowserBridge.bindElementEvents(element, handlers);
        nodeLinesDragDirectiveStates.set(element, state);
      },
      update(element, value) {
        const state = nodeLinesDragDirectiveStates.get(element);
        if (!state) {
          this.bind(element, value);
          return;
        }
        state.value = this.normalizeValue(value);
      },
      unbind(element) {
        const state = nodeLinesDragDirectiveStates.get(element);
        nodeLinesDragDirectiveStates.delete(element);
        state?.value?.app?.clearNodeLineDragState?.();
        if (typeof state?.cleanup === 'function') state.cleanup();
      }
    };

    function focusAndSelectInputElement(element) {
      if (!element) return;
      nextTick(() => {
        try {
          element.focus?.({ preventScroll: true });
        } catch {
          element.focus?.();
        }
        element.select?.();
      });
    }

    const dialogVisibleDirective = {
      mounted(element, binding) {
        uiBrowserBridge.syncDialogVisibility(element, binding.value === true);
      },
      updated(element, binding) {
        if (binding.value === binding.oldValue && !!element.open === (binding.value === true)) return;
        uiBrowserBridge.syncDialogVisibility(element, binding.value === true);
      }
    };

    const scrollResetDirective = {
      mounted(element) {
        uiBrowserBridge.resetScrollPosition(element);
      },
      updated(element, binding) {
        if (binding.value === binding.oldValue) return;
        uiBrowserBridge.resetScrollPosition(element);
      }
    };

    const autoFocusSelectDirective = {
      mounted(element, binding) {
        if (binding.value === true) focusAndSelectInputElement(element);
      },
      updated(element, binding) {
        if (binding.value !== true || binding.oldValue === true) return;
        focusAndSelectInputElement(element);
      }
    };

    const autoDownloadDirective = {
      updated(element, binding) {
        const nextValue = binding.value && typeof binding.value === 'object' ? binding.value : {};
        const prevValue = binding.oldValue && typeof binding.oldValue === 'object' ? binding.oldValue : {};
        if (!nextValue.href) return;
        if (String(nextValue.key || '') === String(prevValue.key || '')) return;
        uiBrowserBridge.triggerDownload(element);
      }
    };

    const autoAnimateDirective = {
      mounted(element, binding) {
        bindAutoAnimate(element, binding.value);
      },
      updated(element, binding) {
        if (binding.value === false) return;
        if (!autoAnimateControllers.has(element)) bindAutoAnimate(element, binding.value);
      },
      unmounted(element) {
        const controller = autoAnimateControllers.get(element);
        autoAnimateControllers.delete(element);
        if (controller && typeof controller.disable === 'function') controller.disable();
      }
    };

    const lucideIconsDirective = {
      mounted(element) {
        scheduleLucideIconsRender(element);
      },
      updated(element) {
        scheduleLucideIconsRender(element);
      }
    };

    const trafficChartDirective = {
      mounted(element, binding) {
        syncTrafficChart(element, binding.value);
      },
      updated(element, binding) {
        syncTrafficChart(element, binding.value);
      },
      unmounted(element) {
        const currentChart = trafficChartInstances.get(element);
        trafficChartInstances.delete(element);
        trafficChartSignatures.delete(element);
        if (currentChart && typeof currentChart.destroy === 'function') {
          currentChart.destroy();
        }
      }
    };

    const nodeLinesDragDirective = {
      mounted(element, binding) {
        nodeLineDragAdapter.bind(element, binding.value);
      },
      updated(element, binding) {
        nodeLineDragAdapter.update(element, binding.value);
      },
      unmounted(element) {
        nodeLineDragAdapter.unbind(element);
      }
    };

    const CopyButton = defineComponent({
      name: 'CopyButton',
      props: {
        text: { type: String, default: '' },
        label: { type: String, default: '复制' }
      },
      data() {
        return { copied: false };
      },
      methods: {
        async copyText() {
          try {
            await uiBrowserBridge.writeClipboard(this.text);
            this.copied = true;
            uiBrowserBridge.startTimer(() => { this.copied = false; }, 1200);
          } catch (error) {
            console.error('copyText failed', error);
          }
        }
      },
      template: '#tpl-copy-button'
    });

    const NodeCard = defineComponent({
      name: 'NodeCard',
      components: { 'copy-button': CopyButton },
      props: {
        node: {
          type: Object,
          default: () => ({})
        },
        app: {
          type: Object,
          required: true
        }
      },
      data() {
        return {
          revealLink: false
        };
      },
      computed: {
        hydratedNode() {
          const nextNode = this.app.hydrateNode(this.node);
          return nextNode && typeof nextNode === 'object' ? nextNode : {};
        },
        displayName() {
          return String(this.hydratedNode.displayName || this.hydratedNode.name || '');
        },
        link() {
          return this.app.buildNodeLink(this.hydratedNode);
        },
        activeLine() {
          return this.app.getActiveNodeLine(this.hydratedNode);
        },
        activeLineName() {
          return this.activeLine?.name || '未启用线路';
        },
        lineCount() {
          return this.app.getNodeLines(this.hydratedNode).length;
        },
        remarkValue() {
          return String(this.hydratedNode.remark || '').trim();
        },
        hasTag() {
          return String(this.hydratedNode.tag || '').trim().length > 0;
        },
        tagToneClass() {
          const tagPillPalette = {
            amber: 'border-amber-200 bg-amber-100 text-amber-800 dark:border-amber-700 dark:bg-amber-900 dark:text-amber-100',
            emerald: 'border-emerald-200 bg-emerald-100 text-emerald-800 dark:border-emerald-700 dark:bg-emerald-900 dark:text-emerald-100',
            sky: 'border-sky-200 bg-sky-100 text-sky-800 dark:border-sky-700 dark:bg-sky-900 dark:text-sky-100',
            violet: 'border-violet-200 bg-violet-100 text-violet-800 dark:border-violet-700 dark:bg-violet-900 dark:text-violet-100',
            rose: 'border-rose-200 bg-rose-100 text-rose-800 dark:border-rose-700 dark:bg-rose-900 dark:text-rose-100',
            slate: 'border-slate-200 bg-slate-100 text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200'
          };
          const tagColorKey = this.app.normalizeNodeKey(this.hydratedNode.tagColor || '');
          const toneKey = this.hasTag ? (tagPillPalette[tagColorKey] ? tagColorKey : 'amber') : 'slate';
          return tagPillPalette[toneKey] || tagPillPalette.amber;
        },
        statusMeta() {
          return this.app.getNodeLatencyMeta(this.activeLine?.latencyMs, this.app.nodeHealth[this.hydratedNode.name] || 0);
        },
        pingPending() {
          return this.app.isNodePingPending(this.hydratedNode.name);
        },
        latencyTitle() {
          return this.activeLine?.latencyUpdatedAt ? ('最近测速：' + this.app.formatLocalDateTime(this.activeLine.latencyUpdatedAt)) : '尚未测速';
        }
      },
      methods: {
        toggleLinkVisibility() {
          this.revealLink = !this.revealLink;
        },
        async pingNode() {
          const nodeName = String(this.hydratedNode.name || '').trim();
          if (!nodeName) return;
          await this.app.checkSingleNodeHealth(nodeName);
        },
        editNode() {
          const nodeName = String(this.hydratedNode.name || '').trim();
          if (!nodeName) return;
          this.app.showNodeModal(nodeName);
        },
        async deleteNode() {
          const nodeName = String(this.hydratedNode.name || '').trim();
          if (!nodeName) return;
          await this.app.deleteNode(nodeName);
        }
      },
      template: '#tpl-node-card'
    });

    const RootApp = defineComponent({
      name: 'RootApp',
      components: { 'node-card': NodeCard },
      template: '#tpl-app',
      setup() {
        const appState = reactive(UiBridge);
        let timeConeTimer = null;
        let unbindDesktopViewportChange = null;
        let unbindHashRouteChange = null;
        uiBrowserBridge.attachDebugApp(appState);
        const handleHashChange = () => appState.route(uiBrowserBridge.readHash(appState.currentHash || '#dashboard'));
        const handleDesktopViewportChange = (event) => {
          appState.syncViewportState(appState.getCurrentRouteHash(), event?.matches === true);
        };

        onMounted(async () => {
          try {
            const initialConfigRes = await appState.apiCall('loadConfig');
            appState.applyRuntimeConfig(initialConfigRes.config || {});
          } catch (e) {
            const message = e?.message || '未知错误';
            if (message !== 'LOGIN_CANCELLED') appState.showMessage('身份验证失败或网络异常: ' + message, { tone: 'error', modal: true });
            return;
          }

          try {
            appState.init();
            timeConeTimer = uiBrowserBridge.startIntervalTimer(() => appState.updateTimeCones(), 60000);
            unbindHashRouteChange = uiBrowserBridge.bindHashChange(handleHashChange);
            const desktopViewportQuery = uiBrowserBridge.createMediaQueryList('(min-width: 768px)');
            if (desktopViewportQuery) {
              appState.syncViewportState(appState.getCurrentRouteHash(), desktopViewportQuery.matches);
              unbindDesktopViewportChange = uiBrowserBridge.bindMediaQueryChange(desktopViewportQuery, handleDesktopViewportChange);
            } else {
              appState.syncViewportState(appState.getCurrentRouteHash());
            }
          } catch (e) {
            console.error('UI 初始化错误:', e);
          }
        });

        onBeforeUnmount(() => {
          if (timeConeTimer) {
            uiBrowserBridge.clearIntervalTimer(timeConeTimer);
            timeConeTimer = null;
          }
          appState.clearToastTimer();
          appState.clearThemeTransitionTimer();
          if (typeof unbindHashRouteChange === 'function') {
            unbindHashRouteChange();
            unbindHashRouteChange = null;
          }
          if (typeof unbindDesktopViewportChange === 'function') {
            unbindDesktopViewportChange();
            unbindDesktopViewportChange = null;
          }
          appState.revokeDownloadUrl();
          uiBrowserBridge.detachDebugApp(appState);
        });

        return { App: appState };
      }
    });

    const app = createApp(RootApp);
    app.directive('dialog-visible', dialogVisibleDirective);
    app.directive('scroll-reset', scrollResetDirective);
    app.directive('auto-focus-select', autoFocusSelectDirective);
    app.directive('auto-download', autoDownloadDirective);
    app.directive('auto-animate', autoAnimateDirective);
    app.directive('lucide-icons', lucideIconsDirective);
    app.directive('traffic-chart', trafficChartDirective);
    app.directive('node-lines-drag', nodeLinesDragDirective);
    app.mount('#app');
    globalThis.__ADMIN_UI_BOOTED__ = true;
    if (globalThis.__ADMIN_UI_DEPENDENCY_TIMEOUT__) clearTimeout(globalThis.__ADMIN_UI_DEPENDENCY_TIMEOUT__);
  </script>
</body>
</html>`;

// ============================================================================
// 6. 运行时入口 (RUNTIME ENTRYPOINTS)
// 说明：
// - `fetch` 负责 UI / API / 代理主入口分发。
// - `scheduled` 负责日志清理与日报等定时任务。
// ============================================================================
function renderAdminPage(env, initHealth = buildInitHealth(env)) {
  const adminPath = getAdminPath(env);
  const bootstrap = {
    adminPath,
    loginPath: getAdminLoginPath(env),
    initHealth
  };
  const html = UI_HTML
    .replace("__ADMIN_BOOTSTRAP__", () => serializeInlineJson(bootstrap))
    .replace('<div id="app" v-cloak></div>', () => `${buildInitHealthBannerHtml(initHealth)}\n  <div id="app" v-cloak></div>`);
  const headers = new Headers({ 'Content-Type': 'text/html;charset=UTF-8', 'Cache-Control': 'public, max-age=0, s-maxage=86400, stale-while-revalidate=3600' });
  applySecurityHeaders(headers);
  return new Response(html, { headers });
}

function renderLandingPage(env, initHealth = buildInitHealth(env)) {
  const adminPath = getAdminPath(env);
  const initBanner = initHealth.ok
    ? ''
    : `<div class="mb-4 rounded-2xl border border-amber-300/40 bg-amber-500/10 px-4 py-3 text-left text-amber-100">
        <div class="text-sm font-semibold">系统未初始化</div>
        <div class="mt-1 text-xs leading-5 text-amber-50/90">缺少关键环境变量：${initHealth.missing.map(item => escapeHtml(item)).join('、')}</div>
      </div>`;
  const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Emby Proxy V18.5</title>
  <script src="https://cdn.tailwindcss.com/3.4.17"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen">
  <main class="min-h-screen flex items-center justify-center px-6 py-12">
    <section class="max-w-3xl w-full rounded-[32px] border border-slate-800 bg-slate-900/95 shadow-2xl overflow-hidden">
      <div class="grid gap-0 md:grid-cols-[1.1fr,0.9fr]">
        <div class="p-8 md:p-10 text-left">
          ${initBanner}
          <div class="inline-flex items-center rounded-full border border-brand-500/30 bg-brand-500/10 px-3 py-1 text-xs font-semibold tracking-[0.16em] uppercase text-brand-300">Headless Edge Relay</div>
          <h1 class="mt-5 text-3xl md:text-4xl font-bold text-white leading-tight">Emby Proxy V18.5</h1>
          <p class="mt-4 text-sm md:text-base leading-7 text-slate-300">为了极致优化视频代理的性能，当前根路径默认只保留一个无头（Headless）数据中继站；真正的管理界面、节点控制和 DNS 运维都收敛到单独的管理台入口。</p>
          <p class="mt-3 text-sm md:text-base leading-7 text-slate-400">如果你现在需要配置节点、查看运行状态或调整 Cloudflare 相关参数，请直接访问 <span class="font-semibold text-white">${escapeHtml(adminPath)}</span>。</p>
          <div class="mt-8 flex flex-col sm:flex-row gap-3">
            <a href="${escapeHtml(adminPath)}" class="inline-flex items-center justify-center rounded-2xl bg-brand-600 px-5 py-3 text-sm font-semibold text-white hover:bg-brand-700 transition">访问 ${escapeHtml(adminPath)}</a>
            <a href="https://github.com/axuitomo/CF-EMBY-PROXY-UI" target="_blank" rel="noopener noreferrer" class="inline-flex items-center justify-center rounded-2xl border border-slate-700 px-5 py-3 text-sm font-semibold text-slate-200 hover:bg-slate-800 transition">查看项目说明</a>
          </div>
        </div>
        <div class="border-t md:border-t-0 md:border-l border-slate-800 bg-slate-950/80 p-8 md:p-10 text-left">
          <div class="rounded-3xl border border-slate-800 bg-slate-900/70 p-6">
            <div class="text-xs font-semibold tracking-[0.16em] uppercase text-slate-500">Routing Notes</div>
            <ul class="mt-4 space-y-3 text-sm leading-6 text-slate-300">
              <li>• 根路径仅提供静态说明页，不承载实时配置数据。</li>
              <li>• \`${escapeHtml(adminPath)}\` 下发 SaaS 控制台骨架，动态数据继续走 \`POST ${escapeHtml(adminPath)}\` API。</li>
              <li>• 媒体代理、日志与 KV / D1 逻辑保持原 Worker 主链路不变。</li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  </main>
</body>
</html>`;
  const headers = new Headers({ 'Content-Type': 'text/html;charset=UTF-8', 'Cache-Control': 'public, max-age=3600, s-maxage=86400' });
  applySecurityHeaders(headers);
  headers.set('X-Frame-Options', 'DENY');
  return new Response(html, { headers });
}

export default {
  async fetch(request, env, ctx) {
    const initHealth = warnInitHealthOnce(env);
    const dynamicCors = getCorsHeadersForResponse(env, request);
    const requestUrl = new URL(request.url);
    const normalizedPathname = sanitizeProxyPath(requestUrl.pathname);
    const pathnameLower = normalizedPathname.toLowerCase();
    const adminPath = getAdminPath(env);
    const adminPathLower = adminPath.toLowerCase();
    const adminLoginPath = getAdminLoginPath(env);
    const adminLoginPathLower = adminLoginPath.toLowerCase();
    let segments;
    try { segments = normalizedPathname.split('/').filter(Boolean); }
    catch {
      const headers = new Headers(dynamicCors);
      applySecurityHeaders(headers);
      return new Response('Bad Request', { status: 400, headers });
    }

    const rootRaw = segments[0] || '';
    const root = safeDecodeSegment(rootRaw).toLowerCase();

    if (request.method === 'GET' && normalizedPathname === '/') return renderLandingPage(env, initHealth);

    if (request.method === 'GET' && pathnameLower === adminPathLower) return renderAdminPage(env, initHealth);

    if (request.method === 'OPTIONS' && (
      pathnameMatchesPrefix(pathnameLower, adminPathLower)
      || pathnameLower === adminLoginPathLower
      || (adminPathLower === '/admin' && pathnameLower === '/api/auth/login')
    )) {
      const headers = new Headers(dynamicCors);
      applySecurityHeaders(headers);
      if (headers.get('Access-Control-Allow-Origin') !== '*') mergeVaryHeader(headers, 'Origin');
      return new Response(null, { headers });
    }

    if (request.method === 'POST' && (pathnameLower === adminLoginPathLower || (adminPathLower === '/admin' && root === 'api' && segments[1] === 'auth' && segments[2] === 'login'))) {
      return Auth.handleLogin(request, env);
    }

    if (request.method === 'POST' && pathnameLower === adminPathLower) {
      if (!(await Auth.verifyRequest(request, env))) return jsonError('UNAUTHORIZED', '未授权', 401);
      try {
        return await normalizeJsonApiResponse(await Database.handleApi(request, env, ctx));
      } catch (e) {
        return jsonError('INTERNAL_ERROR', 'Server Error', 500, { reason: e?.message || 'unknown_error' });
      }
    }

    if (root) {
      const nodeData = await Database.getNode(root, env, ctx);
      if (nodeData) {
        const secret = nodeData.secret;
        let valid = true;
        let prefixLen = 0;

        if (secret) {
          const secretRaw = segments[1] || '';
          if (safeDecodeSegment(secretRaw) === secret) prefixLen = 1 + rootRaw.length + 1 + secretRaw.length;
          else valid = false;
        } else {
          prefixLen = 1 + rootRaw.length;
        }

        if (valid) {
          let remaining = normalizedPathname.substring(prefixLen);
          if (remaining === '' && !normalizedPathname.endsWith('/')) {
            const redirectUrl = new URL(request.url);
            redirectUrl.pathname = normalizedPathname + '/';
            const headers = new Headers({ 'Location': redirectUrl.toString(), 'Cache-Control': 'no-store' });
            applySecurityHeaders(headers);
            const redirectStatus = (request.method === 'GET' || request.method === 'HEAD') ? 301 : 307;
            return new Response(null, { status: redirectStatus, headers });
          }
          if (remaining === '') remaining = '/';
          remaining = sanitizeProxyPath(remaining);
          return Proxy.handle(request, nodeData, remaining, root, secret, env, ctx, { requestUrl, corsHeaders: dynamicCors });
        }
      }
    }

    const headers = new Headers(dynamicCors);
    applySecurityHeaders(headers);
    if (headers.get('Access-Control-Allow-Origin') !== '*') mergeVaryHeader(headers, 'Origin');
    return new Response('Not Found', { status: 404, headers });
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      const db = Database.getDB(env);
      const kv = Database.getKV(env);
      if (!kv) return;
      const runtimeConfig = await getRuntimeConfig(env);
      const scheduledLeaseMs = clampIntegerConfig(runtimeConfig?.scheduledLeaseMs, Config.Defaults.ScheduledLeaseMs, Config.Defaults.ScheduledLeaseMinMs, 15 * 60 * 1000);
      const leaseToken = `${nowMs()}-${Math.random().toString(36).slice(2, 10)}`;
      const lease = await Database.tryAcquireScheduledLease(kv, { token: leaseToken, leaseMs: scheduledLeaseMs });
      if (!lease.acquired) {
        await Database.patchOpsStatus(env, {
          scheduled: {
            lastSkippedAt: new Date().toISOString(),
            lastSkipReason: lease.reason || "lease_not_acquired",
            lock: {
              status: "busy",
              reason: lease.reason || "lease_not_acquired",
              expiresAt: lease.lock?.expiresAt || null
            }
          }
        }).catch(() => {});
        return;
      }

      const leaseState = {
        active: true,
        lostReason: null,
        lock: lease.lock || null
      };
      const renewLease = async () => {
        if (!leaseState.active) return null;
        const renewed = await Database.renewScheduledLease(kv, leaseToken, scheduledLeaseMs);
        if (!renewed) {
          leaseState.active = false;
          leaseState.lostReason = leaseState.lostReason || "lease_lost";
          return null;
        }
        leaseState.lock = renewed;
        return renewed;
      };
      const ensureLeaseActive = async () => {
        if (!leaseState.active) throw new Error(leaseState.lostReason || "scheduled_lease_lost");
        const renewed = await renewLease();
        if (!renewed) throw new Error(leaseState.lostReason || "scheduled_lease_lost");
        return renewed;
      };
      const leaseRefreshIntervalMs = Math.max(5000, Math.min(Math.floor(scheduledLeaseMs / 3), 60000));
      const waitForLeaseRefreshWindow = async () => {
        let remainingMs = leaseRefreshIntervalMs;
        while (leaseState.active && remainingMs > 0) {
          const sliceMs = Math.min(remainingMs, 1000);
          await sleepMs(sliceMs);
          remainingMs -= sliceMs;
        }
      };
      const leaseKeepalive = (async () => {
        while (leaseState.active) {
          await waitForLeaseRefreshWindow();
          if (!leaseState.active) break;
          await renewLease();
        }
      })().catch(() => {
        leaseState.active = false;
        leaseState.lostReason = leaseState.lostReason || "lease_renew_failed";
      });

      const startedAt = new Date().toISOString();
      await Database.patchOpsStatus(env, {
        scheduled: {
          status: "running",
          lastStartedAt: startedAt,
          lock: {
            status: "held",
            token: leaseToken,
            expiresAt: leaseState.lock?.expiresAt || (nowMs() + scheduledLeaseMs)
          }
        }
      }).catch(() => {});

      const scheduledState = {
        status: "success",
        lastStartedAt: startedAt,
        lastFinishedAt: null,
        lastSuccessAt: null,
        lastErrorAt: null,
        lastError: null,
        cleanup: {},
        kvTidy: {},
        report: {},
        alerts: {}
      };

      try {
        const config = runtimeConfig || {};
        const previousScheduledStatus = await Database.getOpsStatusSection(env, "scheduled").catch(() => ({}));
        
        if (db) {
          try {
            await ensureLeaseActive();
            const rawRetentionDays = Number(config.logRetentionDays);
            const retentionDays = Number.isFinite(rawRetentionDays)
              ? Math.min(Config.Defaults.LogRetentionDaysMax, Math.max(1, Math.floor(rawRetentionDays)))
              : Config.Defaults.LogRetentionDays;
            const expireTime = Date.now() - (retentionDays * 24 * 60 * 60 * 1000);
            await db.prepare("DELETE FROM proxy_logs WHERE timestamp < ?").bind(expireTime).run();
            const previousCleanupStatus = previousScheduledStatus?.cleanup && typeof previousScheduledStatus.cleanup === "object"
              ? previousScheduledStatus.cleanup
              : {};
            let vacuumStatus = "skipped";
            let vacuumError = null;
            let lastVacuumAt = typeof previousCleanupStatus.lastVacuumAt === "string" ? previousCleanupStatus.lastVacuumAt : "";
            if (Database.shouldRunLogsVacuum(lastVacuumAt)) {
              await ensureLeaseActive();
              try {
                await Database.vacuumLogsDb(db);
                vacuumStatus = "success";
                lastVacuumAt = new Date().toISOString();
              } catch (vacuumErr) {
                vacuumStatus = "failed";
                vacuumError = vacuumErr?.message || String(vacuumErr);
                scheduledState.status = "partial_failure";
                console.error("Scheduled DB VACUUM Error: ", vacuumErr);
              }
            }
            scheduledState.cleanup = {
              status: vacuumStatus === "failed" ? "partial_failure" : "success",
              lastSuccessAt: new Date().toISOString(),
              retentionDays,
              vacuumStatus,
              lastVacuumAt,
              vacuumError
            };
            await ensureLeaseActive();
          } catch (dbErr) {
            scheduledState.status = "partial_failure";
            scheduledState.cleanup = {
              status: "failed",
              lastErrorAt: new Date().toISOString(),
              lastError: dbErr?.message || String(dbErr)
            };
            console.error("Scheduled DB Cleanup Error: ", dbErr);
          }
        } else {
          scheduledState.cleanup = {
            status: "skipped",
            lastSkippedAt: new Date().toISOString(),
            reason: "db_not_configured"
          };
        }

        try {
          await ensureLeaseActive();
          const previousKvTidyState = previousScheduledStatus?.kvTidy && typeof previousScheduledStatus.kvTidy === "object"
            ? previousScheduledStatus.kvTidy
            : {};
          let lastKvTidyAt = typeof previousKvTidyState.lastSuccessAt === "string"
            ? previousKvTidyState.lastSuccessAt
            : "";
          if (Database.shouldRunKvTidy(lastKvTidyAt)) {
            const tidyResult = await Database.tidyKvData(env, { kv, ctx });
            lastKvTidyAt = new Date().toISOString();
            scheduledState.kvTidy = {
              status: "success",
              lastSuccessAt: lastKvTidyAt,
              lastTriggeredBy: "scheduled",
              summary: tidyResult.summary
            };
          } else {
            scheduledState.kvTidy = {
              status: "skipped",
              lastSkippedAt: new Date().toISOString(),
              lastSuccessAt: lastKvTidyAt,
              reason: "cooldown_active"
            };
          }
        } catch (kvTidyErr) {
          scheduledState.status = scheduledState.status === "success" ? "partial_failure" : scheduledState.status;
          scheduledState.kvTidy = {
            status: "failed",
            lastErrorAt: new Date().toISOString(),
            lastError: kvTidyErr?.message || String(kvTidyErr),
            lastTriggeredBy: "scheduled"
          };
          console.error("Scheduled KV Tidy Error: ", kvTidyErr);
        }
        
        const { tgBotToken, tgChatId } = config;
        if (tgBotToken && tgChatId) {
            try {
              await ensureLeaseActive();
              await Database.sendDailyTelegramReport(env);
              scheduledState.report = {
                status: "success",
                lastSuccessAt: new Date().toISOString()
              };
            } catch (reportErr) {
              scheduledState.status = scheduledState.status === "success" ? "partial_failure" : scheduledState.status;
              scheduledState.report = {
                status: "failed",
                lastErrorAt: new Date().toISOString(),
                lastError: reportErr?.message || String(reportErr)
              };
              console.error("Scheduled Daily Report Error: ", reportErr);
            }
        } else {
          scheduledState.report = {
            status: "skipped",
            lastSkippedAt: new Date().toISOString(),
            reason: "telegram_not_configured"
          };
        }

        try {
          await ensureLeaseActive();
          const alertResult = await Database.maybeSendRuntimeAlerts(env, scheduledState);
          scheduledState.alerts = alertResult.sent
            ? {
                status: "success",
                lastSuccessAt: new Date().toISOString(),
                issueCount: Number(alertResult.issueCount) || 0
              }
            : {
                status: "skipped",
                lastSkippedAt: new Date().toISOString(),
                reason: alertResult.reason || "no_alerts"
              };
        } catch (alertErr) {
          scheduledState.status = scheduledState.status === "success" ? "partial_failure" : scheduledState.status;
          scheduledState.alerts = {
            status: "failed",
            lastErrorAt: new Date().toISOString(),
            lastError: alertErr?.message || String(alertErr)
          };
          console.error("Scheduled Alert Error: ", alertErr);
        }
      } catch (err) {
          scheduledState.status = "failed";
          scheduledState.lastErrorAt = new Date().toISOString();
          scheduledState.lastError = err?.message || String(err);
          console.error("Scheduled Task Error: ", err);
      } finally {
          leaseState.active = false;
          await leaseKeepalive.catch(() => {});
          const finishedAt = new Date().toISOString();
          scheduledState.lastFinishedAt = finishedAt;
          if (scheduledState.status === "success") scheduledState.lastSuccessAt = finishedAt;
          const released = leaseState.lostReason ? false : await Database.releaseScheduledLease(kv, leaseToken).catch(() => false);
          scheduledState.lock = leaseState.lostReason
            ? {
                status: "lost",
                reason: leaseState.lostReason,
                lastCheckedAt: finishedAt
              }
            : {
                status: released ? "released" : "release_skipped",
                releasedAt: finishedAt
              };
          await Database.patchOpsStatus(env, { scheduled: scheduledState }).catch(() => {});
      }
    })());
  }
};
