/**
 * 短篇拆文助手 · 云端同步版后端
 * - 静态托管原应用（/app/app_17e3ev426db/* → SPA fallback）
 * - 同步 API：bootstrap / ops / events(SSE) / poll(长轮询降级)
 * - 持久化：data/snapshot.json（原子快照）+ data/oplog.jsonl（增量日志）
 * 零 npm 依赖，纯 Node 标准库。
 */
'use strict';
const http = require('http');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const zlib = require('zlib');
const { URL } = require('url');
const { Readable } = require('stream');

/* 原平台上游：AI 能力（story_analyzer_1 / writing_coach_ai_generator_1 等）在此执行 */
const UPSTREAM_ORIGIN = process.env.UPSTREAM_ORIGIN || 'https://4m1wmbcu3t32z.aiforce.cloud';

const PORT = parseInt(process.env.PORT || '3000', 10);
const HOST = '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const SNAPSHOT_FILE = path.join(DATA_DIR, 'snapshot.json');
const OPLOG_FILE = path.join(DATA_DIR, 'oplog.jsonl');
const APP_PREFIX = '/app/app_17e3ev426db';
const INDEX_HTML = path.join(PUBLIC_DIR, 'app', 'index.html');
const MAX_BODY = 64 * 1024 * 1024; // 64MB（整库导入场景）
const COMPACT_EVERY = 400;         // 每 N 条 op 做一次快照压缩
const OPS_BUFFER = 2000;           // 内存中保留的近期 op（供长轮询追赶）

/* ---------------- 内存状态 ---------------- */
/** @type {Map<string, {version:number, stores:Map<string,{keyPath:string,indexes:Array,records:Map<string,any>}>}>} */
let dbs = new Map();
let rev = 0;
let opsSinceSnapshot = 0;
let dirty = false;
/** 近期 op 环形缓冲 [{rev, op}] */
const recentOps = [];
/** SSE 客户端集合 [{res, clientId}] */
const sseClients = new Set();
/** 长轮询等待者 [{res, clientId, sinceRev, timer}] */
const pollers = new Set();

/* ---------------- 持久化 ---------------- */
function ensureDataDir() { if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true }); }

function serializeState() {
  const out = { rev, dbs: {} };
  for (const [dbName, db] of dbs) {
    const stores = {};
    for (const [storeName, st] of db.stores) {
      const records = {};
      for (const [k, v] of st.records) records[k] = v;
      stores[storeName] = { keyPath: st.keyPath, indexes: st.indexes, records };
    }
    out.dbs[dbName] = { version: db.version, stores };
  }
  return out;
}

function hydrateState(obj) {
  const next = new Map();
  let r = obj.rev || 0;
  if (obj.dbs) {
    for (const [dbName, db] of Object.entries(obj.dbs)) {
      const stores = new Map();
      if (db.stores) {
        for (const [storeName, st] of Object.entries(db.stores)) {
          const records = new Map();
          if (st.records) for (const [k, v] of Object.entries(st.records)) records.set(k, v);
          stores.set(storeName, { keyPath: st.keyPath || 'id', indexes: st.indexes || [], records });
        }
      }
      next.set(dbName, { version: db.version || 1, stores });
    }
  }
  dbs = next; rev = r;
}

async function writeSnapshotAtomic() {
  ensureDataDir();
  const tmp = SNAPSHOT_FILE + '.tmp';
  const data = JSON.stringify(serializeState());
  await fsp.writeFile(tmp, data, 'utf8');
  await fsp.rename(tmp, SNAPSHOT_FILE);
  await fsp.writeFile(OPLOG_FILE, '', 'utf8'); // 快照后清空 oplog
  opsSinceSnapshot = 0;
}

let appendChain = Promise.resolve();
function appendOplog(line) {
  appendChain = appendChain.then(() => fsp.appendFile(OPLOG_FILE, line + '\n', 'utf8')).catch(e => {
    console.error('[oplog append failed]', e.message);
  });
  return appendChain;
}

async function loadState() {
  ensureDataDir();
  hydrateState({});
  // 1) 快照
  try {
    if (fs.existsSync(SNAPSHOT_FILE)) {
      const raw = await fsp.readFile(SNAPSHOT_FILE, 'utf8');
      if (raw.trim()) hydrateState(JSON.parse(raw));
      console.log(`[storage] 快照已加载 rev=${rev}`);
    }
  } catch (e) { console.error('[storage] 快照损坏，跳过:', e.message); }
  // 2) 追加日志
  let replayed = 0;
  try {
    if (fs.existsSync(OPLOG_FILE)) {
      const raw = await fsp.readFile(OPLOG_FILE, 'utf8');
      for (const line of raw.split('\n')) {
        const t = line.trim(); if (!t) continue;
        let rec; try { rec = JSON.parse(t); } catch { continue; }
        if (typeof rec.rev === 'number' && rec.rev > rev && rec.op) { applyOpToMemory(rec.op, rec.rev); rev = rec.rev; replayed++; }
      }
    }
  } catch (e) { console.error('[storage] oplog 重放失败:', e.message); }
  if (replayed) console.log(`[storage] 重放 ${replayed} 条增量，当前 rev=${rev}`);
  // 启动时兜底写一次快照，修剪 oplog
  try { await writeSnapshotAtomic(); } catch (e) { console.error('[storage] 启动快照失败:', e.message); }
}

/* ---------------- op 应用与广播 ---------------- */
function getDb(name, version) {
  let db = dbs.get(name);
  if (!db) { db = { version: version || 1, stores: new Map() }; dbs.set(name, db); }
  if (version && version > db.version) db.version = version;
  return db;
}
function getStore(dbName, storeName, version) {
  const db = getDb(dbName, version);
  let st = db.stores.get(storeName);
  if (!st) { st = { keyPath: 'id', indexes: [], records: new Map() }; db.stores.set(storeName, st); }
  return st;
}

/** 将一条 op 应用到内存（不落盘、不广播）。返回 true 表示有数据变化 */
function applyOpToMemory(op, newRev) {
  if (!op || typeof op !== 'object') return false;
  if (op.op === 'schema') {
    const db = getDb(op.db, op.version);
    for (const s of (op.stores || [])) {
      let st = db.stores.get(s.name);
      if (!st) { st = { keyPath: s.keyPath || 'id', indexes: [], records: new Map() }; db.stores.set(s.name, st); }
      if (s.keyPath) st.keyPath = s.keyPath;
      if (Array.isArray(s.indexes) && s.indexes.length) st.indexes = s.indexes;
    }
    return false;
  }
  if (op.op === 'put') {
    if (!op.db || !op.store) return false;
    const st = getStore(op.db, op.store);
    st.records.set(String(op.key), op.value);
    return true;
  }
  if (op.op === 'del') {
    const st = getStore(op.db, op.store);
    return st.records.delete(String(op.key));
  }
  if (op.op === 'clear') {
    const st = getStore(op.db, op.store);
    const had = st.records.size > 0; st.records.clear(); return had;
  }
  return false;
}

function broadcast(op, newRev, fromClientId) {
  const payload = `event: op\ndata: ${JSON.stringify({ rev: newRev, op, clientId: fromClientId })}\n\n`;
  for (const c of sseClients) {
    if (c.clientId === fromClientId) continue;
    try { c.res.write(payload); } catch { /* ignore */ }
  }
  // 长轮询等待者
  for (const p of pollers) {
    if (p.clientId === fromClientId) continue;
    if (p.sinceRev < newRev) finishPoll(p, newRev);
  }
}

function recordOp(op, newRev) {
  recentOps.push({ rev: newRev, op });
  if (recentOps.length > OPS_BUFFER) recentOps.splice(0, recentOps.length - OPS_BUFFER);
}

/** 处理一批客户端提交的 op，返回最新 rev */
async function handleOps(ops) {
  let dataChanged = false;
  for (const op of ops) {
    rev += 1;
    const changed = applyOpToMemory(op, rev);
    dataChanged = dataChanged || changed;
    recordOp(op, rev);
    appendOplog(JSON.stringify({ rev, op }));
  }
  if (ops.length) {
    opsSinceSnapshot += ops.length; dirty = true;
    if (opsSinceSnapshot >= COMPACT_EVERY) { try { await writeSnapshotAtomic(); } catch (e) { console.error(e.message); } }
  }
  return rev;
}

/* ---------------- HTTP 工具 ---------------- */
const MIME = {
  '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.html': 'text/html; charset=utf-8',
  '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif',
  '.ico': 'image/x-icon', '.txt': 'text/plain; charset=utf-8', '.woff2': 'font/woff2',
  '.map': 'application/json', '.webmanifest': 'application/manifest+json'
};

function sendJson(res, code, obj, extraHeaders) {
  const body = JSON.stringify(obj);
  res.writeHead(code, Object.assign({
    'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store'
  }, extraHeaders || {}));
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []; let size = 0;
    req.on('data', c => {
      size += c.length;
      if (size > MAX_BODY) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function serveFile(req, res, filePath, cache) {
  fs.stat(filePath, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not Found'); return; }
    const ext = path.extname(filePath).toLowerCase();
    const type = MIME[ext] || 'application/octet-stream';
    const headers = { 'Content-Type': type, 'Cache-Control': cache || 'no-cache' };
    const raw = fs.createReadStream(filePath);
    const accept = req.headers['accept-encoding'] || '';
    if (/\b(gzip|br|deflate)\b/.test(accept) && st.size > 2048 && /\.(js|css|json|html|svg|txt)$/.test(ext)) {
      const enc = accept.includes('gzip') ? 'gzip' : (accept.includes('deflate') ? 'deflate' : null);
      if (enc) {
        headers['Content-Encoding'] = enc;
        res.writeHead(200, headers);
        raw.pipe(zlib.createGzip ? (enc === 'gzip' ? zlib.createGzip() : zlib.createDeflate()) : raw).pipe(res);
        return;
      }
    }
    res.writeHead(200, headers);
    raw.pipe(res);
  });
}

/* ---------------- SSE / 长轮询 ---------------- */
function sseWrite(res, chunk) { try { res.write(chunk); } catch { /* ignore */ } }

function handleSSE(req, res, query) {
  const clientId = query.get('clientId') || '';
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });
  res.write('retry: 3000\n\n');
  res.write(`event: hello\ndata: ${JSON.stringify({ rev, ts: Date.now() })}\n\n`);
  // 落后太多 → 直接补发全量
  const clientRev = parseInt(query.get('rev') || '-1', 10);
  if (clientRev >= 0 && clientRev < rev) sendSnapshotEvent(res);
  const client = { res, clientId };
  sseClients.add(client);
  const hb = setInterval(() => sseWrite(res, ': ping\n\n'), 20000);
  req.on('close', () => { clearInterval(hb); sseClients.delete(client); });
}

function sendSnapshotEvent(res) {
  sseWrite(res, `event: snapshot\ndata: ${JSON.stringify({ rev, state: serializeState() })}\n\n`);
}

function finishPoll(p, newRev) {
  if (pollers.has(p)) {
    pollers.delete(p);
    clearTimeout(p.timer);
    const missed = recentOps.filter(r => r.rev > p.sinceRev && r.rev <= newRev);
    try {
      p.res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
      p.res.end(JSON.stringify({ rev: newRev, ops: missed.map(m => m.op), full: missed.length === 0 && p.sinceRev < newRev }));
    } catch { /* ignore */ }
  }
}

function handlePoll(req, res, query) {
  const clientId = query.get('clientId') || '';
  const sinceRev = parseInt(query.get('rev') || '0', 10);
  const timeoutMs = Math.min(Math.max(parseInt(query.get('timeout') || '25000', 10), 1000), 55000);
  if (sinceRev < rev) {
    const missed = recentOps.filter(r => r.rev > sinceRev);
    if (missed.length || rev - sinceRev > 50) {
      return sendJson(res, 200, { rev, ops: missed.map(m => m.op), full: missed.length === 0 });
    }
  }
  const p = { res, clientId, sinceRev: rev, timer: null };
  p.timer = setTimeout(() => finishPoll(p, rev), timeoutMs);
  pollers.add(p);
  req.on('close', () => { if (pollers.has(p)) { pollers.delete(p); clearTimeout(p.timer); } });
}

/* ---------------- 平台运行时反向代理（AI 能力等） ----------------
   原应用的 AI 拆文/写作助手走 /app/<appId>/__runtime__/api/v1/plugin_server/capability/<id>/execute/stream，
   这里原样转发回原平台执行（已验证支持匿名调用），保证 AI 功能 100% 可用。 */
const HOP_BY_HOP = ['connection', 'keep-alive', 'transfer-encoding', 'upgrade', 'proxy-authenticate', 'proxy-authorization', 'te', 'trailer'];

async function proxyRuntime(req, res, pathname) {
  const target = new URL(UPSTREAM_ORIGIN + req.url);
  let body;
  if (req.method !== 'GET' && req.method !== 'HEAD') body = await readBody(req);
  // 用最小头集合转发（已验证可行）。透传浏览器的原始头（sec-fetch-* / cookie / ua 等）
  // 会被上游 WAF 挂起到超时，因此这里只保留必要头。
  const headers = {
    'content-type': req.headers['content-type'] || 'application/json',
    'accept': req.headers['accept'] || '*/*',
    'origin': UPSTREAM_ORIGIN,
    'referer': UPSTREAM_ORIGIN + '/',
    'accept-encoding': 'identity',
    'user-agent': 'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36'
  };
  const isSSE = /\/execute\/stream(\?|$)/.test(req.url) || /text\/event-stream/.test(req.headers['accept'] || '');

  /* 上游 AI 能力（SSE 流式）的首字延迟存在长尾波动：实测 2~25s 常见，偶尔更久。
     两层风险：上游网关(stgw)60s 超时返回 504；本平台入口网关同样 ~60s 无字节即掐断浏览器连接。
     对策（仅 SSE 类请求）：
       1) 立即向浏览器写 200 + SSE 响应头，每 15s 发一行 SSE 注释（": ping"）保活，
          中间网关再也不会因空闲掐断；
       2) 上游 504 / 超时自动重试一次（TTFT 随机，重试大概率落在快区间，AI 分析无副作用）；
       3) 两次仍失败则向流内写一条 error 事件 —— 原前端本就支持流内错误展示。 */
  let keepAlive = null;
  if (isSSE) {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache',
      'X-Accel-Buffering': 'no',
      'Connection': 'keep-alive'
    });
    keepAlive = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) { /* ignore */ } }, 15000);
  }

  const cleanup = () => { if (keepAlive) { clearInterval(keepAlive); keepAlive = null; } };
  req.on('close', () => { cleanup(); });

  const t0 = Date.now();
  let upstream = null, lastErr = null;
  for (let attempt = 0; attempt < 2 && !upstream; attempt++) {
    try {
      const r = await fetch(target, {
        method: req.method, headers,
        body: body && body.length ? body : undefined,
        redirect: 'manual', signal: AbortSignal.timeout(65000)
      });
      if ((r.status === 502 || r.status === 504) && attempt === 0) {
        try { if (r.body) await r.body.cancel(); } catch (e) { /* ignore */ }
        continue; // 网关超时：立即重试一次
      }
      upstream = r;
    } catch (e) {
      lastErr = e; // 超时/网络错误也重试一次（仅第一次失败时）
    }
  }

  if (isSSE) {
    cleanup();
    if (!upstream) {
      const msg = (lastErr && lastErr.message) || 'upstream timeout';
      try {
        res.write(`data: ${JSON.stringify({ status_code: '0', data: { type: 'error', error: { code: 'proxy_upstream_unreachable', message: '上游服务暂时不可用，请稍后重试 (' + msg + ')' } } })}\n\n`);
      } catch (e) { /* ignore */ }
      res.end();
      return;
    }
    if (!upstream.ok) {
      const text = await upstream.text().catch(() => '');
      try {
        res.write(`data: ${JSON.stringify({ status_code: '0', data: { type: 'error', error: { code: 'proxy_upstream_' + upstream.status, message: ('上游返回 ' + upstream.status + '，请重试。' + text.slice(0, 160)) } } })}\n\n`);
      } catch (e) { /* ignore */ }
      res.end();
      return;
    }
    if (upstream.body) {
      const rs = Readable.fromWeb(upstream.body);
      rs.on('error', () => { try { res.end(); } catch (e) {} });
      rs.pipe(res);
    } else res.end();
    return;
  }

  /* 非 SSE 路径：保持原样透传状态码 */
  if (!upstream) {
    return sendJson(res, 504, { error: 'upstream timeout after retry: ' + ((lastErr && lastErr.message) || '504') });
  }
  const respHeaders = {};
  upstream.headers.forEach((v, k) => {
    if (HOP_BY_HOP.includes(k) || k === 'content-encoding' || k === 'content-length') return;
    respHeaders[k] = v;
  });
  respHeaders['x-accel-buffering'] = 'no';
  respHeaders['cache-control'] = 'no-cache';
  res.writeHead(upstream.status, respHeaders);
  if (upstream.body) {
    const rs = Readable.fromWeb(upstream.body);
    rs.on('error', () => { try { res.end(); } catch (e) {} });
    rs.pipe(res);
  } else res.end();
}

/* ---------------- 路由 ---------------- */
const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = decodeURIComponent(u.pathname);
  const query = u.searchParams;

  try {
    /* ---- 同步 API ---- */
    if (pathname === '/api/sync/bootstrap') {
      if (req.method !== 'GET') return sendJson(res, 405, { error: 'method' });
      const body = Buffer.from(JSON.stringify({ rev, dbs: serializeState().dbs }));
      const headers = { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' };
      if ((req.headers['accept-encoding'] || '').includes('gzip')) headers['Content-Encoding'] = 'gzip';
      res.writeHead(200, headers);
      res.end(headers['Content-Encoding'] ? zlib.gzipSync(body) : body);
      return;
    }
    if (pathname === '/api/sync/ops') {
      if (req.method !== 'POST') return sendJson(res, 405, { error: 'method' });
      const buf = await readBody(req);
      let parsed; try { parsed = JSON.parse(buf.toString('utf8')); } catch { return sendJson(res, 400, { error: 'bad json' }); }
      const ops = Array.isArray(parsed && parsed.ops) ? parsed.ops.slice(0, 5000) : [];
      const fromClient = (parsed && parsed.clientId) || '';
      // 逐条应用并广播
      for (const op of ops) {
        rev += 1;
        applyOpToMemory(op, rev);
        recordOp(op, rev);
        appendOplog(JSON.stringify({ rev, op }));
        broadcast(op, rev, fromClient);
      }
      if (ops.length) {
        opsSinceSnapshot += ops.length; dirty = true;
        if (opsSinceSnapshot >= COMPACT_EVERY) { try { await writeSnapshotAtomic(); } catch (e) { console.error(e.message); } }
      }
      return sendJson(res, 200, { rev });
    }
    if (pathname === '/api/sync/events') { handleSSE(req, res, query); return; }
    if (pathname === '/api/sync/poll') { handlePoll(req, res, query); return; }
    if (pathname === '/api/sync/rev') { return sendJson(res, 200, { rev }); }
    if (pathname === '/api/health') { return sendJson(res, 200, { ok: true, rev, ts: Date.now(), build: 'v4-sse-keepalive' }); }
    /* 网关超时诊断：?ms=N 静默 N 毫秒后响应；?mode=sse 立即写 SSE 头并每 15s 保活 */
    if (pathname === '/api/debug/slow') {
      const ms = Math.min(parseInt(query.get('ms') || '70000', 10), 110000);
      if (query.get('mode') === 'sse') {
        res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no' });
        const ka = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 15000);
        setTimeout(() => { clearInterval(ka); try { res.write(`data: ${JSON.stringify({ ok: true, ms })}\n\n`); res.end(); } catch (e) {} }, ms);
        return;
      }
      setTimeout(() => sendJson(res, 200, { ok: true, ms }), ms);
      return;
    }
    /* 出网连通性诊断（仅限白名单目标，方法仅 GET/HEAD） */
    if (pathname === '/api/debug/net') {
      const target = query.get('url') || UPSTREAM_ORIGIN + '/';
      if (!/^https?:\/\//.test(target)) return sendJson(res, 400, { error: 'bad url' });
      const t0 = Date.now();
      try {
        const r = await fetch(target, { method: 'GET', redirect: 'manual', signal: AbortSignal.timeout(10000) });
        return sendJson(res, 200, { ok: true, status: r.status, ms: Date.now() - t0, target });
      } catch (e) {
        return sendJson(res, 200, { ok: false, error: String(e && e.message || e), ms: Date.now() - t0, target });
      }
    }
    /* POST 出网诊断：逐项模拟代理路径的差异（消融实验）
       ?ua=1 发 UA 头；?ae=1 发 accept-encoding:identity；?accept=1 发 accept:text/event-stream
       ?redirect=1 用 redirect:'manual'；?url=1 用 new URL() 构造目标；?big=1 用 ~40KB 大 body
       ?timeout=N 超时毫秒数（默认 25000，即 25 秒） */
    if (pathname === '/api/debug/post') {
      const buf = await readBody(req);
      const t0 = Date.now();
      const headers = { 'content-type': 'application/json', 'origin': UPSTREAM_ORIGIN, 'referer': UPSTREAM_ORIGIN + '/' };
      if (query.get('ua')) headers['user-agent'] = 'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36';
      if (query.get('ae')) headers['accept-encoding'] = 'identity';
      if (query.get('accept')) headers['accept'] = 'text/event-stream';
      let body = buf.length ? buf : Buffer.from(JSON.stringify({ action: 'textGenerate', params: { story_content: '深夜，他推开那扇虚掩的门，屋里只有一盏油灯还亮着。' } }));
      if (query.get('big')) {
        body = Buffer.from(JSON.stringify({ action: 'textGenerate', params: { story_content: '雨下了一整夜，巷口的青石板泛着幽幽的光。'.repeat(500) } }));
      }
      const opt = {
        method: 'POST', headers,
        body,
        signal: AbortSignal.timeout(parseInt(query.get('timeout') || '25000', 10))
      };
      if (query.get('redirect')) opt.redirect = 'manual';
      let target = UPSTREAM_ORIGIN + '/app/app_17e3ev426db/__runtime__/api/v1/plugin_server/capability/story_analyzer_1/execute/stream';
      if (query.get('url')) target = new URL(target);
      try {
        const r = await fetch(target, opt);
        const reader = r.body && r.body.getReader();
        let first = null;
        if (reader) {
          const { value } = await reader.read();
          first = value ? Buffer.from(value).toString('utf8').slice(0, 160) : null;
          reader.cancel().catch(() => {});
        }
        return sendJson(res, 200, { ok: true, status: r.status, ms: Date.now() - t0, first, bodyBytes: body.length, flags: query.toString() });
      } catch (e) {
        return sendJson(res, 200, { ok: false, error: String(e && e.message || e), ms: Date.now() - t0, bodyBytes: body.length, flags: query.toString() });
      }
    }

    /* 路由探针：验证 /app/* 路径能否到达本服务 */
    if (pathname === APP_PREFIX + '/echo' || pathname === APP_PREFIX + '/__echo') {
      return sendJson(res, 200, { reached: true, path: pathname, url: req.url, ts: Date.now() });
    }

    /* ---- 平台运行时（AI 能力 / 文件存储等）反向代理 ---- */
    if (pathname.startsWith(APP_PREFIX + '/__runtime__/')) {
      // 遥测类上报（日志/链路/指标）：静默接受，避免控制台噪音与无谓外呼
      if (/\/observability\/(logs|metrics|traces)\/collect$/.test(pathname)) {
        return sendJson(res, 200, { code: 0, msg: 'ok' });
      }
      // 时间戳同步：本地直接返回，省一次跨网往返
      if (/\/observability\/current_server_timestamp$/.test(pathname)) {
        return sendJson(res, 200, { data: { timestampNs: String(Date.now()) + '000000' } });
      }
      return proxyRuntime(req, res, pathname);
    }

    /* ---- 入口重定向 ---- */
    if (pathname === '/' || pathname === '/index.html' || pathname === '/app' || pathname === '/app/') {
      res.writeHead(302, { Location: APP_PREFIX + '/stats' });
      res.end();
      return;
    }

    /* ---- 原应用资源：/app_17e3ev426db/25/client/assets/... ---- */
    if (pathname.startsWith('/app_17e3ev426db/')) {
      const rel = pathname.slice('/app_17e3ev426db/'.length).replace(/\.\./g, '');
      const fp = path.join(PUBLIC_DIR, 'app_17e3ev426db', rel);
      if (fp.startsWith(PUBLIC_DIR) && fs.existsSync(fp) && fs.statSync(fp).isFile()) {
        return serveFile(req, res, fp, 'public, max-age=31536000, immutable');
      }
      res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not Found');
      return;
    }

    /* ---- SPA：/app/app_17e3ev426db/* ---- */
    if (pathname === APP_PREFIX || pathname.startsWith(APP_PREFIX + '/')) {
      return serveFile(req, res, INDEX_HTML, 'no-cache');
    }

    /* ---- 其它静态文件（sync-shim.js 等） ---- */
    if (req.method === 'GET' && !pathname.includes('..')) {
      const fp = path.join(PUBLIC_DIR, pathname);
      if (fp.startsWith(PUBLIC_DIR) && fs.existsSync(fp) && fs.statSync(fp).isFile()) {
        return serveFile(req, res, fp, 'no-cache');
      }
    }

    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not Found');
  } catch (e) {
    console.error('[http error]', e);
    try { sendJson(res, 500, { error: String(e && e.message || e) }); } catch { /* ignore */ }
  }
});

/* ---------------- 定时快照与优雅退出 ---------------- */
setInterval(() => {
  if (dirty) { dirty = false; writeSnapshotAtomic().catch(e => console.error('[snapshot]', e.message)); }
}, 30000).unref();

async function shutdown(signal) {
  console.log(`\n[${signal}] 正在落盘并退出…`);
  try { await appendChain; await writeSnapshotAtomic(); console.log('[storage] 最终快照已写入'); } catch (e) { console.error('[storage] 退出快照失败:', e.message); }
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

/* ---------------- 启动 ---------------- */
loadState().then(() => {
  server.listen(PORT, HOST, () => {
    console.log(`[server] 短篇拆文助手·云端同步版 已启动: http://${HOST}:${PORT}`);
    console.log(`[server] 应用入口: http://localhost:${PORT}${APP_PREFIX}/stats`);
  });
}).catch(e => { console.error('[fatal] 启动失败:', e); process.exit(1); });
