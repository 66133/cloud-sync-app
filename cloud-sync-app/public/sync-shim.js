/**
 * sync-shim.js — 云端 IndexedDB 同步层
 * 在原应用加载前覆盖 window.indexedDB：
 *   - 读：启动时从服务端 bootstrap 全量状态到内存镜像，之后所有读操作走内存（速度与原版一致）
 *   - 写：本地立即生效 + 批量推送到服务端（约 150ms 合并），服务端持久化并广播给其它设备
 *   - 实时：SSE 接收其它设备的写入（失败自动降级为长轮询），远端变更后安全自动刷新页面
 *   - 离线：服务端不可达时回退到本地 IndexedDB 镜像，写操作入队（localStorage 持久化），恢复联网后补传
 * 实现了原应用用到的 IndexedDB API 子集（open/transaction/objectStore/index 及读写方法）。
 */
(function () {
  'use strict';
  if (window.__CLOUD_SYNC__) return;
  window.__CLOUD_SYNC__ = true;

  /* ================= 基础工具 ================= */
  var API = '/api/sync';
  var CLIENT_ID = (function () {
    try { var k = '__cs_client_id__'; var v = localStorage.getItem(k); if (v) return v; v = 'c' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8); localStorage.setItem(k, v); return v; } catch (e) { return 'c' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8); }
  })();
  var PENDING_KEY = '__cs_pending_ops__';
  var LOAD_TS = Date.now();

  function clone(v) {
    try { return typeof structuredClone === 'function' ? structuredClone(v) : JSON.parse(JSON.stringify(v)); }
    catch (e) { return v; }
  }
  function log() { try { console.debug.apply(console, ['[cloud-sync]'].concat([].slice.call(arguments))); } catch (e) {} }

  /* ================= 事件目标基类 ================= */
  /**
   * 为原型定义 on<event> 属性（如 onupgradeneeded / onsuccess）。
   * 注意：name 传入的是事件名（如 'upgradeneeded'），属性名为 'on' + name。
   */
  function defineHandler(proto, eventName) {
    var prop = 'on' + eventName;
    Object.defineProperty(proto, prop, {
      get: function () { return this['_h_' + eventName] || null; },
      set: function (fn) {
        var prev = this['_h_' + eventName];
        if (prev) { try { this.removeEventListener(eventName, prev); } catch (e) {} }
        // 回调绑定到本对象，兼容应用里使用 this 的写法
        this['_h_' + eventName] = fn ? fn.bind(this) : null;
        if (fn) { try { this.addEventListener(eventName, this['_h_' + eventName]); } catch (e) {} }
      },
      configurable: true, enumerable: true
    });
  }
  function EVT(name) { try { return new Event(name); } catch (e) { var ev = document.createEvent('Event'); ev.initEvent(name, false, false); return ev; } }

  /** 派发事件，并保证 event.target / event.currentTarget 指向派发对象（原生 dispatchEvent 在非原生 EventTarget 上不会设置 target） */
  function emitEvent(target, type) {
    var ev = EVT(type);
    try {
      Object.defineProperty(ev, 'target', { value: target, configurable: true });
      Object.defineProperty(ev, 'currentTarget', { value: target, configurable: true });
      Object.defineProperty(ev, 'srcElement', { value: target, configurable: true });
    } catch (e) { /* 忽略 */ }
    try { target.dispatchEvent(ev); } catch (e) { /* 忽略 */ }
    return ev;
  }

  function IDBEventTarget() {
    // 每个实例持有独立的原生 EventTarget，避免借用原生方法时 this 非法的 "Illegal invocation"
    var et = new EventTarget();
    // 用闭包方法，不直接覆盖实例属性，以免遮蔽原型上 on* 属性的 setter
    this._et = et;
  }
  IDBEventTarget.prototype._add = function (type, fn) { return this._et.addEventListener(type, fn); };
  IDBEventTarget.prototype._remove = function (type, fn) { return this._et.removeEventListener(type, fn); };
  IDBEventTarget.prototype._emit = function (evt) { return this._et.dispatchEvent(evt); };
  IDBEventTarget.prototype.addEventListener = function (type, fn, opts) { return this._et.addEventListener(type, fn, opts); };
  IDBEventTarget.prototype.removeEventListener = function (type, fn, opts) { return this._et.removeEventListener(type, fn, opts); };
  IDBEventTarget.prototype.dispatchEvent = function (evt) { return this._et.dispatchEvent(evt); };
  IDBEventTarget.prototype.constructor = IDBEventTarget;
  ['success', 'error', 'upgradeneeded', 'blocked', 'complete', 'abort', 'versionchange', 'close'].forEach(function (n) {
    defineHandler(IDBEventTarget.prototype, n);
  });

  /* ================= 内存状态 =================
     localState: Map<dbName, {version, stores: Map<storeName, {keyPath, indexes:[], records:Map<key,value>}>}> */
  var localState = new Map();

  function getDbState(name, version) {
    var db = localState.get(name);
    if (!db) { db = { version: version || 1, stores: new Map() }; localState.set(name, db); }
    if (version && version > db.version) db.version = version;
    return db;
  }
  function getStoreState(dbName, storeName) {
    var db = getDbState(dbName);
    var st = db.stores.get(storeName);
    if (!st) { st = { keyPath: 'id', indexes: [], records: new Map() }; db.stores.set(storeName, st); }
    return st;
  }
  function liveStringList(getArr) {
    return {
      get length() { return getArr().length; },
      item: function (i) { return getArr()[i]; },
      contains: function (n) { return getArr().indexOf(n) !== -1; },
      indexOf: function (n) { return getArr().indexOf(n); }
    };
  }
  function extractKeyPath(value, kp) {
    if (kp == null) return undefined;
    if (typeof kp === 'string') return value ? value[kp] : undefined;
    if (Array.isArray(kp)) return kp.map(function (p) { return value ? value[p] : undefined; });
    return undefined;
  }
  function cmpKeys(a, b) {
    var na = typeof a === 'number', nb = typeof b === 'number';
    if (na && nb) return a < b ? -1 : a > b ? 1 : 0;
    var sa = String(a), sb = String(b);
    return sa < sb ? -1 : sa > sb ? 1 : 0;
  }
  function keyMatch(query, key) {
    if (query == null) return true;
    if (typeof query === 'object' && query !== null && ('lower' in query || 'upper' in query)) {
      if (query.lower !== undefined && cmpKeys(key, query.lower) < 0) return false;
      if (query.upper !== undefined && cmpKeys(key, query.upper) > 0) return false;
      return true;
    }
    if (Array.isArray(key)) { try { return JSON.stringify(key) === JSON.stringify(query); } catch (e) { return false; } }
    return String(key) === String(query);
  }

  /* ================= 本地镜像（真 IndexedDB，离线兜底） ================= */
  var realIDB = window.indexedDB || null;
  var mirrorDB = null;
  function openMirror() {
    return new Promise(function (resolve) {
      if (!realIDB || mirrorDB !== null) return resolve(mirrorDB);
      try {
        var rq = realIDB.open('__cloudsync_mirror__', 1);
        rq.onupgradeneeded = function () { try { rq.result.createObjectStore('kv'); } catch (e) {} };
        rq.onsuccess = function () { mirrorDB = rq.result; resolve(mirrorDB); };
        rq.onerror = function () { mirrorDB = null; resolve(null); };
        setTimeout(function () { resolve(mirrorDB); }, 3000);
      } catch (e) { mirrorDB = null; resolve(null); }
    });
  }
  function mirrorKey(db, store, key) { return db + '/' + store + '/' + String(key); }
  function mirrorWrite(op) {
    if (!mirrorDB) return;
    try {
      if (op.op === 'put' || op.op === 'del') {
        var tx = mirrorDB.transaction('kv', 'readwrite'); var os = tx.objectStore('kv');
        if (op.op === 'put') os.put(op.value, mirrorKey(op.db, op.store, op.key));
        else os.delete(mirrorKey(op.db, op.store, op.key));
      } else if (op.op === 'clear') {
        var tx2 = mirrorDB.transaction('kv', 'readwrite'); var os2 = tx2.objectStore('kv');
        var prefix = op.db + '/' + op.store + '/';
        var req2 = os2.getAllKeys();
        req2.onsuccess = function () {
          (req2.result || []).forEach(function (k) {
            if (typeof k === 'string' && k.indexOf(prefix) === 0) os2.delete(k);
          });
        };
      } else if (op.op === 'schema') {
        var tx3 = mirrorDB.transaction('kv', 'readwrite');
        tx3.objectStore('kv').put({ version: op.version, stores: op.stores }, '__schema__/' + op.db);
      }
    } catch (e) { /* 镜像失败不影响主流程 */ }
  }
  function loadFromMirror() {
    return new Promise(function (resolve) {
      if (!mirrorDB) return resolve(false);
      try {
        var tx = mirrorDB.transaction('kv', 'readonly'); var os = tx.objectStore('kv');
        var req = os.getAllKeys(); var reqV = os.getAll();
        var done = false;
        function finish() {
          if (done || !req.result || !reqV.result) return; done = true;
          var keys = req.result, vals = reqV.result;
          var any = false;
          for (var i = 0; i < keys.length; i++) {
            var k = keys[i], v = vals[i];
            if (typeof k !== 'string') continue;
            if (k.indexOf('__schema__/') === 0) {
              var dbName = k.slice('__schema__/'.length);
              var sch = v || { version: 1, stores: {} };
              var dbS = getDbState(dbName, sch.version);
              Object.keys(sch.stores || {}).forEach(function (sn) {
                var s = sch.stores[sn] || {};
                var st = dbS.stores.get(sn);
                if (!st) { st = { keyPath: s.keyPath || 'id', indexes: s.indexes || [], records: new Map() }; dbS.stores.set(sn, st); }
                else { st.keyPath = s.keyPath || st.keyPath; if (s.indexes && s.indexes.length) st.indexes = s.indexes; }
              });
              any = true;
            } else {
              var sep1 = k.indexOf('/'), sep2 = k.indexOf('/', sep1 + 1);
              if (sep1 <= 0 || sep2 <= 0) continue;
              var db2 = k.slice(0, sep1), st2 = k.slice(sep1 + 1, sep2), key2 = k.slice(sep2 + 1);
              getStoreState(db2, st2).records.set(key2, v);
              any = true;
            }
          }
          resolve(any);
        }
        req.onsuccess = finish; reqV.onsuccess = finish;
        req.onerror = function () { resolve(false); }; reqV.onerror = function () { resolve(false); };
        setTimeout(function () { resolve(false); }, 4000);
      } catch (e) { resolve(false); }
    });
  }

  /* ================= 服务端同步（传输层） ================= */
  var rev = -1;
  var booted = false;
  var pendingOut = [];
  var flushing = false;
  var flushTimer = null;
  var backoff = 500;
  var sseFailed = false;
  var usingPoll = false;

  function hydrateServerState(payload) {
    // payload: {rev, dbs:{name:{version, stores:{name:{keyPath, indexes, records}}}}}
    var next = new Map();
    var dbs = (payload && payload.dbs) || {};
    Object.keys(dbs).forEach(function (dbName) {
      var db = dbs[dbName];
      var stores = new Map();
      Object.keys(db.stores || {}).forEach(function (sn) {
        var st = db.stores[sn];
        var records = new Map();
        Object.keys(st.records || {}).forEach(function (k) { records.set(k, st.records[k]); });
        stores.set(sn, { keyPath: st.keyPath || 'id', indexes: st.indexes || [], records: records });
      });
      next.set(dbName, { version: db.version || 1, stores: stores });
    });
    localState = next;
    if (payload && typeof payload.rev === 'number') rev = payload.rev;
  }

  function loadPendingFromStorage() {
    try {
      var raw = localStorage.getItem(PENDING_KEY);
      if (!raw) return;
      var arr = JSON.parse(raw);
      if (Array.isArray(arr)) {
        arr.forEach(function (e) { if (e && e.op) pendingOut.push(e); });
      }
    } catch (e) {}
  }
  function savePendingToStorage() {
    try {
      if (!pendingOut.length) { localStorage.removeItem(PENDING_KEY); return; }
      var s = JSON.stringify(pendingOut);
      if (s.length > 2.5 * 1024 * 1024) return; // 太大就只留内存
      localStorage.setItem(PENDING_KEY, s);
    } catch (e) {}
  }

  function enqueueOut(op) {
    // 合并同一 key 的连续 put/del（自动保存场景）
    if (op.op === 'put' || op.op === 'del') {
      for (var i = pendingOut.length - 1; i >= 0; i--) {
        var p = pendingOut[i];
        if (p.db !== op.db || p.store !== op.store) continue;
        if (p.op === 'clear') break;
        if ((p.op === 'put' || p.op === 'del') && String(p.key) === String(op.key)) { pendingOut.splice(i, 1); break; }
      }
    }
    pendingOut.push(op);
    savePendingToStorage();
    if (flushTimer) return;
    flushTimer = setTimeout(function () { flushTimer = null; flushOut(); }, 150);
  }

  function flushOut(beaconOnly) {
    if (flushing || !pendingOut.length) return;
    var ops = pendingOut.slice(0, 2000);
    if (beaconOnly) {
      try {
        navigator.sendBeacon && navigator.sendBeacon(API + '/ops', new Blob([JSON.stringify({ clientId: CLIENT_ID, ops: ops })], { type: 'application/json' }));
        pendingOut.splice(0, ops.length); savePendingToStorage();
      } catch (e) {}
      return;
    }
    flushing = true;
    fetch(API + '/ops', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ clientId: CLIENT_ID, ops: ops }), keepalive: true
    }).then(function (r) { if (!r.ok) throw new Error('http ' + r.status); return r.json(); })
      .then(function (j) {
        flushing = false; backoff = 500;
        pendingOut.splice(0, ops.length); savePendingToStorage();
        if (typeof j.rev === 'number' && j.rev > rev) rev = j.rev;
        if (pendingOut.length) { if (!flushTimer) flushTimer = setTimeout(function () { flushTimer = null; flushOut(); }, 150); }
      })
      .catch(function (e) {
        flushing = false;
        backoff = Math.min(backoff * 2, 15000);
        log('flush 失败，' + backoff + 'ms 后重试', e && e.message);
        setTimeout(function () { if (pendingOut.length && !flushTimer) { flushTimer = setTimeout(function () { flushTimer = null; flushOut(); }, 0); } }, backoff);
      });
  }

  function bootstrapFromServer(timeoutMs) {
    return new Promise(function (resolve, reject) {
      var settled = false;
      var t = setTimeout(function () { if (!settled) { settled = true; reject(new Error('bootstrap timeout')); } }, timeoutMs || 5000);
      fetch(API + '/bootstrap', { cache: 'no-store' }).then(function (r) {
        if (!r.ok) throw new Error('http ' + r.status);
        return r.json();
      }).then(function (j) {
        if (settled) return; settled = true; clearTimeout(t); resolve(j);
      }).catch(function (e) {
        if (settled) return; settled = true; clearTimeout(t); reject(e);
      });
    });
  }

  /* ---------- 远端 op 应用 + UI 刷新 ---------- */
  function applyRemote(op, newRev) {
    if (!op || typeof op !== 'object') return;
    if (op.op === 'schema') {
      var db = getDbState(op.db, op.version);
      (op.stores || []).forEach(function (s) {
        var st = db.stores.get(s.name);
        if (!st) { st = { keyPath: s.keyPath || 'id', indexes: [], records: new Map() }; db.stores.set(s.name, st); }
        if (s.keyPath) st.keyPath = s.keyPath;
        if (s.indexes && s.indexes.length) st.indexes = s.indexes;
      });
    } else if (op.op === 'put') {
      getStoreState(op.db, op.store).records.set(String(op.key), op.value);
    } else if (op.op === 'del') {
      getStoreState(op.db, op.store).records.delete(String(op.key));
    } else if (op.op === 'clear') {
      getStoreState(op.db, op.store).records.clear();
    }
    mirrorWrite(op);
    if (typeof newRev === 'number' && newRev > rev) rev = newRev;
    if (op.op !== 'schema') scheduleUiRefresh();
  }

  var refreshTimer = null;
  var refreshPending = false;
  var toastEl = null;
  var busySince = 0;

  function isEditable(el) {
    if (!el) return false;
    var tag = (el.tagName || '').toUpperCase();
    return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || el.isContentEditable === true;
  }
  /** 正在写作的编辑页：自动刷新会打断输入，改为提示用户手动刷新 */
  function isWritingPage() {
    return /\/(write|draft)\//.test(location.pathname) || /\/write(\?|$)/.test(location.pathname);
  }
  function isSafeToReload() {
    if (document.visibilityState !== 'visible') return false;
    if (Date.now() - LOAD_TS < 3000) return false;
    if (isEditable(document.activeElement)) return false;
    if (pendingOut.length) return false;
    if (flushing) return false;
    return true;
  }
  function ensureToast() {
    if (toastEl || !document.body) return;
    toastEl = document.createElement('div');
    toastEl.textContent = '☁️ 云端有更新，点击查看';
    toastEl.setAttribute('style', 'position:fixed;right:16px;bottom:20px;z-index:2147483647;background:#1e293b;color:#fff;padding:10px 16px;border-radius:999px;font-size:14px;font-family:system-ui,-apple-system,sans-serif;box-shadow:0 4px 16px rgba(0,0,0,.3);cursor:pointer;opacity:.96;transition:opacity .2s;user-select:none;');
    toastEl.onclick = function () { location.reload(); };
    document.body.appendChild(toastEl);
  }
  function removeToast() { if (toastEl) { try { toastEl.remove(); } catch (e) {} toastEl = null; } }
  function scheduleUiRefresh() {
    refreshPending = true;
    if (refreshTimer) return;
    refreshTimer = setInterval(function () {
      if (!refreshPending) { clearInterval(refreshTimer); refreshTimer = null; removeToast(); return; }
      // 编辑页 / 用户正在输入 / 有未上送的本地改动 → 只提示，不自动刷新
      if (isWritingPage() || !isSafeToReload()) { ensureToast(); return; }
      refreshPending = false; clearInterval(refreshTimer); refreshTimer = null; removeToast();
      log('远端有更新，自动刷新页面');
      location.reload();
    }, 1200);
  }
  function noteUserActivity() { busySince = Date.now(); }
  ['keydown', 'pointerdown'].forEach(function (evt) { window.addEventListener(evt, noteUserActivity, true); });

  /* ---------- SSE + 长轮询 ---------- */
  var sseHelloTimer = null;
  function startRealtime() {
    if (usingPoll || sseFailed) { startPolling(); return; }
    var es;
    try { es = new EventSource(API + '/events?clientId=' + CLIENT_ID + '&rev=' + Math.max(rev, 0)); }
    catch (e) { sseFailed = true; startPolling(); return; }
    var gotHello = false;
    sseHelloTimer = setTimeout(function () {
      if (!gotHello) { log('SSE 6 秒无响应，降级为长轮询'); try { es.close(); } catch (e) {} sseFailed = true; startPolling(); }
    }, 6000);
    es.addEventListener('hello', function (ev) {
      gotHello = true; clearTimeout(sseHelloTimer);
      try {
        var j = JSON.parse(ev.data);
        if (typeof j.rev === 'number' && j.rev > rev) catchUp();
      } catch (e) {}
    });
    es.addEventListener('op', function (ev) {
      gotHello = true; clearTimeout(sseHelloTimer);
      try {
        var m = JSON.parse(ev.data);
        if (m.clientId === CLIENT_ID) return;
        applyRemote(m.op, m.rev);
      } catch (e) {}
    });
    es.addEventListener('snapshot', function (ev) {
      gotHello = true;
      try {
        var s = JSON.parse(ev.data);
        hydrateServerState(s.state);
        // 重放仍在队列中的本地待传 op，避免被旧快照覆盖
        var replay = pendingOut.slice();
        hydrateThenReplay(s.state, replay);
        scheduleUiRefresh();
      } catch (e) {}
    });
    es.onerror = function () {
      if (es.readyState === 2) { // CLOSED，不再自动重连
        sseFailed = true; startPolling();
      }
      // readyState === 0/1 时 EventSource 会自动重连，hello 会重新校验
    };
  }
  function hydrateThenReplay(state, replay) {
    var next = new Map();
    var dbs = (state && state.dbs) || {};
    Object.keys(dbs).forEach(function (dbName) {
      var db = dbs[dbName]; var stores = new Map();
      Object.keys(db.stores || {}).forEach(function (sn) {
        var st = db.stores[sn]; var records = new Map();
        Object.keys(st.records || {}).forEach(function (k) { records.set(k, st.records[k]); });
        stores.set(sn, { keyPath: st.keyPath || 'id', indexes: st.indexes || [], records: records });
      });
      next.set(dbName, { version: db.version || 1, stores: stores });
    });
    localState = next;
    if (state && typeof state.rev === 'number') rev = state.rev;
    (replay || []).forEach(function (op) {
      if (op.op === 'put') getStoreState(op.db, op.store).records.set(String(op.key), op.value);
    });
  }
  var catchingUp = false;
  function catchUp() {
    if (catchingUp) return; catchingUp = true;
    bootstrapFromServer(6000).then(function (j) {
      var replay = pendingOut.slice();
      hydrateThenReplay(j, replay);
      catchingUp = false;
      scheduleUiRefresh();
    }).catch(function () { catchingUp = false; setTimeout(catchUp, 5000); });
  }

  var polling = false;
  function startPolling() {
    if (polling) return; polling = true; usingPoll = true;
    (function loop() {
      fetch(API + '/poll?clientId=' + CLIENT_ID + '&rev=' + Math.max(rev, 0) + '&timeout=25000', { cache: 'no-store' })
        .then(function (r) { if (!r.ok) throw new Error('http ' + r.status); return r.json(); })
        .then(function (j) {
          if (j.full || (typeof j.rev === 'number' && j.rev > rev && !(j.ops && j.ops.length))) { catchUp(); }
          else if (j.ops && j.ops.length) { j.ops.forEach(function (op) { applyRemote(op, undefined); }); rev = j.rev; }
          else if (typeof j.rev === 'number' && j.rev > rev) rev = j.rev;
          setTimeout(loop, 300);
        })
        .catch(function () { setTimeout(loop, 4000); });
    })();
  }

  /* ---------- 可见性恢复时校验 ---------- */
  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState !== 'visible') return;
    fetch(API + '/rev', { cache: 'no-store' }).then(function (r) { return r.json(); }).then(function (j) {
      if (typeof j.rev === 'number' && j.rev > rev) catchUp();
    }).catch(function () {});
  });
  window.addEventListener('online', function () { flushOut(); catchUp(); });
  window.addEventListener('pagehide', function () { flushOut(true); savePendingToStorage(); });
  window.addEventListener('beforeunload', function () { flushOut(true); savePendingToStorage(); });

  /* ================= Fake IndexedDB ================= */
  function FakeRequest(source, tx) {
    IDBEventTarget.call(this);
    this.result = undefined; this.error = null;
    this.source = source || null; this.transaction = tx || null;
    this.readyState = 'pending';
  }
  FakeRequest.prototype = Object.create(IDBEventTarget.prototype);
  FakeRequest.prototype.constructor = FakeRequest;
  FakeRequest.prototype._succeed = function (result) {
    var self = this;
    if (self._settled) return;
    self._settled = true;
    setTimeout(function () {
      self.result = result; self.error = null; self.readyState = 'done';
      emitEvent(self, 'success');
    }, 0);
  };
  FakeRequest.prototype._fail = function (err) {
    var self = this;
    if (self._settled) return;
    self._settled = true;
    setTimeout(function () {
      self.error = err || new Error('UnknownError'); self.readyState = 'done';
      emitEvent(self, 'error');
    }, 0);
  };
  /** 仅用于 open() 的 upgradeneeded 阶段（不终结请求，随后还会派发 success） */
  FakeRequest.prototype._fireUpgradeNeeded = function () {
    var self = this;
    self.readyState = 'done';
    emitEvent(self, 'upgradeneeded');
  };
  FakeRequest.prototype._fireSuccess = function (result) {
    var self = this;
    self._settled = true;
    self.readyState = 'done';
    self.result = result;
    emitEvent(self, 'success');
  };

  function FakeObjectStore(tx, dbName, name, stState) {
    IDBEventTarget.call(this);
    this.transaction = tx; this.name = name; this._dbName = dbName; this._st = stState;
    this.keyPath = stState.keyPath; this.autoIncrement = false;
    var self = this;
    this.indexNames = liveStringList(function () { return self._st.indexes.map(function (i) { return i.name; }); });
  }
  FakeObjectStore.prototype = Object.create(IDBEventTarget.prototype);
  FakeObjectStore.prototype.constructor = FakeObjectStore;
  FakeObjectStore.prototype._emitOp = function (op, req, result) {
    // 本地生效 + 镜像 + 入队上送
    try {
      if (op.op === 'put') this._st.records.set(String(op.key), op.value);
      else if (op.op === 'del') this._st.records.delete(String(op.key));
      else if (op.op === 'clear') this._st.records.clear();
      mirrorWrite(op); enqueueOut(op);
      req._succeed(result);
    } catch (e) { req._fail(e); }
  };
  FakeObjectStore.prototype.put = function (value, key) {
    var req = new FakeRequest(this, this.transaction);
    var k = key !== undefined ? key : extractKeyPath(value, this._st.keyPath);
    if (k === undefined || k === null) { req._fail(new Error('DataError: key missing')); return req; }
    var self = this;
    setTimeout(function () { self._emitOp({ op: 'put', db: self._dbName, store: self.name, key: k, value: clone(value) }, req, k); }, 0);
    return req;
  };
  FakeObjectStore.prototype.add = function (value, key) { return this.put(value, key); };
  FakeObjectStore.prototype.get = function (key) {
    var req = new FakeRequest(this, this.transaction);
    var self = this;
    setTimeout(function () { req._succeed(self._st.records.has(String(key)) ? clone(self._st.records.get(String(key))) : undefined); }, 0);
    return req;
  };
  FakeObjectStore.prototype.getKey = function (query) {
    var req = new FakeRequest(this, this.transaction);
    var self = this;
    setTimeout(function () {
      var found = undefined;
      self._st.records.forEach(function (v, k) { if (found === undefined && keyMatch(query, k)) found = k; });
      req._succeed(found);
    }, 0);
    return req;
  };
  FakeObjectStore.prototype.getAll = function (query) {
    var req = new FakeRequest(this, this.transaction);
    var self = this;
    setTimeout(function () {
      var out = [];
      self._st.records.forEach(function (v, k) { if (keyMatch(query, k)) out.push(clone(v)); });
      req._succeed(out);
    }, 0);
    return req;
  };
  FakeObjectStore.prototype.getAllKeys = function (query) {
    var req = new FakeRequest(this, this.transaction);
    var self = this;
    setTimeout(function () {
      var out = [];
      self._st.records.forEach(function (v, k) { if (keyMatch(query, k)) out.push(k); });
      req._succeed(out);
    }, 0);
    return req;
  };
  FakeObjectStore.prototype.count = function (query) {
    var req = new FakeRequest(this, this.transaction);
    var self = this;
    setTimeout(function () {
      var n = 0;
      self._st.records.forEach(function (v, k) { if (keyMatch(query, k)) n++; });
      req._succeed(n);
    }, 0);
    return req;
  };
  FakeObjectStore.prototype.delete = function (key) {
    var req = new FakeRequest(this, this.transaction);
    var self = this;
    setTimeout(function () { self._emitOp({ op: 'del', db: self._dbName, store: self.name, key: key }, req, undefined); }, 0);
    return req;
  };
  FakeObjectStore.prototype.clear = function () {
    var req = new FakeRequest(this, this.transaction);
    var self = this;
    setTimeout(function () { self._emitOp({ op: 'clear', db: self._dbName, store: self.name }, req, undefined); }, 0);
    return req;
  };
  FakeObjectStore.prototype.index = function (name) {
    for (var i = 0; i < this._st.indexes.length; i++) {
      if (this._st.indexes[i].name === name) return new FakeIndex(this, this._st.indexes[i]);
    }
    throw new Error('NotFoundError: ' + name);
  };
  FakeObjectStore.prototype.createIndex = function (name, keyPath, options) {
    options = options || {};
    var idx = { name: name, keyPath: keyPath, unique: !!options.unique };
    var exists = false;
    this._st.indexes = this._st.indexes.filter(function (i) { if (i.name === name) { exists = true; return false; } return true; });
    this._st.indexes.push(idx);
    return new FakeIndex(this, idx);
  };

  function FakeIndex(store, meta) {
    IDBEventTarget.call(this);
    this.objectStore = store; this.name = meta.name; this.keyPath = meta.keyPath; this._meta = meta;
  }
  FakeIndex.prototype = Object.create(IDBEventTarget.prototype);
  FakeIndex.prototype.constructor = FakeIndex;
  FakeIndex.prototype._scan = function (query, withValue) {
    var st = this.objectStore._st;
    var kp = this._meta.keyPath;
    var out = [];
    st.records.forEach(function (v, k) {
      var ik = extractKeyPath(v, kp);
      if (keyMatch(query, ik)) out.push({ k: ik, v: v });
    });
    out.sort(function (a, b) { return cmpKeys(a.k, b.k); });
    return out.map(function (e) { return withValue ? clone(e.v) : e.k; });
  };
  FakeIndex.prototype.get = function (key) {
    var req = new FakeRequest(this, this.objectStore, this.objectStore.transaction);
    var vals = this._scan(key, true);
    req._succeed(vals.length ? vals[0] : undefined);
    return req;
  };
  FakeIndex.prototype.getKey = function (key) {
    var req = new FakeRequest(this, this.objectStore, this.objectStore.transaction);
    var vals = this._scan(key, false);
    req._succeed(vals.length ? vals[0] : undefined);
    return req;
  };
  FakeIndex.prototype.getAll = function (query) {
    var req = new FakeRequest(this, this.objectStore, this.objectStore.transaction);
    req._succeed(this._scan(query, true));
    return req;
  };
  FakeIndex.prototype.getAllKeys = function (query) {
    var req = new FakeRequest(this, this.objectStore, this.objectStore.transaction);
    req._succeed(this._scan(query, false));
    return req;
  };
  FakeIndex.prototype.count = function (query) {
    var req = new FakeRequest(this, this.objectStore, this.objectStore.transaction);
    req._succeed(this._scan(query, true).length);
    return req;
  };

  function FakeTransaction(db, storeNames, mode) {
    IDBEventTarget.call(this);
    this.db = db; this.mode = mode || 'readonly';
    this._names = storeNames.slice();
    var self = this;
    this.objectStoreNames = liveStringList(function () { return self._names; });
  }
  FakeTransaction.prototype = Object.create(IDBEventTarget.prototype);
  FakeTransaction.prototype.constructor = FakeTransaction;
  FakeTransaction.prototype.objectStore = function (name) {
    if (this._names.indexOf(name) === -1) throw new Error('NotFoundError: ' + name);
    var dbState = localState.get(this.db.name);
    var stState = dbState && dbState.stores.get(name);
    if (!stState) throw new Error('NotFoundError: ' + name);
    return new FakeObjectStore(this, this.db.name, name, stState);
  };
  FakeTransaction.prototype.abort = function () { emitEvent(this, 'abort'); };

  function FakeDatabase(name, version, dbState) {
    IDBEventTarget.call(this);
    this.name = name; this.version = version; this._state = dbState;
    var self = this;
    this.objectStoreNames = liveStringList(function () { return Array.from(self._state.stores.keys()); });
  }
  FakeDatabase.prototype = Object.create(IDBEventTarget.prototype);
  FakeDatabase.prototype.constructor = FakeDatabase;
  FakeDatabase.prototype.transaction = function (storeNames, mode) {
    var names = Array.isArray(storeNames) ? storeNames.slice() : [storeNames];
    var dbState = this._state;
    for (var i = 0; i < names.length; i++) {
      if (!dbState.stores.has(names[i])) {
        var err = new Error('NotFoundError: ' + names[i]); err.name = 'NotFoundError'; throw err;
      }
    }
    return new FakeTransaction(this, names, mode);
  };
  FakeDatabase.prototype.createObjectStore = function (name, options) {
    options = options || {};
    var st = this._state.stores.get(name);
    if (!st) { st = { keyPath: options.keyPath != null ? options.keyPath : null, indexes: [], records: new Map() }; this._state.stores.set(name, st); }
    else if (options.keyPath != null) st.keyPath = options.keyPath;
    return new FakeObjectStore(new FakeTransaction(this, [name], 'versionchange'), this.name, name, st);
  };
  FakeDatabase.prototype.deleteObjectStore = function (name) { this._state.stores.delete(name); };
  FakeDatabase.prototype.close = function () { /* 状态常驻，close 不销毁 */ };

  function pushSchemaOp(dbName) {
    var dbState = getDbState(dbName);
    var stores = [];
    dbState.stores.forEach(function (st, sn) {
      stores.push({ name: sn, keyPath: st.keyPath, indexes: st.indexes });
    });
    var op = { op: 'schema', db: dbName, version: dbState.version, stores: stores };
    mirrorWrite(op); enqueueOut(op);
  }

  /* ---------- open() ---------- */
  var bootstrapPromise = null;
  function ensureBootstrap() {
    if (bootstrapPromise) return bootstrapPromise;
    bootstrapPromise = (function () {
      return openMirror().then(function () {
        return bootstrapFromServer(5000).then(function (j) {
          booted = true;
          hydrateServerState(j);
          loadPendingFromStorage();
          // 重放本地待传 op（离线期间可能积累了写入）
          pendingOut.slice().forEach(function (op) {
            if (op.op === 'put') getStoreState(op.db, op.store).records.set(String(op.key), op.value);
          });
          log('已从云端加载，rev=' + rev + '，待传 op=' + pendingOut.length);
          startRealtime();
          flushOut();
          return { online: true };
        }).catch(function (e) {
          log('云端不可达，回退本地镜像', e && e.message);
          loadPendingFromStorage();
          return loadFromMirror().then(function (any) {
            booted = true;
            rev = 0;
            // 后台继续尝试连上云端
            (function retry() {
              setTimeout(function () {
                bootstrapFromServer(5000).then(function (j) {
                  var replay = pendingOut.slice();
                  hydrateThenReplay(j, replay);
                  log('已恢复云端连接，rev=' + rev);
                  startRealtime(); flushOut(); scheduleUiRefresh();
                }).catch(retry);
              }, 8000);
            })();
            return { online: false, mirror: any };
          });
        });
      });
    })();
    return bootstrapPromise;
  }

  var CloudIndexedDB = {
    open: function (name, version) {
      version = version || 1;
      var req = new FakeRequest(null, null);
      ensureBootstrap().then(function () {
        var dbState = getDbState(name);
        var needsUpgrade = version > dbState.version || dbState.stores.size === 0;
        dbState.version = Math.max(dbState.version, version);
        var db = new FakeDatabase(name, dbState.version, dbState);
        req.result = db;
        setTimeout(function () {
          try {
            if (needsUpgrade) {
              req._fireUpgradeNeeded();          // 应用在此创建 object store / index
              pushSchemaOp(name);                // 升级后的 schema 上送
            }
            req._fireSuccess(db);
          } catch (e) {
            log('open(' + name + ') 事件派发异常: ' + (e && e.message));
            req._fail(e);
          }
        }, 0);
      }).catch(function (e) {
        log('open(' + name + ') 初始化失败: ' + (e && e.message));
        req._fail(new Error('云存储初始化失败: ' + (e && e.message)));
      });
      return req;
    },
    deleteDatabase: function (name) {
      var req = new FakeRequest(null, null);
      localState.delete(name);
      req._succeed(undefined);
      return req;
    },
    cmp: function (a, b) { return cmpKeys(a, b); },
    databases: function () {
      var req = new FakeRequest(null, null);
      var out = [];
      localState.forEach(function (v, k) { out.push({ name: k, version: v.version }); });
      req._succeed(out);
      return req;
    }
  };

  /* ================= 安装 ================= */
  // 原应用的日志/埋点上报 SDK 会向平台固定域名外呼，部署后不可达且无实际用途。
  // 这里拦截其上报请求，避免控制台噪音与多余的失败重试（不影响任何业务功能）。
  (function muteTelemetry() {
    var BLOCK = [/observability\/(logs|metrics|traces)\/collect/, /apex\.bytednsdoc\.com/, /slardar|ibytedapm|bytescm\.com|feishucdn\.com\/obj\//];
    var shouldBlock = function (url) {
      if (!url) return false;
      var u = String(url);
      for (var i = 0; i < BLOCK.length; i++) if (BLOCK[i].test(u)) return true;
      return false;
    };
    var origFetch = window.fetch;
    if (origFetch) {
      window.fetch = function (input, init) {
        var url = typeof input === 'string' ? input : (input && input.url) || '';
        if (shouldBlock(url)) return Promise.resolve(new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }));
        return origFetch.apply(this, arguments);
      };
    }
    var OrigXHR = window.XMLHttpRequest;
    if (OrigXHR && OrigXHR.prototype && OrigXHR.prototype.open) {
      var origOpen = OrigXHR.prototype.open;
      OrigXHR.prototype.open = function (method, url) {
        this.__csBlocked = shouldBlock(url);
        if (this.__csBlocked) return; // 直接不发出请求
        return origOpen.apply(this, arguments);
      };
    }
    if (navigator.sendBeacon) {
      var origBeacon = navigator.sendBeacon.bind(navigator);
      navigator.sendBeacon = function (url, data) {
        if (shouldBlock(url)) return true;
        return origBeacon(url, data);
      };
    }
  })();

  try {
    Object.defineProperty(window, 'indexedDB', {
      get: function () { return CloudIndexedDB; },
      set: function () { /* 拒绝再次覆盖 */ },
      configurable: false
    });
  } catch (e) { window.indexedDB = CloudIndexedDB; }
  log('云端同步层已安装 clientId=' + CLIENT_ID);
})();
