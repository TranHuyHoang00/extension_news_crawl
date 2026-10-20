// Kho dữ liệu IndexedDB (chỉ background dùng), dùng chung cho mọi site (mỗi link ghi kèm host). Gồm 3 bảng:
//   seen   – link đã gửi Kafka thành công (cache chống trùng)  { url, host, savedAt }
//   outbox – bài đã crawl, đang chờ gửi Kafka                  { url, host, topic, batchSize, record, bytes, createdAt, attempts, lastError }
//   claims – "giữ chỗ" bài đang được một lượt chạy xử lý        { url, owner, expiresAt }
// Một link chỉ nằm ở một trong ba trạng thái: đang xử lý → chờ gửi → đã gửi.

const DB_NAME = 'news_crawler';
const DB_VERSION = 1;

let dbPromise = null;

function openDb() {
  if (!dbPromise) {
    dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = e => {
        const db = req.result;
        if (e.oldVersion < 1) {
          const seen = db.createObjectStore('seen', { keyPath: 'url' });
          seen.createIndex('host', 'host');
          seen.createIndex('host_savedAt', ['host', 'savedAt']);
          const out = db.createObjectStore('outbox', { keyPath: 'url' });
          out.createIndex('topic', 'topic');
          out.createIndex('topic_createdAt', ['topic', 'createdAt']);
          out.createIndex('createdAt', 'createdAt');
          db.createObjectStore('claims', { keyPath: 'url' });
        }
      };
      req.onsuccess = () => {
        const db = req.result;
        db.onversionchange = () => { db.close(); dbPromise = null; };
        resolve(db);
      };
      req.onerror = () => { dbPromise = null; reject(req.error); };
    });
  }
  return dbPromise;
}

const reqDone = req => new Promise((resolve, reject) => {
  req.onsuccess = () => resolve(req.result);
  req.onerror = () => reject(req.error);
});

const txDone = tx => new Promise((resolve, reject) => {
  tx.oncomplete = () => resolve();
  tx.onerror = () => reject(tx.error);
  tx.onabort = () => reject(tx.error);
});

async function open(stores, mode = 'readonly') {
  const db = await openDb();
  const tx = db.transaction(stores, mode);
  const s = {};
  for (const name of [].concat(stores)) s[name] = tx.objectStore(name);
  return { tx, s };
}

// Duyệt con trỏ khoá trên một index, xoá từng bản ghi khớp. Trả về số bản ghi đã xoá.
async function deleteByIndex(storeName, indexName, range) {
  const { tx, s } = await open(storeName, 'readwrite');
  let n = 0;
  const req = s[storeName].index(indexName).openKeyCursor(range);
  req.onsuccess = () => {
    const cur = req.result;
    if (!cur) return;
    s[storeName].delete(cur.primaryKey);
    n++;
    cur.continue();
  };
  await txDone(tx);
  return n;
}

// ===================== seen (cache) =====================

export async function countSeen(host) {
  const { s } = await open('seen');
  return reqDone(s.seen.index('host').count(IDBKeyRange.only(host)));
}

export function clearHost(host) {
  return deleteByIndex('seen', 'host', IDBKeyRange.only(host));
}

export function purgeOlderThan(host, cutoff) {
  return deleteByIndex('seen', 'host_savedAt', IDBKeyRange.bound([host, 0], [host, cutoff], false, true));
}

// ===================== known + claims =====================

// Trong danh sách url, url nào KHÔNG cần crawl: đã gửi, đang chờ gửi, hoặc lượt chạy khác đang giữ chỗ
export async function known(urls, owner = null) {
  if (!urls?.length) return [];
  const now = Date.now();
  const { s } = await open(['seen', 'outbox', 'claims']);
  const rows = await Promise.all(urls.map(u => Promise.all([
    reqDone(s.seen.getKey(u)),
    reqDone(s.outbox.getKey(u)),
    reqDone(s.claims.get(u)),
  ])));
  return urls.filter((_, i) => {
    const [seen, queued, claim] = rows[i];
    if (seen !== undefined || queued !== undefined) return true;
    return !!claim && claim.owner !== owner && claim.expiresAt > now;
  });
}

// Giữ chỗ một url cho owner. Trả về true nếu giữ được.
export async function claim(url, owner, ttlMs) {
  const now = Date.now();
  const { tx, s } = await open(['seen', 'outbox', 'claims'], 'readwrite');
  const [seen, queued, cur] = await Promise.all([
    reqDone(s.seen.getKey(url)),
    reqDone(s.outbox.getKey(url)),
    reqDone(s.claims.get(url)),
  ]);
  let ok = seen === undefined && queued === undefined
    && (!cur || cur.owner === owner || cur.expiresAt <= now);
  if (ok) s.claims.put({ url, owner, expiresAt: now + ttlMs });
  await txDone(tx);
  return ok;
}

export async function release(url, owner) {
  const { tx, s } = await open('claims', 'readwrite');
  const cur = await reqDone(s.claims.get(url));
  if (cur && cur.owner === owner) s.claims.delete(url);
  await txDone(tx);
}

export async function purgeClaims() {
  const now = Date.now();
  const { tx, s } = await open('claims', 'readwrite');
  let n = 0;
  const req = s.claims.openCursor();
  req.onsuccess = () => {
    const cur = req.result;
    if (!cur) return;
    if (cur.value.expiresAt <= now) { cur.delete(); n++; }
    cur.continue();
  };
  await txDone(tx);
  return n;
}

// Trả lại mọi chỗ giữ có owner bắt đầu bằng prefix (các lượt cũ của một tab: runId = "<tabId>-<thời điểm>").
// Dùng khi tab bắt đầu lượt mới: lượt cũ đã chết (F5, tab treo) nên chỗ giữ của nó không còn ai xử lý.
export async function releaseByOwnerPrefix(prefix) {
  const { tx, s } = await open('claims', 'readwrite');
  let n = 0;
  const req = s.claims.openCursor();
  req.onsuccess = () => {
    const cur = req.result;
    if (!cur) return;
    if (String(cur.value.owner || '').startsWith(prefix)) { cur.delete(); n++; }
    cur.continue();
  };
  await txDone(tx);
  return n;
}

// ===================== outbox =====================

// Đưa bài vào hàng chờ gửi và bỏ giữ chỗ (trong cùng một giao dịch)
export async function enqueue(item) {
  const { tx, s } = await open(['outbox', 'claims'], 'readwrite');
  s.outbox.put({ attempts: 0, lastError: null, ...item });
  s.claims.delete(item.url);
  await txDone(tx);
}

export async function outboxCount(topic = null) {
  const { s } = await open('outbox');
  return reqDone(topic ? s.outbox.index('topic').count(IDBKeyRange.only(topic)) : s.outbox.count());
}

export async function outboxTopics() {
  const { s } = await open('outbox');
  return new Promise((resolve, reject) => {
    const topics = [];
    const req = s.outbox.index('topic').openKeyCursor(null, 'nextunique');
    req.onsuccess = () => {
      const cur = req.result;
      if (!cur) return resolve(topics);
      topics.push(cur.key);
      cur.continue();
    };
    req.onerror = () => reject(req.error);
  });
}

// Lấy batch cũ nhất của topic: tối đa maxCount bài và không vượt maxBytes (trừ khi 1 bài đã vượt).
// full = true nếu batch đã đạt số lượng hoặc dung lượng → nên gửi ngay.
export async function peekBatch(topic, maxCount, maxBytes) {
  const { s } = await open('outbox');
  return new Promise((resolve, reject) => {
    const items = [];
    let bytes = 0;
    const range = IDBKeyRange.bound([topic, 0], [topic, Infinity]);
    const req = s.outbox.index('topic_createdAt').openCursor(range);
    req.onsuccess = () => {
      const cur = req.result;
      if (!cur) return resolve({ items, bytes, full: items.length >= maxCount || bytes >= maxBytes });
      const v = cur.value;
      if (items.length && bytes + v.bytes > maxBytes) return resolve({ items, bytes, full: true });
      items.push(v);
      bytes += v.bytes;
      if (items.length >= maxCount) return resolve({ items, bytes, full: true });
      cur.continue();
    };
    req.onerror = () => reject(req.error);
  });
}

// Gửi thành công: xoá khỏi hàng chờ, ghi vào cache
export async function commitSent(items) {
  const now = Date.now();
  const { tx, s } = await open(['outbox', 'seen'], 'readwrite');
  for (const it of items) {
    s.outbox.delete(it.url);
    s.seen.put({ url: it.url, host: it.host, savedAt: now });
  }
  await txDone(tx);
}

export async function markFailed(items, error) {
  const { tx, s } = await open('outbox', 'readwrite');
  for (const it of items) s.outbox.put({ ...it, attempts: (it.attempts || 0) + 1, lastError: error });
  await txDone(tx);
}

export function purgeOutbox(cutoff) {
  return deleteByIndex('outbox', 'createdAt', IDBKeyRange.upperBound(cutoff, true));
}