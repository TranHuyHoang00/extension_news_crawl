import { nextRuns } from './lib/cron.js';
import { DEFAULTS, GLOBAL_DEFAULTS, originPattern } from './lib/config.js';

const $ = id => document.getElementById(id);
const send = msg => chrome.runtime.sendMessage(msg);
const fmt = ts => (ts ? new Date(ts).toLocaleString('vi-VN', { hour12: false }) : '—');

let tabId = null;
let formFilled = false;

function readForm() {
  return {
    home: $('home').value,
    cates: $('cates').value,
    regex: $('regex').value,
    cron: $('cron').value,
    delayMin: $('delayMin').value,
    delayMax: $('delayMax').value,
    cacheDays: $('cacheDays').value,
    topic: $('topic').value,
    batchSize: $('batchSize').value,
  };
}

function readGlobal() {
  return { gatewayUrl: $('gatewayUrl').value, apiKey: $('apiKey').value };
}

function fillGlobal(g) {
  const v = { ...GLOBAL_DEFAULTS, ...(g || {}) };
  $('gatewayUrl').value = v.gatewayUrl;
  $('apiKey').value = v.apiKey;
}

function fillForm(cfg) {
  const c = { ...DEFAULTS, ...(cfg || {}) };
  $('home').value = c.home || '';
  $('cates').value = (c.cates || []).join('\n');
  $('regex').value = c.regex;
  $('cron').value = c.cron;
  $('delayMin').value = c.delayMin;
  $('delayMax').value = c.delayMax;
  $('cacheDays').value = c.cacheDays;
  $('topic').value = c.topic;
  $('batchSize').value = c.batchSize;
  updateHints();
}

// Domain của link home + các link cate (mỗi tab một site)
function cateOrigins() {
  const out = new Set();
  for (const line of [$('home').value, ...$('cates').value.split(/\r?\n/)]) {
    try { if (line.trim()) out.add(new URL(line.trim()).origin); } catch {}
  }
  return [...out];
}

// Xin quyền truy cập site (và gateway). Phải gọi ngay trong thao tác bấm nút (Chrome yêu cầu).
async function requestPermissions({ gateway }) {
  const origins = cateOrigins().map(o => `${o}/*`);
  if (gateway) {
    const g = originPattern($('gatewayUrl').value);
    if (g) origins.push(g);
  }
  if (!origins.length) return true; // thiếu link sẽ được báo lỗi ở bước kiểm tra cấu hình
  const ok = await chrome.permissions.request({ origins });
  if (!ok) showErrors([`Chưa được cấp quyền truy cập ${origins.join(', ')}, extension không chạy được`]);
  return ok;
}

function setHint(el, text, bad = false) {
  el.textContent = text;
  el.classList.toggle('bad', bad);
}

function updateHints() {
  try {
    const runs = nextRuns($('cron').value, 3);
    setHint($('cronHint'), runs.length ? `Tiếp theo: ${runs.map(d => fmt(d)).join(' · ')}` : 'Không có lần chạy nào');
  } catch (e) {
    setHint($('cronHint'), e.message, true);
  }

  const re = $('regex').value.trim();
  if (!re) setHint($('regexHint'), 'Bắt buộc nhập. Bấm Test crawl để xem regex lấy được những link nào.', true);
  else {
    try { new RegExp(re); setHint($('regexHint'), 'Regex hợp lệ'); }
    catch (e) { setHint($('regexHint'), `Regex lỗi: ${e.message}`, true); }
  }

  const origins = cateOrigins();
  if (origins.length > 1) setHint($('catesHint'), `Đang có ${origins.length} domain. Mỗi tab chỉ chạy một site: ${origins.join(', ')}`, true);
  else setHint($('catesHint'), origins.length ? `Site: ${origins[0]}` : '');
}

function showErrors(list) {
  const box = $('errors');
  box.hidden = !list?.length;
  box.textContent = (list || []).join('\n');
}

async function refresh() {
  const s = await send({ type: 'GET_STATE', tabId });
  if (!s?.ok) return;
  if (!formFilled) { fillForm(s.cfg); fillGlobal(s.global); formFilled = true; }

  const st = s.status || {};
  const badge = $('badge');
  if (st.running) { badge.textContent = `Đang chạy · đã gửi ${st.runSent || 0}`; badge.className = 'badge running'; }
  else if (st.enabled) { badge.textContent = 'Đang bật'; badge.className = 'badge on'; }
  else { badge.textContent = 'Tắt'; badge.className = 'badge off'; }

  $('nextRun').textContent = st.enabled ? fmt(st.nextRunAt) : '— (lịch đang tắt)';
  $('lastRun').textContent = st.lastRunAt ? `${fmt(st.lastRunAt)} · ${st.lastRunSummary || ''}` : '—';
  $('totalSent').textContent = st.totalSent || 0;
  $('pendingCount').textContent = s.pendingCount;
  $('btnFlush').hidden = !s.pendingCount;
  $('seenCount').textContent = s.seenCount;
  $('cacheKeep').textContent = s.cfg ? ` (giữ ${s.cfg.cacheDays ?? DEFAULTS.cacheDays} ngày)` : '';

  const err = $('lastError');
  err.hidden = !st.lastError;
  err.textContent = st.lastError ? `Lỗi gần nhất: ${st.lastError}` : '';

  $('btnRun').disabled = !!st.running || !s.cfg;
  $('btnStop').disabled = !st.enabled && !st.running;
  $('btnTest').disabled = !!st.running;

  const logs = $('logs');
  const atBottom = logs.scrollTop + logs.clientHeight >= logs.scrollHeight - 4;
  logs.textContent = (s.logs || []).slice(-60).join('\n') || 'Chưa có hoạt động';
  if (atBottom) logs.scrollTop = logs.scrollHeight;
}

function showTest(r) {
  $('testBox').hidden = false;
  $('testSummary').textContent =
    `Nguồn: ${r.source}\nTìm thấy ${r.total} link bài ở trang đầu, ${r.alreadySeen} link đã có trong cache.`;
  $('testSummary').style.whiteSpace = 'pre-wrap';
  const ol = $('testLinks');
  ol.innerHTML = '';
  for (const u of r.links) {
    const li = document.createElement('li');
    li.textContent = u;
    ol.appendChild(li);
  }
  $('testSample').textContent = r.sample
    ? JSON.stringify(r.sample, null, 2)
    : 'Không có link nào khớp regex — kiểm tra lại regex hoặc link cate.';
}

// fn được gọi ngay (đồng bộ) để chrome.permissions.request vẫn nằm trong thao tác bấm
async function withBusy(btn, label, fn) {
  const old = btn.innerHTML;
  btn.disabled = true;
  btn.textContent = label;
  try { await fn(); } finally { btn.innerHTML = old; btn.disabled = false; refresh(); }
}

// Mỗi tab có panel riêng: tab id nằm trên URL (sidepanel.html?tabId=123).
// Nếu mở bằng panel mặc định (không có tabId) thì làm việc với tab đang xem và đổi theo khi chuyển tab.
const pinnedTabId = Number(new URLSearchParams(location.search).get('tabId')) || null;

async function resolveTabId() {
  if (pinnedTabId) return pinnedTabId;
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab.id;
}

// Đổi sang tab khác → nạp cấu hình của tab đó (thay đổi chưa lưu ở tab cũ sẽ bị bỏ)
async function switchTab(newId) {
  if (newId === tabId) return;
  tabId = newId;
  formFilled = false;
  showErrors([]);
  $('testBox').hidden = true;
  await updateTabInfo();
  await refresh();
}

async function updateTabInfo() {
  try {
    const tab = await chrome.tabs.get(tabId);
    $('tabInfo').textContent = `Tab #${tab.id} · ${tab.url || ''}`;
  } catch {
    $('tabInfo').textContent = `Tab #${tabId} đã đóng`;
  }
}

async function init() {
  tabId = await resolveTabId();
  await updateTabInfo();
  chrome.tabs.onUpdated.addListener((id, info) => { if (id === tabId && info.url) updateTabInfo(); });
  if (!pinnedTabId) {
    const win = await chrome.windows.getCurrent();
    chrome.tabs.onActivated.addListener(({ tabId: id, windowId }) => {
      if (windowId === win.id) switchTab(id);
    });
  }

  ['cron', 'regex', 'cates', 'home'].forEach(id => $(id).addEventListener('input', updateHints));

  $('btnShowKey').addEventListener('click', () => {
    const input = $('apiKey');
    const show = input.type === 'password';
    input.type = show ? 'text' : 'password';
    $('btnShowKey').textContent = show ? 'Ẩn' : 'Hiện';
  });

  $('btnSave').addEventListener('click', () => withBusy($('btnSave'), 'Đang lưu…', async () => {
    showErrors([]);
    if (!(await requestPermissions({ gateway: true }))) return;
    const r = await send({ type: 'SAVE', tabId, raw: readForm(), rawGlobal: readGlobal(), enable: true });
    showErrors(r?.ok ? [] : (r?.errors || [r?.error]));
  }));

  $('btnTestGw').addEventListener('click', () => withBusy($('btnTestGw'), 'Đang gửi thử…', async () => {
    showErrors([]);
    if (!(await requestPermissions({ gateway: true }))) return;
    const r = await send({ type: 'TEST_GATEWAY', tabId, rawGlobal: readGlobal(), topic: $('topic').value });
    if (r?.ok) alert(`Gateway OK (HTTP ${r.status})${r.text ? `\n${r.text}` : ''}`);
    else showErrors(r?.errors || [`Gateway lỗi: ${r?.error || 'không rõ'}`]);
  }));

  $('btnFlush').addEventListener('click', () => withBusy($('btnFlush'), 'đang gửi…', async () => {
    const r = await send({ type: 'FLUSH_NOW', tabId });
    showErrors(r?.ok ? [] : [`Gửi hàng chờ thất bại: ${r?.error || 'không rõ'}`]);
  }));

  $('btnStop').addEventListener('click', () => withBusy($('btnStop'), 'Đang tắt…', async () => {
    await send({ type: 'STOP', tabId });
  }));

  $('btnRun').addEventListener('click', () => withBusy($('btnRun'), 'Đang khởi động…', async () => {
    const r = await send({ type: 'RUN_NOW', tabId });
    showErrors(r?.ok ? [] : [r?.error || 'Không chạy được']);
  }));

  $('btnTest').addEventListener('click', () => withBusy($('btnTest'), 'Đang test…', async () => {
    showErrors([]);
    if (!(await requestPermissions({ gateway: false }))) return;
    const r = await send({ type: 'TEST', tabId, raw: readForm() });
    if (r?.ok) showTest(r.result);
    else showErrors(r?.errors || [r?.error || 'Test thất bại']);
  }));

  $('btnClear').addEventListener('click', async () => {
    if (!confirm('Xoá cache của site này? Lần chạy sau sẽ crawl và gửi lại cả những bài đã gửi.')) return;
    await send({ type: 'CLEAR_CACHE', tabId });
    refresh();
  });

  await refresh();
  setInterval(refresh, 1500);
}

init();
