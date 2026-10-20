// Parser cron 5 trường: phút giờ ngày-trong-tháng tháng thứ-trong-tuần
// Hỗ trợ: *  */n  a  a-b  a-b/n  a/n  a,b,c   (thứ: 0 hoặc 7 = Chủ nhật)
// Tính theo giờ địa phương của máy đang chạy trình duyệt.

const FIELDS = [
  { name: 'phút', min: 0, max: 59 },
  { name: 'giờ', min: 0, max: 23 },
  { name: 'ngày', min: 1, max: 31 },
  { name: 'tháng', min: 1, max: 12 },
  { name: 'thứ', min: 0, max: 7 },
];

function parseField(field, { name, min, max }) {
  const out = new Set();
  for (const part of field.split(',')) {
    const m = part.match(/^(\*|\d+(?:-\d+)?)(?:\/(\d+))?$/);
    if (!m) throw new Error(`Trường ${name} không hợp lệ: "${part}"`);
    const step = m[2] ? Number(m[2]) : 1;
    let lo, hi;
    if (m[1] === '*') {
      lo = min; hi = max;
    } else {
      const [a, b] = m[1].split('-').map(Number);
      lo = a;
      hi = b !== undefined ? b : (m[2] ? max : a);
    }
    if (lo < min || hi > max || lo > hi || step < 1) {
      throw new Error(`Trường ${name} ngoài phạm vi ${min}-${max}: "${part}"`);
    }
    for (let v = lo; v <= hi; v += step) out.add(v);
  }
  return out;
}

export function parseCron(expr) {
  const parts = String(expr || '').trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error('Cron phải có đúng 5 trường: phút giờ ngày tháng thứ');
  }
  const [minute, hour, dom, month, dow] = parts.map((p, i) => parseField(p, FIELDS[i]));
  if (dow.has(7)) { dow.delete(7); dow.add(0); }
  return { minute, hour, dom, month, dow, domStar: parts[2] === '*', dowStar: parts[4] === '*' };
}

function dayMatches(c, d) {
  const dom = c.dom.has(d.getDate());
  const dow = c.dow.has(d.getDay());
  if (c.domStar && c.dowStar) return true;
  if (c.domStar) return dow;
  if (c.dowStar) return dom;
  return dom || dow; // giống cron chuẩn: cả hai đều đặt thì khớp một trong hai
}

// Thời điểm chạy kế tiếp, tính từ phút sau `from`
export function nextRun(cron, from = new Date()) {
  const d = new Date(from.getTime());
  d.setSeconds(0, 0);
  d.setMinutes(d.getMinutes() + 1);
  const limit = from.getTime() + 2 * 366 * 24 * 3600 * 1000;
  while (d.getTime() <= limit) {
    if (!cron.month.has(d.getMonth() + 1)) { d.setMonth(d.getMonth() + 1, 1); d.setHours(0, 0, 0, 0); continue; }
    if (!dayMatches(cron, d)) { d.setDate(d.getDate() + 1); d.setHours(0, 0, 0, 0); continue; }
    if (!cron.hour.has(d.getHours())) { d.setHours(d.getHours() + 1, 0, 0, 0); continue; }
    if (!cron.minute.has(d.getMinutes())) { d.setMinutes(d.getMinutes() + 1, 0, 0); continue; }
    return d;
  }
  return null;
}

export function nextRuns(expr, n = 3, from = new Date()) {
  const c = parseCron(expr);
  const out = [];
  let t = from;
  for (let i = 0; i < n; i++) {
    const r = nextRun(c, t);
    if (!r) break;
    out.push(r);
    t = r;
  }
  return out;
}
