import { parseCron } from "./cron.js";

// Cấu hình của một tab
export const DEFAULTS = {
  home: "", // link home (không bắt buộc): tab được đưa về đây trước khi chạy; trống → trang chủ của domain link cate
  cates: [], // link chuyên mục, cùng một domain (mỗi tab chạy một site); trống → quét link home
  cron: "*/30 * * * *",
  regex: "", // regex lọc link bài viết, bắt buộc nhập
  delayMin: 3,
  delayMax: 8,
  cacheDays: 30, // link đã gửi được nhớ bao nhiêu ngày (chống crawl trùng)
  topic: "", // Kafka topic của tab này
  batchSize: 10, // số bài gửi lên gateway trong một request
};

// Cấu hình dùng chung cho mọi tab
export const GLOBAL_DEFAULTS = {
  gatewayUrl: "",
  apiKey: "",
};

export const LIMITS = {
  batchMax: 100,
};

// Origin dạng "https://host/*" để xin quyền truy cập (site hoặc gateway)
export function originPattern(url) {
  try {
    const u = new URL(String(url || "").trim());
    if (!/^https?:$/.test(u.protocol)) return null;
    return `${u.origin}/*`;
  } catch {
    return null;
  }
}

// Link home thực dùng: ô Link home nếu có nhập, không thì trang chủ của domain link cate đầu tiên
export const homeOf = (cfg) =>
  cfg?.home || (cfg?.cates?.[0] ? new URL(cfg.cates[0]).origin + "/" : null);

// Các trang cần quét: link cate, để trống thì quét link home
export const sourcesOf = (cfg) => (cfg?.cates?.length ? cfg.cates : cfg?.home ? [cfg.home] : []);

// Kiểm tra một link, trả về URL đã chuẩn hoá hoặc null (ghi lỗi)
function checkUrl(value, label, errors) {
  try {
    const u = new URL(value);
    if (!/^https?:$/.test(u.protocol)) throw new Error();
    u.hash = "";
    return u.href;
  } catch {
    errors.push(`${label} không phải URL hợp lệ: ${value}`);
    return null;
  }
}

// Chuẩn hoá + kiểm tra cấu hình của một tab. Trả về { cfg, errors }
//   requireKafka: true khi lưu để chạy thật (bắt buộc có topic); false khi chỉ test crawl
export function normalizeConfig(raw = {}, { requireKafka = true } = {}) {
  const errors = [];
  const cfg = { ...DEFAULTS };

  const home = String(raw.home || "").trim();
  cfg.home = home ? checkUrl(home, "Link home", errors) || "" : "";

  const cates = Array.isArray(raw.cates) ? raw.cates : String(raw.cates || "").split(/\r?\n/);
  cfg.cates = cates
    .map((x) => x.trim())
    .filter(Boolean)
    .map((s) => checkUrl(s, "Link cate", errors))
    .filter(Boolean);

  if (!cfg.home && !cfg.cates.length && !errors.length) errors.push("Nhập link cate (hoặc link home)");
  const hosts = new Set([cfg.home, ...cfg.cates].filter(Boolean).map((u) => new URL(u).origin));
  if (hosts.size > 1)
    errors.push(`Mỗi tab chỉ chạy một site. Link home và link cate đang thuộc ${hosts.size} domain: ${[...hosts].join(", ")}`);

  cfg.cron = String(raw.cron || "").trim();
  try {
    parseCron(cfg.cron);
  } catch (e) {
    errors.push(e.message);
  }

  cfg.regex = String(raw.regex || "").trim();
  if (!cfg.regex) {
    errors.push("Chưa nhập regex lọc link bài viết");
  } else {
    try {
      new RegExp(cfg.regex);
    } catch (e) {
      errors.push(`Regex lỗi: ${e.message}`);
    }
  }

  cfg.delayMin = Number(raw.delayMin);
  cfg.delayMax = Number(raw.delayMax);
  if (!Number.isFinite(cfg.delayMin) || !Number.isFinite(cfg.delayMax) || cfg.delayMin < 0) {
    errors.push("Delay phải là số giây không âm");
  } else if (cfg.delayMin > cfg.delayMax) {
    errors.push("Delay tối thiểu phải nhỏ hơn hoặc bằng tối đa");
  } else if (cfg.delayMax > 120) {
    errors.push("Delay tối đa không quá 120 giây");
  }

  cfg.cacheDays = Number(raw.cacheDays ?? DEFAULTS.cacheDays);
  if (!Number.isInteger(cfg.cacheDays) || cfg.cacheDays < 1 || cfg.cacheDays > 365) {
    errors.push("Số ngày giữ cache phải là số nguyên từ 1 đến 365");
  }

  cfg.topic = String(raw.topic || "").trim();
  if (cfg.topic && !/^[A-Za-z0-9._-]{1,249}$/.test(cfg.topic)) {
    errors.push("Topic chỉ gồm chữ, số, dấu chấm, gạch ngang, gạch dưới");
  } else if (!cfg.topic && requireKafka) {
    errors.push("Chưa nhập Kafka topic");
  }

  cfg.batchSize = Number(raw.batchSize ?? DEFAULTS.batchSize);
  if (!Number.isInteger(cfg.batchSize) || cfg.batchSize < 1 || cfg.batchSize > LIMITS.batchMax) {
    errors.push(`Batch size phải là số nguyên từ 1 đến ${LIMITS.batchMax}`);
  }
  return { cfg, errors };
}

// Chuẩn hoá + kiểm tra cấu hình gateway dùng chung. Trả về { global, errors }
export function normalizeGlobal(raw = {}) {
  const errors = [];
  const global = { ...GLOBAL_DEFAULTS };

  const url = String(raw.gatewayUrl || "").trim();
  if (!url) {
    errors.push("Chưa nhập Gateway URL");
  } else {
    try {
      const u = new URL(url);
      if (!/^https?:$/.test(u.protocol)) throw new Error();
      u.searchParams.delete("apikey"); // API key nhập ở ô riêng
      global.gatewayUrl = u.href;
    } catch {
      errors.push(`Gateway URL không hợp lệ: ${url}`);
    }
  }

  global.apiKey = String(raw.apiKey || "").trim();
  if (!global.apiKey) errors.push("Chưa nhập API key");
  return { global, errors };
}
