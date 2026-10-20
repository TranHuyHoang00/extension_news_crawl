// Content script: chạy trong tab của site, gọi fetch cùng origin nên mang theo cookie của trang.
// Dùng chung cho mọi site: trang đầu của từng cate → lọc link bài bằng regex → lấy nguyên HTML từng bài.
// Background tiêm file này mỗi lần chạy; cờ bên dưới chống đăng ký listener 2 lần.
(() => {
  if (window.__newsCrawlerLoaded) return;
  window.__newsCrawlerLoaded = true;

  let abortFlag = false;

  // ===================== Cấu hình chống chặn =====================
  const BLOCK_RETRY_MS = 20000; // bị chặn: chờ 20 giây rồi thử lại tại chỗ
  const RETRIES_BEFORE_RELOAD = 2; // thử lại tại chỗ 2 lần, vẫn bị → báo background tự F5 trang
  const BEAT_MS = 30000; // trong lúc chờ lâu, cứ 30 giây báo background "vẫn còn sống"

  // Tham số theo dõi quảng cáo, bỏ khỏi link để cùng một bài luôn ra cùng một URL
  const TRACKING_PARAM = /^(utm_\w+|fbclid|gclid|dclid|msclkid|igshid|mc_cid|mc_eid|_ga|_gl)$/i;

  const send = (msg) => chrome.runtime.sendMessage(msg);
  const beat = () => send({ type: "CR_BEAT" }).catch(() => {});
  const log = (text) => send({ type: "CR_LOG", text }).catch(() => {});

  class Aborted extends Error {}
  class Fatal extends Error {} // lỗi khiến cả lượt chạy phải dừng (gateway lỗi, hàng chờ đầy)
  class Blocked extends Error {} // bị chặn / mất mạng sau khi đã thử lại → F5 rồi tự chạy tiếp
  const checkAbort = () => {
    if (abortFlag) throw new Aborted("Đã dừng");
  };

  // Chờ theo từng nhịp ngắn: bấm Dừng là thoát ngay, và gửi nhịp tim cho watchdog
  async function sleep(ms) {
    const end = Date.now() + ms;
    let lastBeat = Date.now();
    while (Date.now() < end) {
      checkAbort();
      await new Promise((r) => setTimeout(r, Math.min(1000, end - Date.now())));
      if (Date.now() - lastBeat >= BEAT_MS) {
        beat();
        lastBeat = Date.now();
      }
    }
  }
  const randomDelay = (cfg) =>
    sleep((cfg.delayMin + Math.random() * (cfg.delayMax - cfg.delayMin)) * 1000);

  // Chuẩn hoá link: bỏ #hash và tham số theo dõi (utm_*, fbclid...), giữ các tham số khác (vd ?id=123)
  function normalizeUrl(href, base) {
    try {
      const u = new URL(href, base);
      if (!/^https?:$/.test(u.protocol)) return null;
      u.hash = "";
      for (const k of [...u.searchParams.keys()]) if (TRACKING_PARAM.test(k)) u.searchParams.delete(k);
      return u.href;
    } catch {
      return null;
    }
  }

  // Link bài trên trang: cùng domain với tab (fetch khác domain bị trình duyệt chặn), khớp regex,
  // bỏ trùng, giữ thứ tự xuất hiện
  function extractLinks(doc, base, re) {
    const out = [];
    const seen = new Set();
    for (const a of doc.querySelectorAll("a[href]")) {
      const url = normalizeUrl(a.getAttribute("href"), base);
      if (!url || seen.has(url)) continue;
      seen.add(url);
      if (new URL(url).origin !== location.origin || !re.test(url)) continue;
      out.push(url);
    }
    return out;
  }

  // Trang trả về là trang kiểm tra của Cloudflare (chưa giải)?
  function isChallengeDoc(doc) {
    const title = doc.title || "";
    if (/just a moment|attention required|checking your browser/i.test(title)) return true;
    return !!doc.querySelector(
      "#challenge-form, #challenge-running, #cf-challenge-running, .cf-browser-verification",
    );
  }

  // Tải một trang. Bị chặn / giới hạn tạm thời (403, 407, 408, 429, 5xx, trang Cloudflare, lỗi mạng):
  //   chờ 20 giây thử lại tại chỗ, tối đa 2 lần; vẫn bị → Blocked (background sẽ F5 rồi tự chạy tiếp).
  // Lỗi khác (ví dụ 404) → Error thường: bỏ qua bài / trang đó.
  async function fetchPage(url) {
    for (let attempt = 0; ; attempt++) {
      checkAbort();
      let reason;
      try {
        const res = await fetch(url, { credentials: "include" });
        if (res.ok) {
          const html = await res.text();
          const doc = new DOMParser().parseFromString(html, "text/html");
          if (!isChallengeDoc(doc)) return { html, doc, finalUrl: res.url };
          reason = "trang kiểm tra Cloudflare";
        } else if ([403, 407, 408, 429].includes(res.status) || res.status >= 500) {
          reason = `HTTP ${res.status}`;
        } else {
          throw new Error(`HTTP ${res.status} khi tải ${url}`);
        }
      } catch (e) {
        if (e instanceof Aborted || e.message.startsWith("HTTP ")) throw e;
        reason = `lỗi mạng: ${e.message}`;
      }
      if (attempt >= RETRIES_BEFORE_RELOAD)
        throw new Blocked(`Bị chặn sau ${RETRIES_BEFORE_RELOAD + 1} lần thử (${reason})`);
      log(`Bị chặn (${reason}), thử lại sau ${BLOCK_RETRY_MS / 1000}s: ${url}`);
      await sleep(BLOCK_RETRY_MS);
    }
  }

  // Hỏi background: link nào không cần crawl (đã gửi Kafka, đang chờ gửi, hoặc lượt chạy khác đang giữ chỗ)
  async function cachedOf(urls, owner = null) {
    if (!urls.length) return new Set();
    const r = await send({ type: "CACHE_FILTER", urls, owner });
    if (!r?.ok) throw new Error(r?.error || "Không đọc được cache");
    return new Set(r.seen);
  }

  // Bản ghi gửi Kafka: domain, url, nguyên văn HTML server trả về
  const buildRecord = (url, html) => ({ domain: new URL(url).hostname, url, html });

  // Xử lý một bài: giữ chỗ → tải → đưa vào hàng chờ gửi Kafka (background gửi khi đủ batch)
  async function processArticle(url, cfg, runId, stats) {
    const c = await send({ type: "CLAIM", url, owner: runId });
    if (c?.stale) throw new Fatal("Lượt chạy này đã được thay bằng lượt mới, dừng");
    if (!c?.granted) {
      stats.skipped++;
      log(`Bỏ qua (đã gửi / đang chờ gửi / tab khác đang xử lý): ${url}`);
      return;
    }
    let r;
    try {
      await randomDelay(cfg); // trong try: bấm Dừng lúc đang chờ thì vẫn trả lại chỗ đã giữ
      const { html } = await fetchPage(url);
      r = await send({ type: "CR_RECORD", runId, record: buildRecord(url, html) });
    } catch (e) {
      await send({ type: "RELEASE", url, owner: runId }).catch(() => {});
      throw e;
    }
    if (!r) throw new Error("Background không phản hồi");
    if (!r.ok) {
      if (r.fatal) throw new Fatal(r.error);
      throw new Error(r.error || "Background không nhận bài");
    }
    stats.queued++;
    if (r.gatewayError) {
      throw new Fatal(
        `Gateway lỗi: ${r.gatewayError}. Dừng lượt chạy, bài đã crawl nằm trong hàng chờ và sẽ gửi lại lượt sau`,
      );
    }
  }

  // Lỗi khiến cả lượt phải dừng (không bỏ qua để làm tiếp bài/cate khác)
  const isStopError = (e) => e instanceof Aborted || e instanceof Fatal || e instanceof Blocked;

  // Một lượt chạy: lần lượt từng cate (trống thì link home) → trang đầu → các bài mới
  async function run({ runId, config: cfg }) {
    const stats = { found: 0, queued: 0, skipped: 0, errors: 0 };
    let err = null;
    try {
      const re = new RegExp(cfg.regex);
      const done = new Set(); // bài nằm ở nhiều cate chỉ xử lý một lần trong lượt này

      for (const cate of cfg.sources) {
        checkAbort();
        let fresh;
        try {
          await randomDelay(cfg);
          const { doc } = await fetchPage(cate);
          const links = extractLinks(doc, cate, re);
          const cached = await cachedOf(links, runId);
          fresh = links.filter((u) => !cached.has(u) && !done.has(u));
          log(`${cate}: ${links.length} link bài, ${fresh.length} mới`);
        } catch (e) {
          if (isStopError(e)) throw e;
          stats.errors++;
          log(`Lỗi khi đọc ${cate}: ${e.message}`);
          continue;
        }
        stats.found += fresh.length;

        for (const url of fresh) {
          checkAbort();
          done.add(url);
          try {
            await processArticle(url, cfg, runId, stats);
          } catch (e) {
            if (isStopError(e)) throw e;
            stats.errors++;
            log(`Lỗi bài ${url}: ${e.message}`);
          }
        }
      }
    } catch (e) {
      err = e;
    }
    await send({
      type: "CR_DONE",
      runId,
      stats,
      error: !err ? null : err instanceof Aborted ? "Đã dừng theo yêu cầu" : err.message,
      gatewayDown: err instanceof Fatal,
      blocked: err instanceof Blocked,
    }).catch(() => {});
  }

  // Test: không gửi Kafka, không ghi cache. Lấy link ở trang đầu của cate đầu tiên (trống thì link home) + tải thử 1 bài.
  async function test(cfg) {
    const re = new RegExp(cfg.regex);
    const cate = cfg.sources[0];
    const { doc } = await fetchPage(cate);
    const links = extractLinks(doc, cate, re);
    const result = {
      source: cate,
      total: links.length,
      alreadySeen: (await cachedOf(links)).size,
      links: links.slice(0, 100),
      sample: null,
    };
    if (links.length) {
      await randomDelay(cfg);
      const { html, doc: d } = await fetchPage(links[0]);
      const rec = buildRecord(links[0], html);
      result.sample = {
        domain: rec.domain,
        url: rec.url,
        // chỉ để xem bài có đúng không, không gửi đi
        title:
          d.title ||
          d.querySelector('meta[property="og:title"]')?.content ||
          d.querySelector("h1")?.textContent.replace(/\s+/g, " ").trim() ||
          null,
        html_length: html.length,
        html: html.slice(0, 300) + "…",
      };
    }
    return result;
  }

  chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg.type === "CR_RUN") {
      abortFlag = false;
      run(msg);
      sendResponse({ ok: true });
      return false;
    }
    if (msg.type === "CR_TEST") {
      abortFlag = false;
      test(msg.config).then(
        (r) => sendResponse({ ok: true, result: r }),
        (e) => sendResponse({ ok: false, error: e.message }),
      );
      return true;
    }
    if (msg.type === "CR_ABORT") {
      abortFlag = true;
      sendResponse({ ok: true });
      return false;
    }
    return false;
  });
})();
