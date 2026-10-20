# News Craw

Extension Chrome (Manifest V3) crawl bài viết từ **trang đầu** của các chuyên mục (cate) trên các trang tin tức, lấy **nguyên văn HTML** từng bài và gửi lên Kafka qua gateway theo batch.

- Dùng chung cho mọi site, không cần viết code riêng: chỉ cần nhập link cate và regex lọc link bài.
- Mỗi tab chạy một site với cấu hình riêng. Muốn crawl nhiều site thì mở nhiều tab.
- Chỉ lấy trang đầu của cate: không bấm Load more, không phân trang.

## Cài đặt

1. Mở `chrome://extensions`, bật **Developer mode**.
2. Bấm **Load unpacked**, chọn thư mục `extension_news_crawl`.
3. Cập nhật code: chép đè file rồi bấm **Reload** (mũi tên tròn) ở extension.

## Dùng

1. Mở một tab vào site cần crawl (tab này dành riêng cho crawler, đừng dùng để lướt web).
2. Bấm icon extension. Side panel **riêng của tab này** mở ở bên phải (cần Chrome 116+). Nhập:
   - **Link home** (không bắt buộc): trước mỗi lượt, tab đang ở domain khác thì được đưa về link này. Để trống thì dùng trang chủ của domain link cate (ví dụ `https://www.timesofisrael.com/`).
   - **Link cate**: mỗi dòng một link chuyên mục, **cùng site với link home**. Để trống thì quét link home. Ô bên dưới hiện site đang dùng.
   - **Regex lọc link bài viết**: bắt buộc. Chỉ link khớp regex mới được crawl. Ví dụ:
     - `^https://www\.timesofisrael\.com/(?:[a-z0-9]+-){4,}[a-z0-9]+/$`
     - `^https://www\.nbr\.co\.nz/(?!tag/|tags/|author/|authors/|topic/|topics/|category/|page/)[a-z0-9-]+/[a-z0-9]+(?:-[a-z0-9]+){3,}/$`
   - **Lịch chạy (cron)**: ví dụ `*/30 * * * *`. Ô bên dưới hiện 3 lần chạy tiếp theo.
   - **Delay ngẫu nhiên**: chờ ngẫu nhiên trong khoảng này trước mỗi request tới site.
   - **Giữ cache (ngày)**: link đã gửi được nhớ bao lâu để không gửi lại (mặc định 30).
   - **Kafka**: **Gateway URL**, **API key** dùng chung cho mọi tab; **Topic**, **Batch size** riêng từng tab.
3. Bấm **Test crawl**: xem regex lấy được những link nào ở trang đầu cate thứ nhất, và tải thử một bài. Không gửi Kafka, không ghi cache. Lần đầu Chrome hỏi cấp quyền truy cập site, chọn **Allow**.
4. Bấm **Lưu & bật lịch**. Chrome hỏi cấp quyền truy cập site và gateway (nếu chưa cấp), chọn **Allow**. Muốn chạy ngay thì bấm **Chạy ngay**.

## Luồng chạy

1. Đầu lượt: dọn cache quá hạn, gửi nốt **hàng chờ** còn lại từ lượt trước. Gateway lỗi thì dừng, không crawl thêm.
2. Đưa tab về link home nếu tab đang ở domain khác.
3. Với từng link cate (trống thì link home): tải **trang đầu** → lấy các link **cùng domain** khớp regex → bỏ link đã gửi (cache).
4. Với từng bài mới: **giữ chỗ** → tải → lấy nguyên văn HTML → đưa vào **hàng chờ gửi**.
5. Hàng chờ đủ **batch size** hoặc khoảng **4 MB dữ liệu** thì gửi một request lên gateway. Hết lượt thì gửi nốt phần còn lại.
6. Batch gửi thành công thì các bài được xoá khỏi hàng chờ và ghi vào **cache**.

Mọi request tới site chạy bằng `fetch` từ trong tab của site nên mang cookie của trang (qua được Cloudflare nếu tab đã qua).

**Chuẩn hoá link** trước khi so cache: bỏ phần `#...` và các tham số theo dõi (`utm_*`, `fbclid`, `gclid`...), **giữ** các tham số khác (ví dụ `?id=123`).

## Bị chặn, mất mạng: tự F5 và tự chạy tiếp

1. Tải trang / bài bị **403, 407, 408, 429, 5xx, trang kiểm tra Cloudflare, lỗi mạng** → chờ 20 giây thử lại tại chỗ, tối đa 2 lần.
2. Vẫn bị → **tự F5 tab**. Trang tải xong, chờ 5 giây: còn ở trang kiểm tra Cloudflare thì đợi giải xong; không thì **tự chạy lại lượt** (bài đã gửi được cache bỏ qua).
3. **Watchdog** (mỗi phút): tab đang chạy mà **3 phút** không có tín hiệu → tự F5.
4. Tự F5 **10 lần liên tiếp** mà không lấy thêm được bài nào → dừng hẳn, ghi "Lỗi gần nhất". Lượt lịch sau sẽ thử lại.
5. Gateway lỗi → dừng (F5 không giúp gì), bài đã crawl nằm trong hàng chờ.

## Request gửi lên gateway

```
POST {gatewayUrl}?apikey={apiKey}
Content-Type: application/json

{ "topic": "<topic>", "message": "<JSON.stringify(mảng bài)>" }
```

Mỗi phần tử trong `message`:

```json
{
  "domain": "www.timesofisrael.com",
  "url": "https://www.timesofisrael.com/...",
  "html": "<!DOCTYPE html>... (nguyên văn server trả về)"
}
```

- HTTP 2xx là thành công.
- Lỗi mạng, 5xx, 408, 429: thử lại tối đa 3 lần (chờ 2 giây, rồi 5 giây). Lỗi 4xx khác (sai key, sai topic) không thử lại.
- Gửi thất bại: bài vẫn nằm trong hàng chờ, lượt sau gửi lại mà không cần crawl lại.
- HTML nguyên văn khá nặng (thường 100–200 KB/bài). Batch size × dung lượng một bài phải nhỏ hơn giới hạn body của gateway, nếu không gateway trả lỗi 413.

## Chống trùng

Dữ liệu nằm trong **IndexedDB** của extension (database `news_crawler`), dùng chung cho mọi site (mỗi link ghi kèm host). Mỗi link ở một trong ba trạng thái:

| Bảng     | Ý nghĩa |
| -------- | ------- |
| `claims` | Đang được một lượt chạy xử lý (giữ chỗ 10 phút). Tab khác thấy thì bỏ qua |
| `outbox` | Đã crawl, đang chờ gửi Kafka. Tối đa 300 bài (tính chung mọi site); đầy thì lượt chạy dừng. Quá 7 ngày chưa gửi được thì bỏ |
| `seen`   | Đã gửi Kafka thành công (cache). Giữ theo số ngày cài đặt |

- Link đã nằm ở một trong ba bảng thì không crawl.
- Nút **xoá cache** chỉ xoá cache của site đang cấu hình ở tab. Nút **gửi ngay** gửi hàng chờ mà không đợi lượt sau.
- Xem dữ liệu: `chrome://extensions` → bấm **service worker** của extension → DevTools → tab **Application** → **IndexedDB** → `news_crawler`.
- Cache nằm trong từng Chrome profile. Hai profile không biết bài của nhau, nên đừng cho hai profile quét cùng một cate. Backend vẫn nên chống trùng theo `url`.

## Giới hạn

- Mỗi tab một site: link home và các link cate phải cùng domain. Link bài khác domain (kể cả subdomain khác) bị bỏ qua vì trình duyệt chặn tải khác domain.
- Site tự chuyển hướng (ví dụ `example.com` → `www.example.com`): nhập link home / link cate theo địa chỉ sau khi chuyển hướng.
- Cấu hình gắn theo **tab id**. Đóng tab hoặc khởi động lại Chrome thì cần cấu hình lại. Cấu hình gateway và cache vẫn giữ nguyên.
- Lịch cron tính theo giờ máy tính; Chrome có thể trễ tối đa ~30 giây và không chạy khi máy ngủ.
- Đổi tên hoặc chuyển thư mục extension thì Chrome coi là extension mới (ID khác), mất cache và hàng chờ.

## Cấu trúc thư mục

| File | Việc |
| --- | --- |
| `manifest.json` | Khai báo extension |
| `background.js` | Lịch, khởi động lượt chạy, tự F5, gửi Kafka |
| `content.js` | Chạy trong tab: tải trang cate, lọc link, tải bài, chống chặn |
| `sidepanel.html`, `sidepanel.js`, `sidepanel.css` | Giao diện cấu hình |
| `lib/config.js` | Kiểm tra cấu hình |
| `lib/cache.js` | IndexedDB: cache, hàng chờ, giữ chỗ |
| `lib/cron.js` | Đọc biểu thức cron |
