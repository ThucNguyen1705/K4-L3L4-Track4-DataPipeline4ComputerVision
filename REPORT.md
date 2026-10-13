# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Đăng Thực |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202603014` |
| GitHub | `ThucNguyen1705` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Từ hai dòng mẫu `step 1` in ra (đầu ra đầy đủ: `report/outputs/01_bronze.txt`):

| | Nguồn A (`bronze.raw_telemetry_a`, 3021 dòng) | Nguồn B (`bronze.raw_telemetry_b`, 3234 dòng) |
|---|---|---|
| Tên cột | `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer` | `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering` |
| Đơn vị tốc độ | m/s (`1.213`) | km/h (`55.11`) |
| Kiểu timestamp | chuỗi ISO UTC `2025-10-07T08:00:00.014Z` | epoch mili-giây `1759933801509` |
| Bàn đạp | tỉ lệ 0..1 (`gas 0.028`) | phần trăm (`throttle_pct 2.0`) |
| Đồng hồ | `telemetry_clock_offset_ms = 0` | `= 1500`: mẫu đầu `1759933801509` = 14:30:01.509, trong khi video B bắt đầu 14:30:00 |

Ở bronze **mọi cột là VARCHAR**, kể cả 30 ô tốc độ trống ở mỗi nguồn (`speed_mps = ''` ở A, `spd_kmh = ''` ở B). Giữ mỗi nguồn một bảng và để dạng chữ vì bronze là **bản sao trung thực, chỉ thêm**: nếu ép kiểu/gộp ngay lúc nạp thì phải chọn sẵn một schema, một ô hỏng (`''`, chữ lạ) có thể làm hỏng cả lô hoặc bị ép thành 0, km/h có thể bị hiểu nhầm là m/s, và khi đó không còn bản gốc để chạy lại. Để ở dạng chữ thì lượt nạp không bao giờ mất dòng; quyết định kiểu, đơn vị, trừ độ lệch đồng hồ nằm ở silver (dbt), là mã có test và có thể chạy lại từ bronze khi luật thay đổi. Các cột `_source_file`, `_batch_id` (12 ký tự đầu sha256), `_row_no` giữ lineage tới tận dòng CSV. Nạp lặp không nhân đôi: lần hai báo `already present`, `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, WRITE, 3021 dòng), `bronze.video_files` có 2 dòng (mỗi drive một, nhận diện bằng sha256).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Hai view staging đổi cả hai nguồn về một schema (m/s, độ, `timestamptz` UTC): `stg_telemetry_a` chỉ `cast(ts_utc as timestamptz)` và `try_cast(nullif(speed_mps,''))`; `stg_telemetry_b` đổi tên cột (`hdg→heading_deg`, `latitude→lat`, `ax→acc_x`…), `spd_kmh / 3.6` → m/s, `throttle_pct/100` → 0..1, và tính thời gian bằng `to_timestamp((timestamp_ms − offset_ms) / 1000.0)`, trong đó `offset_ms = 1500` lấy từ `bronze.video_files` (khai báo trong `sources.json`, không đoán). Sau đó `silver_telemetry_classified` gộp bằng `union all by name` và gắn `reject_reason` cho từng dòng.

Bằng chứng trong lượt chạy của tôi (`report/outputs/02_silver_telemetry.txt`): `bat_dau` của B là **2025-10-08 14:30:00.009** (giá trị thô 1759933801509 = 14:30:01.509, trừ 1,5 s) khớp `video_start_utc` 14:30:00; A bắt đầu 08:00:00.014 khớp video A 08:00:00. Tốc độ tối đa sau chuẩn hóa: A **71 km/h**, B **100 km/h** (hợp lý; nếu quên chia 3,6 thì B sẽ là 100 "m/s" và bị loại hàng loạt). `verify --module 2` đạt 7/7.

| drive | duplicate | speed_missing | speed_out_of_range | sạch | sạch + cách ly = nhận |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 | 2968 + 53 = 3021 |
| drive_b | 20 | 30 | 3 | 3181 | 3181 + 53 = 3234 |

Ba dòng `speed_out_of_range` mỗi drive đều là **250 m/s (900 km/h)** ở `_row_no` 400, 1700, 2500. Dòng lỗi vào `silver_telemetry_rejects` (kèm `reject_reason`, `_batch_id`, `_row_no`) thay vì bị xoá vì: (1) đẳng thức *sạch + cách ly = nhận về* chứng minh không dòng nào biến mất im lặng; (2) đếm theo lý do là tín hiệu cho đội cảm biến (30 ô trống/drive là lỗi thiết bị, không phải lỗi pipeline); (3) nếu một luật sai (ví dụ 250 m/s thật ra là lỗi đơn vị) có thể sửa luật và lấy lại dòng từ rejects/bronze thay vì mất vĩnh viễn.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` đo **độ đầy đủ**: ở bước 2 nó báo `WARN 2`, tức mỗi drive có 1 khoảng mất tín hiệu > 1 s (A: **3073 ms**, B: **3124 ms**). Đó là tình huống thật của cảm biến; các dòng hai bên khoảng hở vẫn đúng, và frame rơi vào khoảng đó tự bị gắn `sync_lost` rồi loại ở gold (`sync_tol_ms = 150`), nên không cần dừng cả pipeline — chỉ cần báo để người vận hành biết. `in_range` (0..70 m/s) đo **tính đúng**: 250 m/s là vật lý không thể, nghĩa là một phép biến đổi/luật làm sạch đã hỏng; nếu để lọt, giá trị sai đi thẳng vào gold (cột `speed_mps` của dataset) và mart, nên phải `error` để chặn.

Thí nghiệm (`report/evidence/break_gate.json`): nới `max_speed_mps` từ 70 lên 1000 → 6 dòng 250 m/s (3/drive) không còn bị cách ly mà lọt vào `silver_telemetry`. `dbt test` chuyển từ `PASS=6 WARN=1 ERROR=0` sang **`PASS=5 WARN=1 ERROR=1`**: test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**; khôi phục luật thì về `PASS=6 WARN=1 ERROR=0`. Điều này cho thấy cổng chất lượng là **lớp phòng thủ độc lập** với luật làm sạch: ngưỡng của test nằm trong `schema.yml`, không dùng chung biến `max_speed_mps`, nên ai nới luật làm sạch (cố ý hay vô tình) vẫn bị test chặn. Trong DAG, `dbt_test_silver` lỗi thì `dbt_run_gold` và các task sau không chạy: dữ liệu xấu không tới gold/warehouse. Còn `WARN` không đổi giữa hai lượt, đúng vai trò "báo nhưng không chặn".

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Tôi chạy `scripts/lab.sh exp idempotency --trip trip-04871` (chính trip dùng cho bài tập sập ở bước 5). Theo `report/evidence/idempotency.json`: trước **1246 dòng**, vân tay `6c7480dfde7e`; sau **hai lần** trích lại trip đó vẫn **1246 dòng**, vân tay `6c7480dfde7e` (`unchanged: true`). Kiểu ghi `append` hai lần cho cùng trip 60 khung ra **120 dòng** (nhân đôi 2x). `cli history silver.frames` cho thấy hai lần chạy lại tạo version **26** và **27**, mỗi version `WRITE` 60 dòng với `predicate: trip_id = 'trip-04871'`: bảng có thêm phiên bản nhưng nội dung cuối không đổi.

`overwrite` với predicate là **một commit nguyên tử** trong `_delta_log`: cùng một giao dịch vừa đánh dấu xoá các file cũ của trip vừa thêm file mới. Người đọc chỉ thấy version cũ hoặc version mới, không bao giờ thấy trạng thái nửa vời. Nếu tiến trình chết giữa chừng (đã ghi nửa file parquet lên MinIO) thì chưa có commit, bảng vẫn ở version trước, file mồ côi bị bỏ qua; chạy lại là xong. Còn "xoá rồi ghi" là **hai** commit: chết sau bước xoá thì trip biến mất khỏi bảng (gold build lúc đó sẽ thiếu trip mà không ai biết), và nếu cơ chế thử lại không biết đã xoá hay chưa thì dễ ghi trùng. Overwrite theo partition cũng chỉ đụng đúng một trip, nên 25 trip còn lại không bị ảnh hưởng và có thể chạy lại riêng từng trip (đúng cách DAG chia 26 task).

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 / 602 | 8 / 644 |
| 2 | 149 / 602 | 53 / 644 |
| 6 | 363 / 602 | 244 / 644 |

**Đen và tối.** Ở bước 3 (`report/outputs/03_silver_frames.txt`): drive A có 6 khung đen, B có 4; khung tối giữ lại: A 4, B **61**. Cả 10 khung đen có độ sáng **0.0** và độ nét **0.0**: không còn thông tin (camera/encoder mất hình), nên bị loại ở gold. 65 khung tối có độ sáng 12.8–39.6 (TB 31.2) nhưng độ nét TB **378.7**, nghĩa là cảnh vẫn có cạnh, có đường, có xe (ảnh `report/screenshots/silver_frames_examples.png`: `drive_b-01405` bóng râm, `drive_b-00997` trip-03365 có 25/60 khung tối). Pipeline chỉ gắn `is_dark` và **giữ**. Tôi đồng ý: đó là điều kiện lái thật (hầm, bóng cây), bỏ đi thì dataset lệch về cảnh sáng và mô hình hỏng đúng ở chỗ khó. Điểm tôi muốn sửa: luật "đen" chỉ dựa vào độ sáng < 12, nên một cảnh đêm thật rất tối cũng có thể bị coi là đen. Nên thêm điều kiện độ nét ≈ 0 (ở dữ liệu này khung đen đều có blur 0.0).

**Ngưỡng trùng.** Tôi giữ ngưỡng **2**. Ngưỡng 0 chỉ bắt khung giống hệt từng bit (B chỉ 8), bỏ sót khung đứng hình có nhiễu nén; ngưỡng 2 bắt được camera đứng hình 6 s ở trip-04622 (`drive_a-02406…02466`, verify 11/12). Ngưỡng 6 loại **363/602 (60%)** khung A và 244/644 (38%) khung B, tức là loại cả những khung khác nhau thật. Ngưỡng phụ thuộc tốc độ: tôi đo thêm theo dải tốc độ, ở **0–10 km/h** có 50.9% khung trùng tại t2 (trung vị chỉ 2 bit khác), trong khi ở **50+ km/h** chỉ 13.7% (trung vị 7 bit). Xe chạy 65 km/h đi được khoảng 9 m giữa hai khung (0,5 s) nên khung khác nhau thật, nhưng t6 vẫn loại 48.7% số khung đó. Xe càng chậm thì càng nhiều khung thừa thật; xe nhanh thì nên dùng ngưỡng thấp hơn, hoặc tốt hơn là lấy mẫu theo **quãng đường** (mỗi N mét) thay vì theo thời gian.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
