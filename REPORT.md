# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Đăng Thực |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202603014` |
| GitHub | `ThucNguyen1705` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04871` |

## Kết quả cuối

> Cách tính trip: lệnh trong GUIDE `int(MSSV) % 26` báo lỗi vì MSSV `2A202603014` có chữ `A`, nên tôi giữ toàn bộ chữ số: `int("2202603014") % 26 = 14` → `list-trips[14]` = **`trip-04871`** (cũng là `map_index=14` của `silver_frames` trong DAG).

- `dataset_version` của bạn: `a7e44e5cd467`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **736 / 144 / 139** (tổng 1019; số trip 18 / 4 / 4)
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

Theo `report/evidence/leakage.json`: có **1019** khung đáng dùng thuộc **26** trip. Chia ngẫu nhiên **theo khung hình** (70/15/15 theo `hash(frame_id)`): **26/26 trip** nằm ở nhiều hơn một split và **26/26 trip có khung ở cả train lẫn test**, tức là trip nào cũng rò rỉ. Chia **theo trip** (gold của pipeline): **0** trip bị chia cắt. Dbt test `no_straddle` và `verify` ("no trip is in two splits", 26 trips) cũng xác nhận điều này.

Điểm kiểm thử sẽ đẹp giả tạo vì hai khung liền nhau chỉ cách 0,5 s: cùng con đường, cùng ánh sáng, cùng xe phía trước, và nhãn (tốc độ, góc lái) gần như bằng nhau. Bước khử trùng chỉ bỏ khung khác ≤ 2 bit dHash, nên những khung "gần giống" (trung vị 7 bit ở tốc độ cao) vẫn còn. Nếu một khung ở test có hàng xóm ngay bên cạnh nằm trong train, mô hình chỉ cần **nhớ cảnh** là đoán đúng, nên điểm test đo khả năng nhớ chứ không đo khả năng tổng quát hoá. Khi gặp một con đường mới thì độ chính xác tụt. Với split của tôi, test gồm các trip `trip-07774`, `trip-07900` (drive A) và `trip-03365`, `trip-09209` (drive B): mô hình không thấy bất kỳ khung nào của chúng khi train. Tuy vậy, các trip trong cùng một drive vẫn chung xe, chung ngày, chung thời tiết, nên muốn đánh giá chặt hơn thì nên giữ hẳn một drive/ngày/khu vực làm test.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`export_dataset` tính `sha256` của các dòng `frame_id|split` (đã sắp theo `frame_id`) rồi lấy 12 ký tự đầu. Cùng nội dung (cùng tập khung, cùng cách chia) thì luôn ra cùng version, dù chạy lúc nào, trên máy nào, bao nhiêu lần; nội dung khác thì version khác. Vì vậy version vừa là tên vừa là bằng chứng: giảng viên chạy lại từ lake trống phải ra đúng chuỗi đó. Nếu dùng ngày giờ thì mỗi lần chạy lại một dataset y hệt cũng có tên mới (không biết hai lần có giống nhau không); số đếm thì phụ thuộc đã chạy bao nhiêu lần và đụng nhau giữa các máy.

Kết quả của tôi: `step all --reset` lần đầu, `step 4`, rồi `cli export-dataset` lần nữa đều cho **`a7e44e5cd467`** (1019 khung: train 736 / val 144 / test 139; `manifest_sha256` `b31cc33f…`), và `export-dataset` không chép lại ảnh đã có. Version của tôi khác bạn bè vì `gold_dataset_frames` xếp trip trong mỗi drive theo `md5(trip_id || LAB_STUDENT_ID)`: MSSV đổi thì thứ tự đổi, trip rơi vào split khác, dòng `frame_id|split` khác, nên hash khác. Tôi đã thử chạy bước 4 với MSSV giả `2024000` (chỉ truyền `-e` cho một lệnh, không sửa `.env`): ra **`8fc131884584`**, train/val/test = **703/182/134**. Tập 1019 khung giữ nguyên (lọc chất lượng không phụ thuộc MSSV), số trip mỗi split vẫn 18/4/4 (mỗi drive 13 trip → 9/2/2), chỉ có việc trip nào vào split nào là đổi. Chạy lại bước 4 với `2A202603014` thì về đúng `a7e44e5cd467` (ảnh `report/screenshots/minio_gold_versions.png` thấy cả hai thư mục version). Một hạn chế: version chỉ băm `frame_id|split`, nên nếu nhãn (tốc độ đã join) đổi mà tập khung và split giữ nguyên thì version **không đổi**. Phần đó do `manifest_sha256` (băm toàn bộ `manifest.parquet`) phát hiện.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/a7e44e5cd467/manifest.json` ở bước 4 (bản lưu: `report/outputs/04_manifest_a7e44e5cd467.json`):

```json
"lineage": {"bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
            "silver.frames": 27, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
            "input_video_sha256": {"drive_a": "636ba1a98dfc…5795957d5e7e9aa", "drive_b": "bfa62703f645…c8e2af265bbee9fdc"}},
"rules": {"drop": ["is_black","is_blurry","is_near_dup","not sync_ok"], "split": "per trip, stratified by drive, md5 order"}
```

Tức là: dataset `a7e44e5cd467` được tạo từ **hai video gốc** có đúng hai sha256 trên, telemetry bronze A/B và bảng trip ở **version 0** (nạp đúng một lần), `bronze.video_files` version 1 (2 commit, mỗi drive một), và `silver.frames` **version 27**. Con số 27 là 26 partition trip (v0–v25) cộng hai lần tôi chạy lại `trip-04871` trong thí nghiệm idempotency (v26, v27). Version vẫn là `a7e44e5cd467` dù `silver.frames` đã lên v27, đúng như mong đợi: lịch sử bảng dài hơn nhưng nội dung không đổi. Ở lượt chạy cuối từ lake trống, `silver.frames` là v25 (xem `run_summary.json`/manifest cuối) mà version vẫn giữ nguyên.

Ba tháng sau, để tái tạo đúng dataset này cần: (1) hai file video khớp sha256 (`bronze/video/<drive>/raw.mp4`); (2) đọc các bảng Delta **as of** đúng các version trong `lineage` (time travel, với điều kiện chưa `VACUUM` các file cũ); (3) luật lọc/chia trong `rules` và `manifest_sha256` để so lại; (4) ảnh đã chép sẵn trong `gold/datasets/a7e44e5cd467/{train,val,test}/` cùng `manifest.parquet` (mỗi khung có `image_path`, `split`, nhãn). Manifest **chưa** ghi lại ba thứ cần thiết: giá trị ngưỡng trong `dbt_project.yml` (`dup_hamming=2`, `sync_tol_ms=150`, `split_train=0.70`…), commit git của mã pipeline/dbt, và muối `LAB_STUDENT_ID` (phải tra trong REPORT này: `2A202603014`). Tôi sẽ bổ sung ba trường đó nếu được sửa pipeline.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Đặt `LAB_FAIL_ONCE=trip-04871` trong `.env`, `docker compose --profile orchestrate up -d`, trigger `day09_reset` (xanh) rồi `day09_video_pipeline` (run `pipeline_crash`). Kết quả:
- **1 task đỏ**: `silver_frames` `map_index=14`, log có `RuntimeError: simulated crash while extracting trip-04871; run it again` (`report/screenshots/fail_task_log.png`, `airflow_fail_graph.png`). Lỗi xảy ra **trước khi ghi** Delta, nên bảng không bị ghi dở.
- **25 task `silver_frames` khác vẫn xanh** (mapped task độc lập, `airflow_fail_mapped_tasks.png`); **7 task `upstream_failed`**: `dbt_run_silver, dbt_test_silver, dbt_run_gold, dbt_test_gold, publish_lake, export_dataset, publish_warehouse`. Dữ liệu dở dang không đi tới gold.
- Tôi chỉ **Clear task đỏ** trong UI (hộp thoại *Clear and Retry*, Map Index 14, mặc định Downstream + Recursive): **Affected Tasks: 8** = `silver_frames[14]` + 7 task phía sau vốn chưa chạy (`airflow_clear_failed_task.png`). Chỉ **1** task trích frame chạy lại (`attempt=2.log`, in `{"trip": "trip-04871", "frames": 60}`); 25 trip còn lại và 3 task bronze **không** chạy lại (chỉ có `attempt=1`). DAG chuyển xanh (`airflow_retry_graph.png`, `airflow_retry_mapped_tasks.png`).
- **Không nhân đôi**: `silver.frames` 1246 dòng = 1246 `frame_id` khác nhau, trip-04871 có đúng 60 khung, gold 1019 khung, 1 version duy nhất. `scripts/lab.sh verify` → **27/27** (`report/verify.txt`), `dataset_version` **vẫn là `a7e44e5cd467`**, giống trước khi mô phỏng lỗi. Lý do: task trích frame ghi Delta `overwrite` theo `trip_id` (Câu 4), nên chạy lại một partition thì thay đúng partition đó. Xong bài tập, tôi đã xoá `LAB_FAIL_ONCE` khỏi `.env` và áp dụng lại Compose.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Với 500 xe/ngày (thay vì 2 drive × 5 phút), ba chỗ vỡ đầu tiên:
1. **Trích frame trong một container Airflow (LocalExecutor, `PARALLELISM=2`, `max_active_tis_per_dagrun=1`)**: ở đây 26 trip mất ~3–6 phút (bước 3: 142 s), tức khoảng 5–10 s cho mỗi trip 30 s video. 500 xe × vài giờ video là hàng chục nghìn trip/ngày, không xong trong ngày. Decode video là nút cổ chai (slide 22–23). Cần thay bằng worker phân tán (Celery/KubernetesExecutor hoặc Spark/Ray), decode bằng GPU (NVDEC), và trích frame ngay khi file tới (event-driven, kiểu Kafka/queue) thay vì một DAG batch.
2. **DuckDB một file + dbt chạy trên một máy** (`work/lakehouse.duckdb`), mỗi lần `publish_lake` lại `overwrite` toàn bộ bảng silver/gold: dữ liệu tăng tuyến tính thì RAM/IO một máy và việc ghi đè toàn bảng không theo kịp. Cần engine phân tán đọc Delta trực tiếp (Spark/Trino/Databricks), model dbt **incremental** theo partition ngày/xe, và `MERGE` thay vì overwrite toàn bảng (slide 12, 22).
3. **Lưu trữ và chép ảnh**: một MinIO một node, mỗi khung là một JPG nhỏ, và mỗi `dataset_version` **chép lại** toàn bộ ảnh (`copy_object` từng ảnh). Hàng tỉ object nhỏ sẽ làm chậm list/copy và tốn dung lượng gấp nhiều lần. Cần object storage phân tán (S3), đóng gói frame thành shard (WebDataset/TFRecord/Parquet), và để manifest **trỏ** tới ảnh bất biến thay vì chép. Ngoài ra Postgres/Metabase cũng nên chuyển sang warehouse cột (BigQuery/ClickHouse) cho mart.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu SQL tôi thêm vào dashboard (card *"Tỷ lệ khung bị loại theo trip (câu hỏi riêng)"*), chạy trên warehouse Postgres:

```sql
select trip_id, drive_id, split, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_bi_loai
from marts.mart_trip_summary
order by pct_bi_loai desc, trip_id;
```

Kết quả (8 dòng đầu): `trip-04622` (drive_a, train) bị loại **56.7%** (26/60 khung giữ lại), `trip-03904` 50.0%, `trip-02086` 46.7%, `trip-08888` 40.0%, `trip-07774` (test) 38.2%, `trip-06570` 28.9%, `trip-04871` 28.3%, `trip-08448` 25.0%. Năm trip loại nhiều nhất đều ở **drive A**. Nguyên nhân chính là khung trùng: trip-04622 có camera đứng hình 6 s (`drive_a-02406…02466`) cộng đoạn đường cao tốc ít chi tiết; trip-08888 xe đi 6.5 km/h; trip-02086 17.6 km/h. trip-06570 bị loại vì 9 khung mờ. Gộp theo split: train bị loại 20.6% (927 → 736), val 8.9%, test 13.7%. Như vậy bước làm sạch lấy đi nhiều khung của train hơn và của các trip chạy chậm. Nếu muốn dataset cân bằng theo cảnh, nên theo dõi chỉ số này mỗi lần chạy, và nên lấy mẫu theo quãng đường thay vì theo thời gian (xem Câu 5).

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất**: Git trên Windows (`core.autocrlf=true`) đổi file sang CRLF, làm `airflow-entrypoint.sh` hỏng (`exec ... no such file or directory`) và làm sai hash của các file được bảo vệ. Phải đặt `core.autocrlf=false` cho repo rồi đưa file về LF. Ngoài ra, MSSV có chữ nên công thức `int(MSSV) % 26` của GUIDE không chạy (tôi giữ phần chữ số, xem đầu báo cáo).
- **Bất ngờ về dữ liệu**: khung đầu tiên của mỗi drive (`drive_a-00000`, `drive_b-00000`) bị `sync_lost` dù telemetry chỉ đến sau 14 ms, vì ASOF join chỉ tìm dòng **trước** khung. Chọn dòng gần nhất cả hai phía sẽ giữ được khung này.
- **Chưa rõ**: vì sao trip-04622 chạy 65 km/h mà vẫn có 34/60 khung "gần trùng". Với dHash 9×8, đoạn cao tốc nhiều trời và mặt đường có thể trông giống nhau dù xe đang chạy thật.
- **Lỗi pipeline chưa bắt**: `is_near_dup` so với khung **liền trước** chứ không so với khung **được giữ gần nhất**, nên khi cảnh trôi chậm, cả chuỗi bị loại dần dần. Pipeline cũng không kiểm tra telemetry nhảy vọt nhưng vẫn trong 0..70 m/s (ví dụ GPS nhảy vài trăm mét, tốc độ đổi 30 m/s trong 0,1 s). Luật "đen" chỉ dựa vào độ sáng < 12, nên một cảnh đêm thật cũng sẽ bị loại.
- Lượt chạy cuối của tôi dùng `run_summary.json` từ `step all --reset` (27/27, `a7e44e5cd467`); `report/verify.txt` lưu sau bài tập sập/chạy lại.
