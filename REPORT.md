# 📊 BÁO CÁO KẾT QUẢ THỰC HIỆN LAB 18: DATA LAKEHOUSE ARCHITECTURE (REPORT.MD)

Tài liệu này tổng hợp toàn bộ kết quả thực hiện, số liệu đo đạc kỹ thuật, phương pháp và bằng chứng nghiệm thu cho từng Notebook trong bài Lab 18.

---

## 🟢 NOTEBOOK 01: `01_delta_basics.ipynb` — DELTA LAKE BASICS

### 1. Nội dung ban đầu của Notebook
* **Mục tiêu:** Khảo sát các đặc tính nền tảng của định dạng bảng Delta Lake (ACID Transaction Log, Schema Enforcement, Schema Evolution).
* **Công nghệ sử dụng:** `deltalake` (delta-rs 1.x thuần Python viết bằng Rust), `polars`, `duckdb`. Hoạt động offline 100%, không cần Spark/JVM.
* **Bối cảnh lý thuyết:** Minh họa cho slide §2 (Delta Lake Architecture). Lưu trữ dữ liệu tại `_lakehouse/scratch/users_delta`.

---

### 2. Yêu cầu của Notebook (Pass Criteria: 8/8 điểm)
Theo tiêu chí chấm điểm trong `rubric.md`:
* `_delta_log/` chứa ít nhất 2 commits JSON hợp lệ (4 điểm).
* Schema Enforcement chặn thành công bản ghi lỗi kiểu dữ liệu (`age = 'thirty'` vào cột `Int64`) (2 điểm).
* Schema Evolution thêm thành công cột mới (`tier`) khi bật `schema_mode="merge"` (2 điểm).
* DuckDB truy vấn Zero-copy qua Arrow Table phân nhóm đúng 2 nhóm `tier`.

---

### 3. Phương pháp hoàn thành
1. **Khởi tạo & Ghi dữ liệu:** Dùng `polars.DataFrame` tạo 3 bản ghi ban đầu và ghi đè vào `table_path` qua `write_deltalake(..., mode="overwrite")`.
2. **Kiểm tra Transaction Log:** Đọc lịch sử commit từ `DeltaTable(table_path).history()`.
3. **Thử nghiệm Schema Enforcement:** Bắt lỗi ngoại lệ `Cast error` bằng khối `try/except` khi cố tình ghi DataFrame chứa kiểu dữ liệu không khớp.
4. **Thử nghiệm Schema Evolution:** Ghi bản ghi thứ 4 kèm cột `tier="premium"` với cờ `schema_mode="merge"`.
5. **Truy vấn Zero-copy:** Đăng ký bảng Arrow vào DuckDB `con.register("users", ...)` và thực hiện truy vấn tổng hợp SQL.

---

### 4. Lý do chọn phương pháp đó
* **Tuân thủ chuẩn Open Table Format:** Sử dụng thư viện `delta-rs` chuẩn đảm bảo sinh đúng cấu trúc commit JSON tương thích hoàn toàn với hệ sinh thái Databricks / Spark.
* **Hoạt động Offline 100%:** Truy vấn DuckDB qua bảng Arrow trung gian trong RAM giúp không phải tải extension qua mạng, an toàn tuyệt đối trong môi trường không có kết nối Internet.
* **Tính xác thực cao:** Dùng `assert` kiểm tra trực tiếp các file vật lý trên ổ cứng và schema của bảng.

---

### 5. Kết quả hoàn thành (Outputs & Metrics)
* **Bảng dữ liệu sau Schema Evolution:**
  ```text
  shape: (4, 5)
  ┌─────┬─────────┬─────┬────────┬─────────┐
  │ id  ┆ name    ┆ age ┆ city   ┆ tier    │
  │ --- ┆ ---     ┆ --- ┆ ---    ┆ ---     │
  │ i64 ┆ str     ┆ i64 ┆ str    ┆ str     │
  ╞═════╪═════════╪═════╪════════╪═════════╡
  │ 1   ┆ alice   ┆ 30  ┆ Hanoi  ┆ null    │
  │ 2   ┆ bob     ┆ 25  ┆ HCMC   ┆ null    │
  │ 3   ┆ charlie ┆ 35  ┆ Danang ┆ null    │
  │ 4   ┆ dan     ┆ 28  ┆ Hue    ┆ premium │
  └─────┴─────────┴─────┴────────┴─────────┘
  ```
* **Kết quả gom nhóm DuckDB:** `[('premium', 1), (None, 3)]`.
* **Số commit log JSON:** Sinh ra 2 commit files: `00000000000000000000.json`, `00000000000000000001.json`.

---

## 🟢 NOTEBOOK 02: `02_optimize_zorder.ipynb` — SMALL FILES & OPTIMIZE + Z-ORDER

### 1. Nội dung ban đầu của Notebook
* **Mục tiêu:** Tái hiện và giải quyết vấn đề phân mảnh file nhỏ (Small-File Problem) trong streaming ingestion; đo lường hiệu năng của Compaction (`OPTIMIZE`) và sắp xếp đa chiều (`Z-ORDER`).
* **Công nghệ sử dụng:** `deltalake` (`optimize.compact()`, `optimize.z_order()`), `polars`, `duckdb`.
* **Bối cảnh lý thuyết:** Slide §6 (Storage Optimization & Anti-Patterns). Dữ liệu lưu tại `_lakehouse/scratch/events_smallfiles`.

---

### 2. Yêu cầu của Notebook (Pass Criteria: 12/12 điểm)
Theo bảng tiêu chí đánh giá trong `rubric.md`:
* Tái hiện tình trạng phân mảnh file nhỏ: Có ít nhất 100 files (ở đây tạo 200 files) trước khi chạy OPTIMIZE (3 điểm).
* Đo đạc hiệu năng: Đạt **Speedup ≥ 3×** HOẶC **Tỷ lệ tỉa file (Files-pruned ratio) ≥ 10×** (6 điểm).
* Số lượng file giảm rõ rệt sau khi chạy `compact()` (3 điểm).
* Kiểm chứng min/max stats trong transaction log: Chỉ có ~1 file chứa `user_id = 4242`.

---

### 3. Phương pháp hoàn thành
1. **Tạo 200 micro-batches:** Vòng lặp 200 lần ghi dữ liệu nhỏ (mỗi batch 5.000 dòng, 100.000 users khác nhau, payload 200 bytes) mô phỏng luồng streaming liên tục, tạo ra đúng 200 file Parquet rời rạc.
2. **Đo hiệu năng trước tối ưu (Benchmark Before):** Dùng filter pushdown bản địa của `delta-rs` (`filters=[("user_id", "=", 4242), ("kind", "=", "purchase")]`), đo trung vị (median) của 3 lần query quét qua 200 files.
3. **Thực hiện OPTIMIZE + Z-ORDER:**
   * `dt.optimize.compact(target_size=256*1024)`: Gom các file nhỏ lại theo kích thước mục tiêu 256 KB.
   * `dt.optimize.z_order(["user_id"], target_size=256*1024)`: Sắp xếp lại dữ liệu theo đường cong Z-order để các giá trị `user_id` gần nhau nằm cùng một file.
4. **Đo hiệu năng sau tối ưu (Benchmark After):** Chạy lại câu truy vấn tương tự qua 3 lần đo.
5. **Kiểm tra File Stats trong Transaction Log:** Đọc trực tiếp file JSON commit cuối cùng trong `_delta_log/` để trích xuất dải giá trị `[minValues.user_id, maxValues.user_id]` của từng file Parquet.

---

### 4. Lý do chọn phương pháp đó
* **Cơ chế File-skipping thực tế:** Z-order chỉ có ý nghĩa khi bảng có nhiều file và các dải giá trị min/max không bị chồng lấn (non-overlapping). Việc đặt `target_size=256KB` giữ lại ~55 files sau compaction giúp chứng minh rõ ràng khả năng prune file (bỏ qua 54 file, chỉ mở đúng 1 file).
* **Đo lường Deterministic (Không phụ thuộc phần cứng):** Tốc độ CPU/SSD trên laptop có thể dao động, nhưng chỉ số **Files-pruned ratio (Tỷ lệ file được bỏ qua)** là giá trị xác định tuyệt đối dựa trên thống kê metadata.

---

### 5. Kết quả hoàn thành (Outputs & Metrics)
* **Số lượng file trước và sau:** Giảm từ **200 files ➔ 55 files** (Gom gọn gấp 4×).
* **Độ trễ truy vấn điểm (Point Query):**
  * *Trước OPTIMIZE:* **121.4 ms**
  * *Sau OPTIMIZE + Z-ORDER:* **10.8 ms**
* **Tốc độ tăng tốc (Speedup):** **11.2×** (Vượt xa mục tiêu đề bài ≥ 3×).
* **Tỷ lệ tỉa file (Files-pruned ratio):** **55.0×** (Chỉ duy nhất 1 trong 55 files chứa `user_id = 4242` có dải `[3696, 5534]`).

---

### 6. Bằng chứng kiểm chứng (Evidence Log)
```text
  [PASS] compaction reduced file count
  [PASS] speedup ≥ 3x OR pruning ≥ 10x
  [PASS] stats isolate the target user

  (speedup=11.2x, pruning=55.0x — the slide accepts EITHER;
   wall-clock is noisy on a laptop, which is why file-pruning is the fallback.)

NB2 complete.
```
* **File lưu kết quả:** `notebooks/02_optimize_zorder.ipynb`
* **Điểm số đạt:** **12 / 12 điểm** (Part A).

---

## 🟢 NOTEBOOK 03: `03_time_travel.ipynb` — TIME TRAVEL, MERGE & RESTORE

### 1. Nội dung ban đầu của Notebook
* **Mục tiêu:** Khảo sát tính năng Time Travel (truy vấn dữ liệu trong quá khứ theo version), thực hiện phép `MERGE` Upsert quy mô lớn và dùng lệnh `RESTORE` để rollback dữ liệu lỗi.
* **Công nghệ sử dụng:** `deltalake` (`DeltaTable(path, version=N)`, `dt.merge()`, `dt.restore()`), `polars`.
* **Bối cảnh lý thuyết:** Slide §3 (Time Travel & ACID Transactions). Dữ liệu lưu tại `_lakehouse/scratch/customers_tt`.

---

### 2. Yêu cầu của Notebook (Pass Criteria: 12/12 điểm)
Theo bảng tiêu chí đánh giá trong `rubric.md`:
* Lịch sử `history()` ghi nhận ít nhất 5 versions bao gồm cả dòng commit của sự kiện `RESTORE` (4 điểm).
* Thực hiện thành công phép `MERGE` upsert 100K dòng (50K updates, 50K inserts) dưới 60s (4 điểm).
* Lệnh `RESTORE` hoàn tất rollback thành công dưới 30s và số bản ghi lỗi `score < 0` trở về đúng bằng 0 (4 điểm).

---

### 3. Phương pháp hoàn thành
1. **Xây dựng chuỗi Version (v0 ➔ v3):**
   * `v0`: Khởi tạo bảng 100.000 khách hàng ban đầu (`status="active"`, `score=0..999`).
   * `v1`: Mở rộng schema thêm cột `tier` (`gold` nếu score > 800, ngược lại `silver`).
   * `v2`: Thực hiện `MERGE` upsert 100.000 dòng (`customer_id=50.000..149.999`) với `status="vip"`, `tier="platinum"`, `score=999`. Sử dụng `.when_matched_update_all()` và `.when_not_matched_insert_all()`.
   * `v3`: Cố tình ghi đè/append 50 dòng dữ liệu lỗi (`score = -1`, `status = None`, `tier = "UNKNOWN"`).
2. **Truy vấn Time-Travel:** Đọc dữ liệu tại snapshot quá khứ bằng `DeltaTable(table_path, version=0)` (đủ 100K dòng ban đầu) và `version=1` (đủ schema 4 cột).
3. **Thực thi Rollback bằng `RESTORE`:**
   * Gọi `dt.restore(2)` để tua ngược trạng thái bảng về đúng `version 2`.
   * Thao tác `RESTORE` bản chất là một transaction mới được ghi vào log thành `v4 RESTORE`, giúp bảo toàn tính toàn vẹn và audit trail (không xóa lịch sử cũ).
4. **Kiểm tra dữ liệu sau Rollback:** Dùng filter pushdown `filters=[("score", "<", 0)]` xác nhận không còn bất kỳ dòng dữ liệu lỗi nào.

---

### 4. Lý do chọn phương pháp đó
* **Không phụ thuộc vào restore vật lý:** Trong kiến trúc Lakehouse, không cần khôi phục backup từ bản sao lưu bên ngoài; lệnh `RESTORE` của Delta Lake chỉ cập nhật con trỏ metadata về snapshot hợp lệ trước đó, diễn ra gần như tức thì (< 0.1s).
* **Tuân thủ Audit & Compliance:** Khác với việc xóa bỏ log, `RESTORE` ghi nhận một phiên bản mới trong `history()`, giúp kiểm toán viên biết chính xác ai đã khôi phục dữ liệu, vào thời điểm nào và từ version nào.

---

### 5. Kết quả hoàn thành (Outputs & Metrics)
* **Thời gian thực hiện `MERGE` 100K dòng:** **0.08 giây** (Yêu cầu đề bài < 60s).
  * *Metrics chi tiết:* 50.000 dòng updated, 50.000 dòng inserted, tổng số dòng sau merge là 150.000 dòng.
* **Thời gian thực hiện `RESTORE` về v2:** **0.01 giây** (Yêu cầu đề bài < 30s).
* **Số lượng dòng lỗi (`score < 0`) sau restore:** **0 dòng** (Đã loại bỏ sạch sẽ 50 dòng lỗi của v3).
* **Tổng số version trong lịch sử audit:** **5 versions** (v0 WRITE ➔ v1 WRITE ➔ v2 MERGE ➔ v3 WRITE ➔ v4 RESTORE).

---

### 6. Bằng chứng kiểm chứng (Evidence Log)
```text
  [PASS] history ≥ 5 versions
  [PASS] history includes the RESTORE
  [PASS] MERGE recorded in history
  [PASS] bad rows gone after restore

NB3 complete.
```
* **File lưu kết quả:** `notebooks/03_time_travel.ipynb`
* **Điểm số đạt:** **12 / 12 điểm** (Part A).

---

## 🟢 NOTEBOOK 04: `04_medallion.ipynb` — MEDALLION PIPELINE (BRONZE ➔ SILVER ➔ GOLD)

### 1. Nội dung ban đầu của Notebook
* **Mục tiêu:** Xây dựng một luồng dữ liệu chuẩn Medallion Architecture 3 tầng (Bronze ➔ Silver ➔ Gold) hoàn chỉnh cho bài toán **LLM Observability** (theo dõi chi phí, độ trễ và tỷ lệ lỗi của các mô hình ngôn ngữ lớn).
* **Công nghệ sử dụng:** `deltalake`, `duckdb` (SQL Engine phân tích & parse JSON), `polars`.
* **Bối cảnh lý thuyết:** Slide §8 (Lakehouse cho AI/ML: Medallion Pipeline). Dữ liệu được lưu trữ phân tầng tại `_lakehouse/{bronze, silver, gold}/`.

---

### 2. Yêu cầu của Notebook (Pass Criteria: 12/12 điểm)
Theo bảng tiêu chí đánh giá trong `rubric.md`:
* Khởi tạo đầy đủ cả 3 bảng `Bronze`, `Silver`, `Gold` trên tầng lưu trữ vật lý (4 điểm).
* Tầng Silver thực hiện khử trùng lặp (dedup) thành công: Số dòng `Silver < Bronze` (4 điểm).
* Tầng Gold tính toán chính xác các chỉ số (`p50_latency_ms`, `p95_latency_ms`, `cost_usd`, `error_rate`) trải dài qua ít nhất 7 ngày × 3 models (`claude-haiku-4-5`, `claude-sonnet-4-6`, `claude-opus-4-7`) (4 điểm).

---

### 3. Phương pháp hoàn thành
1. **Tầng Bronze (Raw Ingestion):** Tải 200.000 bản ghi thô JSON (`llm_calls_raw`) chứa `request_id`, `ts`, `raw_json` (chứa input/output tokens, latency, status, user_id).
2. **Tầng Silver (Cleaned & Conformed):**
   * Sử dụng DuckDB đọc bảng Delta trung gian qua Apache Arrow (zero-copy, 100% offline).
   * Parse các trường JSON: `json_extract_string`, `json_extract`.
   * Khử trùng lặp (Deduplication): Sử dụng Window Function `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts) AS rn WHERE rn = 1 AND model IS NOT NULL`.
   * Ghi ra Delta Table phân vùng theo ngày: `partition_by=["date"]`.
3. **Tầng Gold (Aggregated Business Metrics):**
   * Định nghĩa ma trận đơn giá tokens: `Haiki (0.8$ in / 4$ out)`, `Sonnet (3$ in / 15$ out)`, `Opus (15$ in / 75$ out) / 1M tokens`.
   * Tính toán các chỉ số thống kê phân vị: `QUANTILE_CONT(latency_ms, 0.50)` cho p50, `QUANTILE_CONT(latency_ms, 0.95)` cho p95.
   * Tính `cost_usd`, `error_rate`, `total_prompt_tokens`, `total_completion_tokens`.
   * Ghi bảng Gold phân vùng theo `date` và chạy `optimize.z_order(["model"])` để tăng tốc dashboard.

---

### 4. Lý do chọn phương pháp đó
* **Xử lý JSON & Window Function hiệu năng cao:** DuckDB cung cấp cú pháp SQL chuẩn (`json_extract`, `QUANTILE_CONT`, `ROW_NUMBER`) thực thi nhanh gấp nhiều lần trên tập dữ liệu hàng trăm nghìn dòng so với parse Python thủ công.
* **Tối ưu Partitioning & Z-Order theo Medallion:** Phân vùng theo `date` ở Silver/Gold giúp query theo thời gian không phải quét toàn bảng; Z-order theo `model` ở Gold giúp các dashboard lọc theo từng dòng mô hình phản hồi tức thì.

---

### 5. Kết quả hoàn thành (Outputs & Metrics)
* **Tầng Bronze:** **200,000 bản ghi thô**.
* **Tầng Silver:** **190,052 bản ghi sạch** (Đã khử thành công **9,948 bản ghi trùng lặp** do cơ chế retry).
* **Tầng Gold:** 
  * Số ngày phân tích: **8 ngày** (2026-04-01 đến 2026-04-08 — vượt mục tiêu ≥ 7 ngày).
  * Số mô hình LLM: **3 mô hình** (`claude-haiku-4-5`, `claude-sonnet-4-6`, `claude-opus-4-7`).
  * Tổng số bản ghi Gold: **24 dòng** (8 ngày × 3 models).
  * Các chỉ số `p50_latency_ms`, `p95_latency_ms`, `error_rate` (~4.9% - 5.9%), và `cost_usd` đều được tính toán đầy đủ và chính xác.

---

### 6. Bằng chứng kiểm chứng (Evidence Log)
```text
Bronze rows: 200,000
Silver rows: 190,052 (Bronze 200,000 → dedup dropped 9,948)

──── Gold deliverable metrics ────
  Distinct dates:     8   (target ≥ 7)
  Distinct models:    3
  Total Gold rows:   24   (= dates × models)
```
* **File lưu kết quả:** `notebooks/04_medallion.ipynb`
* **Điểm số đạt:** **12 / 12 điểm** (Hoàn thành **Part A: 44 / 44 điểm tối đa**).

---

## 🟢 NOTEBOOK 05: `05_iceberg_catalog.ipynb` — APACHE ICEBERG & CATALOG AS CONTROL PLANE

### 1. Nội dung ban đầu của Notebook
* **Mục tiêu:** Khảo sát định dạng bảng Apache Iceberg và kiến trúc **Catalog đóng vai trò Control Plane** (quản lý metadata, scan planning, security boundary); chứng minh tính năng **Hidden Partitioning**, **Metadata Tree 3 tầng**, **Schema Evolution theo field-ID** và **Partition Evolution**.
* **Công nghệ sử dụng:** `pyiceberg` + SQLite Catalog (`SqlCatalog`), `pyarrow`. Hoàn toàn không cần server REST bên ngoài, không cần JVM.
* **Bối cảnh lý thuyết:** Slide §4 (Apache Iceberg) & §12 (Catalog = Control Plane). Dữ liệu lưu tại `_lakehouse/iceberg/nb5`.

---

### 2. Yêu cầu của Notebook (Pass Criteria: 13/13 điểm)
Theo bảng tiêu chí đánh giá trong `rubric.md`:
* Tạo bảng Iceberg thông qua Catalog với partition spec dùng `day(ts)` (3 điểm).
* Đo đạc tính năng **Hidden-partition pruning ≥ 5×** thông qua `plan_files()` khi chỉ lọc trên cột gốc `ts` (5 điểm).
* Duyệt cây metadata 3 tầng và báo cáo tỷ lệ byte metadata:data (1 điểm).
* Đổi tên cột giữ nguyên `field_id` vĩnh viễn (metadata-only) & có ít nhất 2 `spec_id` phân vùng cùng tồn tại mà bảng vẫn đọc trơn tru (4 điểm).

---

### 3. Phương pháp hoàn thành
1. **Khởi tạo bảng qua Catalog:** Dùng `cat.create_table("lake.llm_events", schema=SCHEMA)` thay vì chỉ định đường dẫn thư mục trực tiếp, giúp Catalog quản lý toàn quyền layout và phân quyền.
2. **Cấu hình Hidden Partitioning:** Thêm biến đổi `DayTransform()` trên cột `ts` thành phân vùng `ts_day`. Người dùng chỉ cần truy vấn trên cột `ts` chuẩn.
3. **Ghi 10 Daily Batches:** Nạp 10 ngày dữ liệu mô phỏng (500 dòng/ngày = 5.000 dòng), tạo ra đúng 10 atomic snapshot commits.
4. **Scan Planning & Pruning:** Chạy `tbl.scan().plan_files()` so sánh giữa không lọc (10 files) và lọc `WHERE ts >= '2026-08-05' AND ts < '2026-08-06'` (chỉ quét đúng 1 file).
5. **Duyệt Metadata Tree 3 tầng:** Trích xuất từ `tbl.inspect.snapshots()`, `tbl.inspect.manifests()`, `tbl.inspect.files()`.
6. **Schema & Partition Evolution:**
   * Thêm cột `tier` và đổi tên `latency_ms ➔ latency_millis`. Kiểm tra `field_id = 4` không đổi.
   * Cập nhật partition spec thêm `IdentityTransform("model")`, ghi tiếp ngày thứ 11. Bảng cùng lúc mang 2 `spec_id` (`[1, 2]`) mà không cần migration/rewrite dữ liệu cũ.

---

### 4. Lý do chọn phương pháp đó
* **Giải quyết dứt điểm nhược điểm của Hive:** Trong Hive, nếu người dùng quên lọc trên cột phụ `dt=2026-08-05`, hệ thống sẽ Full Scan toàn bộ bảng (gây tốn hàng nghìn USD chi phí cloud). Hidden Partitioning của Iceberg chuyển việc suy diễn phân vùng vào metadata của engine, loại bỏ hoàn toàn khả năng người dùng quên điều kiện lọc.
* **Schema Evolution bất biến theo field-ID:** Iceberg định danh cột bằng số nguyên cố định thay vì theo tên hay thứ tự vị trí, giúp việc đổi tên hay đổi vị trí cột chỉ là thao tác metadata (tốc độ < 1ms, 0 byte dữ liệu bị ghi lại).

---

### 5. Kết quả hoàn thành (Outputs & Metrics)
* **Tỷ lệ tỉa file (Pruning Ratio):** **10×** (Từ 10 files xuống còn đúng **1 file duy nhất** khi lọc theo ngày `ts` — vượt mục tiêu ≥ 5×).
* **Số lượng Snapshots:** **10 snapshots** tương ứng với 10 commits.
* **Độ ổn định Field ID:** Cột `latency_millis` giữ nguyên `field_id = 4`.
* **Tiến hóa phân vùng (Partition Evolution):** Ghi nhận cùng lúc 2 specs: `spec_id = [1, 2]`, tổng số dòng đọc được trên cả 2 spec là **5,500 dòng**.

---

### 6. Bằng chứng kiểm chứng (Evidence Log)
```text
  [PASS] pruning ratio ≥ 5x
  [PASS] ≥ 10 snapshots
  [PASS] field_id stable on rename
  [PASS] ≥ 2 partition specs
  [PASS] all rows readable

NB5 complete.
```
* **File lưu kết quả:** `notebooks/05_iceberg_catalog.ipynb`
* **Điểm số đạt:** **13 / 13 điểm** (Part B).

---

## 🟢 NOTEBOOK 06: `06_maintenance.ipynb` — TABLE MAINTENANCE: 4 MANDATORY JOBS

### 1. Nội dung ban đầu của Notebook
* **Mục tiêu:** Thực hành và đo lường định lượng **4 Job bảo trì bảng bắt buộc** (Compaction, Clustering, Snapshot Expiry, Orphan Removal) và Job thứ 5 (Log Checkpoint) trên cả Delta Lake và Apache Iceberg; phân tích chi phí FinOps (Object storage GET requests vs Data volume).
* **Công nghệ sử dụng:** `deltalake` (`optimize.compact()`, `optimize.z_order()`, `vacuum()`, `create_checkpoint()`), `pyiceberg` (`expire_snapshots()`), `pyarrow`.
* **Bối cảnh lý thuyết:** Slide §6 (Table Maintenance: 4 Job Bắt Buộc) & §12 (FinOps). Dữ liệu lưu tại `_lakehouse/scratch/maint_events` và `_lakehouse/iceberg/nb6`.

---

### 2. Yêu cầu của Notebook (Pass Criteria: 13/13 điểm)
Theo bảng tiêu chí đánh giá trong `rubric.md`:
* **Job 1 (Compaction):** Giảm số lượng file ít nhất 10× (báo cáo số liệu trước/sau) (4 điểm).
* **Job 2 (Clustering):** Bỏ qua ít nhất 50% số file khi query điểm dựa trên min/max stats (3 điểm).
* **Job 3 (Snapshot Expiry):** Delta `VACUUM` thu hồi dung lượng; Iceberg thu gọn còn đúng 3 snapshots (3 điểm).
* **Job 4 (Orphan Removal):** Quét và xoá sạch 3 orphan file Delta được cài cắm + dọn sạch các stranded manifest lists của Iceberg (2 điểm).
* **Job 5 (Checkpoint):** Tạo file checkpoint log (`*.checkpoint.parquet` + `_last_checkpoint`) (1 điểm).

---

### 3. Phương pháp hoàn thành
1. **Tạo tình trạng phân mảnh ban đầu:** Append 200 micro-batches (100.000 dòng) mô phỏng 200 lần commit liên tục từ streaming.
2. **Thực thi Job 1 (Compaction):** Chạy `dt.optimize.compact(target_size=1MB)`. Thu gom 200 files nhỏ thành 11 files lớn tối ưu.
3. **Thực thi Job 2 (Clustering):** Chạy `dt.optimize.z_order(["user_id"])`. Kiểm tra số file phải mở cho query `user_id = 12345` dựa trên việc kiểm tra dải `[min.user_id, max.user_id]` trong metadata add actions.
4. **Thực thi Job 3 (Snapshot Expiry):** 
   * Delta: Chạy `dt.vacuum(retention_hours=0)` để dọn dẹp các file cũ đã bị tombstone từ đợt compaction.
   * Iceberg: Chạy `ice.maintenance.expire_snapshots(older_than_ms=...)` giữ lại 3 snapshots gần nhất.
5. **Thực thi Job 4 (Orphan Removal):**
   * Giả lập 3 orphan files (file Parquet do job bị crash ghi dở dang, không nằm trong commit log).
   * Viết thuật toán Orphan Sweep: Tìm hiệu tập hợp `Files on disk − Files referenced in metadata log` (có guard time 24h) và xóa bỏ 3 file này.
   * Quét và dọn sạch các file manifest list `.avro` bị mồ côi sau khi Iceberg expire snapshots.
6. **Thực thi Job 5 (Log Checkpoint):** Tạo checkpoint Parquet gom 204 file log JSON nhỏ thành 1 file checkpoint Parquet và cập nhật `_last_checkpoint`.

---

### 4. Lý do chọn phương pháp đó
* **Bẫy sản xuất 1 (`VACUUM` không dọn được orphan chưa commit):** `VACUUM` của `deltalake` (Rust) chỉ dọn file được *tombstone* trong log. Một file do worker crash để lại chưa từng commit sẽ hoàn toàn vô hình trước `VACUUM`. Do đó, bắt buộc phải dùng thuật toán quét so khớp tập hợp đĩa vs metadata.
* **Bẫy sản xuất 2 (`expire_snapshots` chỉ xóa metadata):** Khi expire snapshot trong Iceberg, metadata manifest list cũ bị tách rời (stranded) vẫn nằm trên đĩa S3. Phải kết hợp cặp đôi Job 3 (Expiry) + Job 4 (Orphan Sweep) thì hóa đơn lưu trữ mới thực sự giảm.

---

### 5. Kết quả hoàn thành (Outputs & Metrics)
* **Job 1 (Compaction):** Giảm từ **200 files ➔ 11 files** (**18× ít file hơn**, vượt mục tiêu ≥ 10×).
* **Job 2 (Clustering):** Khi query `user_id = 12345`, trước clustering phải quét **11/11 files**, sau clustering chỉ cần quét **1/10 files** (**Tỷ lệ bỏ qua đạt 90%**, vượt mục tiêu ≥ 50%).
* **Job 3 (Expiry):** `VACUUM` thu hồi thành công **9.7 MB** dữ liệu tombstone; Iceberg thu gọn từ 20 snapshots về đúng **3 snapshots**.
* **Job 4 (Orphan Sweep):** Phát hiện và xóa sạch **3/3 orphan files Delta** và xóa **21 stranded manifest lists** (74.4 KB) của Iceberg.
* **Job 5 (Checkpoint):** Sinh thành công `00000000000000000204.checkpoint.parquet` và `_last_checkpoint`.
* **Phân tích FinOps:** Chứng minh chi phí theo số lượng object chiếm **24% hóa đơn**, khẳng định việc kiểm soát tần suất ghi streaming rẻ hơn nhiều so với việc trả tiền dọn dẹp file nhỏ liên tục.

---

### 6. Bằng chứng kiểm chứng (Evidence Log)
```text
  [PASS] compaction ≥ 10x fewer files
  [PASS] clustering skips ≥ 50% files
  [PASS] vacuum reclaimed bytes
  [PASS] 3 delta orphans removed
  [PASS] no delta orphans remain
  [PASS] checkpoint written
  [PASS] iceberg expired to 3 snaps
  [PASS] iceberg stranded files swept
  [PASS] iceberg data intact

NB6 complete.
```
* **File lưu kết quả:** `notebooks/06_maintenance.ipynb`
* **Điểm số đạt:** **13 / 13 điểm** (Part B).

---

## 🟢 NOTEBOOK 07: `07_vectors_multimodal.ipynb` — MULTIMODAL & VECTORS: INT8 QUANTIZATION & LIFECYCLE BUG

### 1. Nội dung ban đầu của Notebook
* **Mục tiêu:** Giải quyết bài toán dữ liệu đa phương tiện (Multimodal) và Vector Embeddings trên Data Lakehouse; so sánh kiến trúc lưu trữ **Blob Inline vs Pointer URI**; đo lường hiệu quả nén **Lượng tử hóa Int8 Quantization**; xây dựng **Semantic Search trực tiếp bằng SQL**; tái hiện và khắc phục **Lifecycle Bug** giữa Lakehouse và Vector DB độc lập thông qua **Change Data Feed (CDF)**.
* **Công nghệ sử dụng:** `deltalake` (`load_cdf()`, `delete()`), `duckdb` (hàm core `array_cosine_similarity`), `numpy`, `pyarrow`. Hoàn toàn không cần tải model AI hay cài Vector DB bên ngoài.
* **Bối cảnh lý thuyết:** Slide §11 (AI 2026: Multimodal, Vector & Agent). Dữ liệu lưu tại `_lakehouse/bronze/docs_multimodal`, `_lakehouse/blobs` và `_lakehouse/scratch/`.

---

### 2. Yêu cầu của Notebook (Pass Criteria: 13/13 điểm)
Theo bảng tiêu chí đánh giá trong `rubric.md`:
* **Đo lường Random-access Amplification (4 điểm):** Đo lường và giải thích hiện tượng khuếch đại đọc ngẫu nhiên (Amplification ≥ 5×) do đơn vị I/O của Parquet là Row Group.
* **Int8 Quantization (4 điểm):** Giảm dung lượng đĩa ít nhất 3×; báo cáo chỉ số `recall@10 ≥ 0.80` và `topic_fidelity ≥ 0.95`.
* **Semantic Search bằng SQL (1 điểm):** Truy vấn tương đồng cosine qua SQL DuckDB trả về các tài liệu lân cận cùng chủ đề.
* **Tái hiện Lifecycle Bug & CDF (4 điểm):** Chứng minh lỗi: trong bảng Lakehouse có 0 kết quả (đã xóa) nhưng Vector DB ngoài vẫn trả về kết quả rác cũ (> 0 hits); giải quyết bằng cách đọc sự kiện `delete` từ Change Data Feed.

---

### 3. Phương pháp hoàn thành
1. **Thử nghiệm Inline Blob vs Pointer:**
   * Tạo 2 bảng: `media_inline` (lưu mảng binary trực tiếp trong cột) và `media_pointer` (lưu URI trỏ đến file trong `blobs/`).
   * Phân tích metadata Parquet: Analytical scan (`SELECT topic, count(*)`) được bảo vệ hoàn toàn nhờ Column Pruning (chỉ đọc vài KB).
   * Đo lường Random Access: Để đọc 1 frame (64 KB) trong bảng Inline, Parquet buộc phải đọc và giải nén toàn bộ Row Group (12.5 MB), gây hiện tượng **Khuếch đại I/O (Amplification = 200×)** làm nghẽn GPU.
2. **Lượng tử hóa Vector (Int8 Quantization):**
   * Chuyển đổi vector 256 chiều từ `float32` sang `int8` theo công thức đối xứng: `np.clip(np.round(emb * 127), -127, 127)`.
   * So sánh kích thước vật lý trên đĩa và đánh giá chất lượng truy vấn: đo `recall@10` và độ trung thực ngữ cảnh `topic_fidelity`.
3. **Semantic Search trực tiếp bằng SQL:**
   * Đăng ký bảng Arrow vào DuckDB, ép kiểu `emb::FLOAT[256]` và gọi hàm `array_cosine_similarity` tìm Top 5 tài liệu tương đồng nhất.
4. **Tái hiện Lifecycle Bug & giải pháp Change Data Feed:**
   * Thực hiện xóa dữ liệu của đối tượng `user_042` khỏi bảng Lakehouse (`dt.delete()`).
   * Tái hiện lỗi: Vector DB ngoài không biết hành động xóa này nên vẫn trả về dữ liệu của `user_042`.
   * Khắc phục bằng CDF: Kích hoạt `delta.enableChangeDataFeed = true`, gọi `load_cdf(starting_version=1)` để trích xuất danh sách các `doc_id` bị xóa phát ra cho Vector Index cập nhật.

---

### 4. Lý do chọn phương pháp đó
* **Hiểu đúng bản chất Row Group của Parquet:** Parquet không đọc theo từng dòng mà đọc theo Row Group. Do đó, với dữ liệu media lớn cần đọc ngẫu nhiên liên tục cho GPU, mô hình Pointer hoặc định dạng chuyên biệt (Lance) là giải pháp tối ưu.
* **Lợi ích của Vector trực tiếp trong Lakehouse:** Khi nhúng vector vào cùng dòng với các trường quản trị (governance, consent, timestamps), việc truy vấn lọc bản quyền và xóa bỏ theo GDPR/PDPL diễn ra đồng bộ, loại bỏ hoàn toàn nguy cơ rò rỉ dữ liệu của Vector DB ngoài.

---

### 5. Kết quả hoàn thành (Outputs & Metrics)
* **Khuếch đại đọc ngẫu nhiên (Amplification):** **200×** (Phải đọc 12.5 MB Row Group để lấy 64.2 KB frame ảnh — vượt xa mục tiêu ≥ 5×).
* **Hiệu quả nén Int8 Quantization:**
  * Kích thước đĩa: **2.6 MB (Float32) ➔ 451.9 KB (Int8)** (**5.8× nhỏ hơn**, vượt mục tiêu ≥ 3×).
  * Độ chính xác truy vấn: `recall@10 = 0.906` (Mục tiêu ≥ 0.80); `topic_fidelity = 0.999` (Mục tiêu ≥ 0.95).
* **Tốc độ Semantic Search:** Quét toàn bộ 2.000 vectors trong **10.5 ms**; Top 5 kết quả đều chính xác 100% thuộc chủ đề `"storage"`.
* **Tái hiện Lifecycle Bug:** Trong bảng Lakehouse = **0 hits**, trong External Index = **5 hits**.
* **Change Data Feed:** Bắt đúng **5 sự kiện `delete`** tương ứng với 5 tài liệu của `user_042`.

---

### 6. Bằng chứng kiểm chứng (Evidence Log)
```text
  [PASS] random-access amplification ≥ 5x
  [PASS] int8 ≥ 3x smaller
  [PASS] int8 recall@10 ≥ 0.80
  [PASS] int8 topic fidelity ≥ 0.95
  [PASS] top-5 share query topic
  [PASS] lifecycle bug reproduced
  [PASS] CDF emits delete events

NB7 complete.
```
* **File lưu kết quả:** `notebooks/07_vectors_multimodal.ipynb`
* **Điểm số đạt:** **13 / 13 điểm** (Part B).

---

## 🟢 NOTEBOOK 08: `08_agents_provenance.ipynb` — AGENT TRAJECTORIES, MCP & DATA PROVENANCE (EU AI ACT)

### 1. Nội dung ban đầu của Notebook
* **Mục tiêu:** Quản trị vết thực thi của AI Agent (**Agent Trajectories** qua mô hình Medallion); thiết lập giao diện tích hợp **Model Context Protocol (MCP)** chuẩn 2026-07-28 kết nối giữa AI Agent và Data Catalog; quản trị nguồn gốc dữ liệu (**Data Provenance**) tuân thủ **EU AI Act Điều 10** và thực thi quyền xóa dữ liệu cá nhân (**Right-to-erasure** theo Luật Bảo vệ Dữ liệu Cá nhân PDPL).
* **Công nghệ sử dụng:** `deltalake`, `duckdb`, `pyiceberg`, `polars`. Hoàn toàn không phụ thuộc gọi LLM bên ngoài hay API key.
* **Bối cảnh lý thuyết:** Slide §11 (Agent Memory & Trajectory, MCP 2026-07-28) & §12 (Provenance & EU AI Act Art. 10). Dữ liệu lưu tại `_lakehouse/bronze/agent_traces`, `_lakehouse/silver/agent_trajectories`, `_lakehouse/gold/agent_performance`.

---

### 2. Yêu cầu của Notebook (Pass Criteria: 11/11 điểm)
Theo bảng tiêu chí đánh giá trong `rubric.md`:
* **Trajectory Medallion (3 điểm):** Tầng Silver phân vùng theo `agent_version` (`policy-v2`, `policy-v3`); Tầng Gold tổng hợp chỉ số hiệu năng bao phủ cả 2 chính sách.
* **Version Pinning (3 điểm):** Ghim cố định `table_version` vào metadata của lượt huấn luyện; khi chạy lại (replay) tại version đã ghim cho kết quả khớp chính xác 100%.
* **MCP Protocol Surface (3 điểm):** Danh sách tool có thể cache (`5 turns ➔ 1 catalog read`); thao tác nguy hiểm yêu cầu xác thực người dùng (`input_required`); tiến trình tác vụ chạy nền (`tasks/get`) hoàn tất trạng thái `completed`.
* **Data Provenance & Governance (2 điểm):** Phân vùng đủ **4 rổ dữ liệu hợp chuẩn Điều 10 EU AI Act**; loại bỏ hoàn toàn các dòng `UNCLASSIFIED` khỏi tập huấn luyện mô hình; thực hiện thành công yêu cầu xóa dữ liệu cá nhân (Right-to-erasure).

---

### 3. Phương pháp hoàn thành
1. **Quản lý Agent Trajectory qua Medallion:**
   * Tầng Bronze: Nạp 1.578 bước hành động thô `(observation, action, reward)`.
   * Tầng Silver: Chuẩn hóa cột, phân vùng theo `agent_version` để dễ dàng tái huấn luyện hoặc cô lập từng phiên bản chính sách RL.
   * Tầng Gold: Tổng hợp tỷ lệ thành công (`success_rate`), số bước trung bình (`avg_steps`), chi phí USD (`avg_cost_usd`) cho từng `agent_version`.
2. **Kỹ thuật Ghim phiên bản (Version Pinning):**
   * Lưu `table_version` vào cấu hình training run. Khi dữ liệu mới tiếp tục append vào bảng, huấn luyện viên / kiểm toán viên vẫn truy vấn chính xác dữ liệu gốc tại thời điểm huấn luyện bằng `DeltaTable(path, version=pinned_version)`.
3. **Mô phỏng MCP Server Protocol (2026-07-28 Spec):**
   * Stateless Core & Cacheable list tools: Sử dụng `ttlMs` giúp Agent gọi 5 lượt liên tiếp nhưng chỉ cần đọc Catalog đúng 1 lần.
   * Multi-round-trip Human-in-the-loop: Khi Agent cố tình gọi lệnh phá hủy (`drop_table`), MCP Server trả về `resultType: "input_required"`. Sau khi có xác nhận của con người, lệnh mới được thực thi (`resultType: "ok"`).
   * Background Task Polling: Gửi scan tác vụ lớn, poll qua `tasks/get` đến khi trạng thái chuyển sang `completed`.
4. **Quản trị Provenance & Quyền được xóa dữ liệu (Right-to-erasure):**
   * Phân loại dữ liệu thành 4 rổ hợp lệ: `LICENSED`, `SYNTHETIC`, `INTERNAL_LOGS`, `USER_CONSENTED` và 1 rổ cấm `UNCLASSIFIED`.
   * Loại bỏ các dòng `UNCLASSIFIED` (thiếu nguồn gốc bản quyền) khi xuất Model Card huấn luyện.
   * Khi người dùng `user_007` yêu cầu xóa dữ liệu: Truy xuất lịch sử nguồn gốc, thực hiện `dt.delete("subject_id = 'user_007'")` và xác nhận số dòng của đối tượng này trong bảng về đúng bằng 0.

---

### 4. Lý do chọn phương pháp đó
* **Đảm bảo tính tái lập (Reproducibility Contract) cho AI:** Dữ liệu RL và fine-tuning thay đổi liên tục theo thời gian. Ghim version Delta Lake vào metadata huấn luyện là giải pháp kỹ thuật duy nhất giúp giải trình được với các cơ quan kiểm toán AI (Annex IV của EU AI Act).
* **Tuân thủ pháp lý AI toàn cầu & Việt Nam:** Phân tách rổ dữ liệu theo Điều 10 EU AI Act và hỗ trợ cơ chế xóa dữ liệu cá nhân theo Luật Bảo vệ Dữ liệu Cá nhân (PDPL) đảm bảo hệ thống Lakehouse sẵn sàng đáp ứng mọi tiêu chuẩn tuân thủ quốc tế.

---

### 5. Kết quả hoàn thành (Outputs & Metrics)
* **Phân vùng Trajectory:** Tầng Silver tạo đủ 2 phân vùng `agent_version=policy-v2` và `agent_version=policy-v3`.
* **Hiệu năng Agent tại Gold:** `policy-v2` (Success 68.7%, 5.25 steps/session) vs `policy-v3` (Success 72.8%, 5.27 steps/session).
* **Khả năng tái lập:** Replay tại version ghim khớp chính xác tuyệt đối **1.578 steps**.
* **MCP Protocol:** 5 lượt hội thoại chỉ tốn **1 lần đọc Catalog**; lệnh drop table trả về đúng trạng thái `input_required`.
* **Provenance:** Phân vùng đủ **4 rổ dữ liệu chuẩn**; loại bỏ **80 bản ghi UNCLASSIFIED** khỏi tập train.
* **Xóa dữ liệu (Erasure):** Xóa toàn bộ **8 bản ghi của `user_007`** (trước: 8 ➔ sau: 0).

---

### 6. Bằng chứng kiểm chứng (Evidence Log)
```text
  [PASS] silver partitioned by agent_version
  [PASS] gold covers both policies
  [PASS] version pin replays exactly
  [PASS] 5 turns → 1 catalog read
  [PASS] destructive needs confirmation
  [PASS] confirmed call proceeds
  [PASS] tasks poll completes
  [PASS] all 4 Art.10 buckets present
  [PASS] unclassified rows found
  [PASS] erasure removed subject rows

NB8 complete.
```
* **File lưu kết quả:** `notebooks/08_agents_provenance.ipynb`
* **Trạng thái:** **Hoàn thành toàn bộ yêu cầu Part B.**

---

## 🟢 PHẦN C: KIỂM THỬ TÁI LẬP & KIỂM TOÁN HỆ THỐNG (REPRODUCIBILITY)

### 1. Kết quả kiểm thử tự động với Pytest (`pytest`)
* **Số lượng bài test:** **24 / 24 bài kiểm tra đạt PASS** (vượt chỉ tiêu 22 bài kiểm tra của đề bài).
* **Thời gian thực thi:** **2.68 giây**.
* **Nội dung kiểm chứng:** Môi trường Python 3.12, Delta-rs 1.x, tính năng cosine similarity core trong DuckDB, tính bất biến của Corpus Embeddings, tính năng cô lập và xóa Catalog Iceberg SQLite trên môi trường Windows.

### 2. Kết quả chạy tự động toàn diện 8 Notebooks (`scripts/run_all.py`)
* **Số lượng notebook:** **8 / 8 notebooks đạt PASS 100%**.
* **Thời gian thực thi:** **28.4 giây**.
* **Bằng chứng kiểm tra thực tế:**
  ```text
  Running 8 notebooks with C:\Users\Admin\Downloads\CodeLab18\K3A-Track2-Day18-NguyenNamAnh-2A202601703\.venv\Scripts\python.exe

    PASS  01_delta_basics.py                  0.6s
    PASS  02_optimize_zorder.py              11.4s
    PASS  03_time_travel.py                   0.7s
    PASS  04_medallion.py                     1.0s
    PASS  05_iceberg_catalog.py               1.6s
    PASS  06_maintenance.py                  10.7s
    PASS  07_vectors_multimodal.py            0.8s
    PASS  08_agents_provenance.py             1.5s

  8/8 passed in 28.4s
  ```

---

# 📋 BẢNG TỔNG HỢP TIẾN ĐỘ VÀ KẾT QUẢ THỰC HIỆN DỰ ÁN

| Phần | Hạng mục / Tên Notebook | Trạng thái | Tóm tắt kết quả chính đạt được |
| :--- | :--- | :---: | :--- |
| **Part A** | `01_delta_basics.ipynb` | ✅ Hoàn thành | Ghi nhận ACID JSON Log; Schema Enforcement chặn lỗi kiểu dữ liệu; Schema Evolution tự động merge cột `tier`; DuckDB truy vấn Zero-copy. |
| **Part A** | `02_optimize_zorder.ipynb` | ✅ Hoàn thành | Compaction gom 200 micro-batches ➔ 55 files; Z-Order theo `user_id` đạt **Tốc độ truy vấn tăng 11.2×** và **Tỷ lệ tỉa file đạt 55.0×**. |
| **Part A** | `03_time_travel.ipynb` | ✅ Hoàn thành | MERGE Upsert 100K dòng trong 0.08s; Time Travel RESTORE khôi phục về v2 trong 0.01s (rollback sạch 50 dòng lỗi); lưu vết đủ 5 phiên bản. |
| **Part A** | `04_medallion.ipynb` | ✅ Hoàn thành | Xây dựng Medallion 3 tầng; Silver khử trùng lặp loại bỏ 9.948 dòng lỗi; Gold tổng hợp đủ p50/p95 latency, error rate, cost qua 8 ngày × 3 models. |
| **Part B** | `05_iceberg_catalog.ipynb` | ✅ Hoàn thành | Quản trị bảng qua Iceberg SqlCatalog; Hidden Partitioning đạt **Tỷ lệ tỉa file 10×** khi lọc trên `ts`; Schema & Partition Evolution song hành (`spec_id = [1, 2]`). |
| **Part B** | `06_maintenance.ipynb` | ✅ Hoàn thành | Thực thi 4 Job bảo trì: Compaction (200 ➔ 11 files, 18×), Z-Order skip 90% files, VACUUM thu hồi 9.7 MB, Xóa 3 Delta orphans & 21 Iceberg stranded manifests, Checkpoint Parquet log. |
| **Part B** | `07_vectors_multimodal.ipynb` | ✅ Hoàn thành | Đo lường Amplification đọc ngẫu nhiên (200×); Int8 Quantization nén 5.8× (recall 0.906, fidelity 0.999); Semantic Search SQL (10.5ms); Sửa Lifecycle Bug qua CDF. |
| **Part B** | `08_agents_provenance.ipynb` | ✅ Hoàn thành | Trajectory Medallion phân vùng theo `agent_version`; Version Pinning tái lập 100%; MCP Server Cache & Human-in-the-loop; Phân loại 4 rổ EU AI Act; Thực thi Right-to-erasure. |
| **Part C** | `pytest` (24 bài test) | ✅ Hoàn thành | Toàn bộ 24/24 test unit kiểm tra tính toàn vẹn hệ thống và môi trường đều đạt PASS trong 2.68 giây. |
| **Part C** | `scripts/run_all.py` | ✅ Hoàn thành | Chạy kiểm thử tự động không giao diện liên hoàn 8/8 Notebooks đều đạt PASS trong 28.4 giây. |









