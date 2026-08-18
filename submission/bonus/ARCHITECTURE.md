# BÁO CÁO THIẾT KẾ KIẾN TRÚC LAKEHOUSE (ARCHITECTURE BRIEF)

**Đề tài lựa chọn:** **Topic C — Nền tảng CDC Streaming từ Ride-Hailing Việt Nam sang Lakehouse tuân thủ Nghị định 13/2023/NĐ-CP**  
**Vai trò:** Lead Data Architect  
**Tác giả:** Nguyễn Nam Anh (K3A-Track2-Day18)  
**Deliverable path:** `submission/bonus/ARCHITECTURE.md`  

---

## 1. Problem Statement (Tuyên bố bài toán — ≤ 200 từ)

Hệ thống đặt xe công nghệ xử lý **100 triệu chuyến/năm**, chịu tải đỉnh **30.000 writes/giây** (giờ cao điểm mưa ngập/tan tầm). Toàn bộ thay đổi dữ liệu từ Oracle OLTP được truyền phát qua Debezium CDC sang Lakehouse để phục vụ Dashboard vận hành (SLA: dữ liệu cập nhật **< 60 giây**) và Ad-hoc query tài chính/vận hành (SLA: **p95 < 1 giây**).

**Thách thức cốt lõi:**
1. **Ràng buộc pháp lý nghiêm ngặt:** Dữ liệu chứa PII nhạy cảm (Số điện thoại, CCCD, GPS thời gian thực, số dư ví) thuộc phạm vi điều chỉnh của **Nghị định 13/2023/NĐ-CP** và Luật Bảo vệ Dữ liệu Cá nhân Việt Nam. Mọi truy cập PII thô phải được ghi vết (Audit trail), và hệ thống phải đáp ứng quyền rút lại sự đồng ý / xóa dữ liệu cá nhân (Right-to-erasure) trong vòng 72 giờ.
2. **Dữ liệu đến muộn (Late-arriving data):** Ứng dụng tài xế tại các vùng sóng yếu thường xuyên mất mạng, gửi bù các gói dữ liệu định vị/hoàn thành cuốc xe trễ hàng giờ.
3. **Xung đột ghi đồng thời (Concurrency Conflicts):** 30K events/giây cập nhật trạng thái cuốc xe liên tục dễ gây lỗi xung đột ghi (Write Collision) khi thực hiện Upsert vào Lakehouse.

---

## 2. Architecture Diagram (Sơ đồ kiến trúc toàn thể)

```text
┌───────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   INGESTION & GOVERNANCE PIPELINE                                 │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
 [Oracle OLTP] (30K w/s) ──► [Debezium CDC] ──► [Kafka Topics: trip_events, driver_status]
                                                       │
                                                       ▼
 ┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
 │ BRONZE LAYER (_lakehouse/bronze/cdc_raw)                                                         │
 │  * Storage: Delta Lake (Append-only, hourly partitioned: date=YYYY-MM-DD/hour=HH)                │
 │  * Ingestion: Spark Structured Streaming (Trigger: 30s micro-batch)                             │
 │  * In-line Tokenization: SHA-256 + Per-Tenant Secret Salt (Hash PII ngay tại cửa khẩu Ingestion)│
 │  * Raw PII Vault: Mã hóa AES-256 lưu ở bucket biệt lập (chỉ DPO & Security Service có key giải mã│
 └────────────────────────────────┬────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
 │ SILVER LAYER (_lakehouse/silver/trips_cleaned)                                                  │
 │  * Storage: Delta Lake 4.x (Liquid Clustering by [driver_id, date, status])                      │
 │  * Deduplication & Late-Data Resolution: MERGE WHEN MATCHED AND src.event_ts > tgt.event_ts      │
 │  * SCD Type 2: Theo dõi biến động trạng thái tài xế (active, en-route, completed, suspended)     │
 │  * Deletion Vectors enabled: Giảm 95% chi phí I/O khi cập nhật/xóa dòng đơn lẻ                  │
 │  * Delta Change Data Feed (CDF): Phát luồng CDC phái sinh cho downstream feature store/ML       │
 └────────────────────────────────┬────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
 │ GOLD LAYER (_lakehouse/gold/operational_metrics)                                                │
 │  * Storage: Delta Lake (Pre-aggregated aggregates, partitioned by [region_id, date])            │
 │  * Metrics: Revenue, surge_multiplier, avg_pickup_eta, cancel_rate (refresh mỗi 60s)            │
 └────────────────────────────────┬────────────────────────────────────────────────────────────────┘
                                  │
 ┌────────────────────────────────┴────────────────────────────────────────────────────────────────┐
 │ SERVING & AUDIT CONTROL PLANE                                                                   │
 │  * Query Engine: DuckDB / Trino Zero-copy qua Apache Arrow                                      │
 │  * Governance & Lineage: OpenLineage + Apache Polaris REST Catalog (RBAC + Row-Level Filter)   │
 │  * Audit Trail: Bảng Delta `audit_pii_access` ghi lại 100% truy vấn chạm vào PII (User, Time, Lý do)│
 └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Quyết định kiến trúc chính & Các lựa chọn đã loại bỏ (≥ 5 Quyết định)

### 📌 Quyết định 1: Định dạng bảng lưu trữ (Table Format)
* **Tôi chọn:** **Delta Lake 4.x**.
* **Tôi loại Apache Iceberg vì:** Delta Lake hỗ trợ tính năng **Deletion Vectors** (ghi nhận các dòng bị xóa/sửa vào bitmap thay vì phải rewrite toàn bộ file Parquet ngay lập tức) và cơ chế **Change Data Feed (CDF)** được tích hợp tự nhiên với Debezium format. Ở tần suất 30.000 writes/giây, Iceberg v2 Merge-on-Read tạo ra nhiều positional delete files làm tăng độ trễ truy vấn khi chưa kịp minor-compact.
* **Tôi loại Apache Hudi vì:** Mặc dù Hudi mạnh về Upsert streaming (MoR), nhưng hệ sinh thái catalog và khả năng query zero-copy từ DuckDB/Polars của Hudi kém linh hoạt hơn Delta Lake trên nền tảng mở.

---

### 📌 Quyết định 2: Chiến lược Tokenization & Tuân thủ Nghị định 13
* **Tôi chọn:** **In-line Deterministic Pseudonymization tại Bronze Landing** (HMAC-SHA256 kết hợp Khóa Salt động lưu trong AWS KMS / HashiCorp Vault).
* **Tôi loại phương án Masking ở Serving Layer (Dynamic Masking) vì:** Nếu lưu PII thô (raw phone, CCCD, GPS) trực tiếp vào tầng Bronze/Silver và chỉ che ở tầng view/query, một sự cố rò rỉ dữ liệu (data dump / S3 bucket misconfiguration) sẽ làm lộ toàn bộ dữ liệu thật, vi phạm nghiêm trọng Điều 13 & 38 Nghị định 13/2023/NĐ-CP (mức phạt lên tới 5% doanh thu).
* **Tôi loại phương án Random Surrogate ID Lookup Table vì:** Tạo bảng tra cứu khóa ngẫu nhiên (Lookup DB) cho 100 triệu người dùng sẽ tạo nút thắt cổ chai (bottleneck) và single-point-of-failure khi streaming tốc độ 30K ops/s.

---

### 📌 Quyết định 3: Xử lý dữ liệu đến muộn & Xung đột trạng thái (Late-Arriving Events)
* **Tôi chọn:** **Stateful Delta MERGE với điều kiện so khớp timestamp nguồn (`src.event_ts > tgt.event_ts`)**.
* **Tôi loại phương án Append-only với View Dedup (`ROW_NUMBER()`) vì:** Mặc dù ghi Append-only rất nhanh ở cửa vào, nhưng khi Analyst và Dashboard truy vấn trên 100 triệu dòng, việc thực thi Window Function `ROW_NUMBER()` trong thời gian thực làm ad-hoc query vượt quá SLA 1 giây (p95 tăng vọt lên 8–15 giây).
* **Tôi loại phương án Overwrite Partition theo ngày vì:** Ghi đè toàn bộ phân vùng khi có event cũ đến muộn sẽ gây lock bảng và tốn chi phí I/O khổng lồ (write amplification).

---

### 📌 Quyết định 4: Chiến lược Phân vùng và Sắp xếp dữ liệu (Partitioning & Clustering)
* **Tôi chọn:** **Partition theo `date` kết hợp Liquid Clustering trên `[driver_id, status]`**.
* **Tôi loại Hive-style Multi-level Partitioning (`date/region/driver_id`) vì:** Phân vùng quá sâu theo `driver_id` (hơn 200.000 tài xế) dẫn tới **Small-File Disaster** nghiêm trọng (hàng triệu file vài KB), khiến metadata log phình to làm sập Query Planner.
* **Tôi loại Static Z-ORDER định kỳ thủ công vì:** Z-ORDER cổ điển yêu cầu chạy lệnh nặng nề trên toàn bộ dữ liệu lịch sử. Liquid Clustering phân cụm gia tăng (incremental clustering) trên các micro-batches mới, phù hợp với luồng streaming 24/7.

---

### 📌 Quyết định 5: Kiểm soát ranh giới bảo mật & Quản trị (Control Plane)
* **Tôi chọn:** **Apache Polaris Catalog (REST Catalog Spec) làm Control Plane trung tâm**.
* **Tôi loại Hive Metastore (HMS) truyền thống vì:** HMS chỉ đóng vai trò tra cứu tên thư mục (Path lookup), không có tính năng Server-side scan planning, không có phân quyền bảo mật cấp dòng/cột (Row/Column level security) và không hỗ trợ chuẩn REST Catalog 2026.
* **Tôi loại Databricks Unity Catalog độc quyền vì:** Mục tiêu kiến trúc là tránh Vendor Lock-in, đảm bảo 100% mã nguồn mở và có thể triển khai on-premise tại trung tâm dữ liệu Việt Nam đáp ứng yêu cầu lưu trữ dữ liệu trong nước (Nghị định 53/2022/NĐ-CP).

---

## 4. Failure Modes & 3:00 AM Runbook (Kịch bản sự cố & Xử lý lúc 3h sáng)

### 🔴 Sự cố 1: Driver App mất mạng gửi dồn 50.000 chuyến xe cũ làm sai lệch doanh thu ngày hôm trước
* **Triệu chứng (Detection):** Alert từ Prometheus: độ trễ `event_delay_seconds > 7200` tăng vọt; doanh thu ngày hôm trước bị nhảy số bất thường trên dashboard tài chính.
* **Cơ chế phòng thủ:**
  1. Câu lệnh `MERGE` tại Silver sử dụng biểu thức bảo vệ:
     ```sql
     MERGE INTO silver_trips tgt USING incoming_batch src ON tgt.trip_id = src.trip_id
     WHEN MATCHED AND src.event_ts > tgt.event_ts THEN UPDATE SET *
     WHEN NOT MATCHED THEN INSERT *;
     ```
  2. Ghi nhận các event đến muộn vào một phân vùng Delta riêng `is_late_arriving = true` kèm theo trigger cập nhật bảng tổng hợp Gold tương ứng.
* **Kế hoạch Rollback:** Nếu batch bị lỗi dữ liệu từ client: Sử dụng **Time Travel RESTORE** về phiên bản trước khi nạp batch lỗi (`RESTORE TABLE silver_trips TO VERSION AS OF <safe_version>`). Thời gian phục hồi: **< 1 phút**.

---

### 🔴 Sự cố 2: Yêu cầu Xóa Dữ liệu Cá nhân (Right-to-Erasure) khẩn cấp từ cơ quan quản lý
* **Triệu chứng:** Người dùng `user_9981` gửi yêu cầu hủy dịch vụ và xóa vĩnh viễn dữ liệu theo Điều 16 Nghị định 13.
* **Cơ chế xử lý 3 bước chuẩn hóa:**
  1. **Bước 1 (Xóa logic tức thì):** Chạy `DELETE FROM silver_trips WHERE user_token = 'token_9981'`. Nhờ **Deletion Vectors**, thao tác hoàn tất trong **< 500ms** mà không lock bảng.
  2. **Bước 2 (Phát sự kiện xóa cho hệ sinh thái):** Bật Delta CDF, hệ thống stream sự kiện `_change_type = 'delete'` sang Elasticsearch/Vector DB downstream để xóa đồng bộ.
  3. **Bước 3 (Xóa vật lý triệt để & Expiry):** Chạy Job bảo trì `VACUUM silver_trips RETAIN 72 HOURS` sau khi hết thời hạn lưu trữ kiểm toán để thu hồi toàn bộ file Parquet vật lý cũ trên đĩa.

---

### 🔴 Sự cố 3: Đụng độ ghi đồng thời (Concurrent Write Conflict) giữa Streaming Ingestion và Compaction Job
* **Triệu chứng:** Pipeline streaming báo lỗi `ConcurrentAppendException` hoặc `ConcurrentTransactionException`.
* **Cơ chế xử lý:**
  1. Cấu hình Delta **Optimistic Concurrency Control (OCC)** với chiến lược retry cấp số nhân (`max_retries = 5, backoff = 2s`).
  2. Phân tách ranh giới ghi: Job Compaction chỉ chạy trên các phân vùng tĩnh cũ (`date < CURRENT_DATE()`), luồng Streaming Ingestion ghi vào phân vùng hiện tại (`date = CURRENT_DATE()`). Do hai tác vụ chạm vào các tập file hoàn toàn độc lập, xung đột ghi giảm về **0%**.

---

## 5. Ước lượng chi phí (Back-of-Envelope FinOps Math)

### Giả định quy mô dữ liệu:
* 100 triệu chuyến/năm ≈ **274.000 chuyến/ngày**.
* Kèm theo GPS pings & Driver status: ~**50 triệu events/ngày**.
* Kích thước bản ghi Bronze thô: ~500 bytes/event ➔ **25 GB/ngày raw**.
* Tốc độ nén Parquet (Snappy): ~4× ➔ **6.25 GB/ngày nén**.
* Lưu trữ 1 năm (365 ngày): $6.25 \text{ GB} \times 365 \approx \mathbf{2.3 \text{ TB/năm}}$.

### 💰 Chi phí Lưu trữ (Storage Cost - AWS ap-southeast-1 / VN Cloud tương đương):
1. **Tầng Hot (30 ngày gần nhất - S3 Standard @ $0.023/GB/tháng):**
   $$187.5 \text{ GB} \times \$0.023 \approx \mathbf{\$4.31/\text{tháng}}$$
2. **Tầng Warm (31–90 ngày - S3 Infrequent Access @ $0.0125/GB/tháng):**
   $$375 \text{ GB} \times \$0.0125 \approx \mathbf{\$4.69/\text{tháng}}$$
3. **Tầng Cold (91–365 ngày - S3 Glacier Flexible @ $0.0036/GB/tháng):**
   $$1,718 \text{ GB} \times \$0.0036 \approx \mathbf{\$6.18/\text{tháng}}$$
4. **Chi phí S3 API Requests (đã qua Compaction gom file 128MB):**
   * Số lượng file sinh ra/ngày: $\sim 50 \text{ files/ngày}$.
   * PUT/GET requests: $\sim \mathbf{\$1.50/\text{tháng}}$.
* ➔ **Tổng chi phí lưu trữ Storage:** $\approx \mathbf{\$16.68/\text{tháng}}$ (Cực kỳ tối ưu so với ngân sách cho phép).

### 💻 Chi phí Compute (Xử lý Streaming & Bảo trì):
* **Cluster Streaming Ingestion:** 2 nodes `c6g.xlarge` (4 vCPU, 8GB RAM Graviton) chạy Spark Structured Streaming 24/7:
  $$2 \times \$0.136/\text{giờ} \times 730 \text{ giờ} \approx \mathbf{\$198.56/\text{tháng}}$$
* **Query Engine (DuckDB Serverless / Trino on-demand):** $\sim \mathbf{\$50.00/\text{tháng}}$.
* ➔ **TỔNG CHI PHÍ NỀN TẢNG (Total FinOps Bill):** $\approx \mathbf{\$265.24/\text{tháng}}$.

---

## 6. Lát cắt triển khai MVP trong 1 tuần (Week-1 Shippable Vertical Slice)

Trong tuần đầu tiên, team sẽ không dựng toàn bộ hạ tầng mà tập trung phát triển một **Lát cắt dọc (Vertical Slice)** chứng minh tính khả thi của 2 cơ chế khó nhất:

```text
[Ngày 1-2] Khởi tạo Bronze Delta Table + Viết hàm Python Tokenization PII (SHA-256 + Salt).
[Ngày 3-4] Xây dựng luồng Debezium CDC giả lập ➔ Ingest vào Bronze ➔ Chạy MERGE vào Silver xử lý late-arriving events.
[Ngày 5]   Kiểm thử tính năng Xóa dữ liệu (Right-to-erasure) bằng Deletion Vectors và Time Travel Replay.
[Ngày 6]   Đo lường SLA: Xác nhận độ trễ End-to-end từ Event ➔ Dashboard đạt < 60 giây.
[Ngày 7]   Đóng gói Demo PoC, chuẩn bị bài thuyết trình Design Review.
```

---

## 7. Mã nguồn thực nghiệm (Proof-of-Concept Implementation)
Mã nguồn thực nghiệm độc lập kiểm chứng cơ chế **Tokenization tại Bronze** và **Late-Arriving Data MERGE** được lưu tại:  
👉 [submission/bonus/poc/poc_cdc_decree13.py](file:///c:/Users/Admin/Downloads/CodeLab18/K3A-Track2-Day18-NguyenNamAnh-2A202601703/submission/bonus/poc/poc_cdc_decree13.py)
