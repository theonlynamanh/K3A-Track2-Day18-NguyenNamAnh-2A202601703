# Reflection: Top Lakehouse Anti-Patterns

**Anti-pattern team tôi có nguy cơ vướng phải cao nhất:** *Anti-Pattern #2 — Bỏ qua bài toán Small-File Problem và thiếu lịch trình 4 Job Bảo trì Bắt buộc (Table Maintenance).*

### Lý do và Phân tích Rủi ro:
1. **Bẫy Streaming Ingestion:** Khi ingest dữ liệu liên tục từ Kafka/CDC với chu kỳ micro-batch ngắn (5–10s), hệ thống tích tụ hàng trăm nghìn file Parquet nhỏ (< 1 MB). Chi phí S3 GET requests tăng phi mã (chiếm tới ~24% hóa đơn lưu trữ), đồng thời làm tốc độ quét bảng suy giảm nghiêm trọng.
2. **Điểm mù của `VACUUM`:** `VACUUM` chuẩn chỉ dọn dẹp các file đã được ghi nhận (tombstoned) trong transaction log. Các file rác phát sinh do worker crash giữa chừng chưa từng commit sẽ hoàn toàn "vô hình" trước `VACUUM`, gây rò rỉ chi phí lưu trữ âm thầm.

### Giải pháp Khắc phục:
* **Buffer Streaming:** Tăng chu kỳ commit micro-batch lên 1–2 phút để giảm tần suất tạo file rác.
* **Tự động hóa 4 Job Maintenance:** Thiết lập cron job định kỳ thực hiện **Compaction** (đưa file về chuẩn 128–512 MB), **Z-ORDER clustering**, chạy cặp đôi **Snapshot Expiry** và thuật toán **Orphan Sweep** (so khớp tập hợp đĩa vật lý vs metadata log).
