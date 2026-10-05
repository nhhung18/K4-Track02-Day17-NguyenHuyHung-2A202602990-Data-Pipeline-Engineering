# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Huy Hùng / 2A202602990
**Repo:** K4-Track02-Day17-NguyenHuyHung-2A202602990-Data-Pipeline-Engineering
**Commit bài nộp:** fbf74549564cf474f0b59b1452db9dd4b07cd131
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity Coding Assistant (Gemini 3.7 Flash) hỗ trợ phân tích luồng CDC, viết MERGE/Lookback SQL, refactor logic LLM cache và kiểm thử tự động.
**Nguồn tham khảo khác (nếu có):** Slide Ngày 17 Data Pipeline Engineering; Tài liệu Debezium CDC Postgres Connector; dbt Core Microbatch documentation.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` fail contract vì có 24 dòng cho 12 tickets; re-run ngày cũ làm lệch trạng thái T-91. | `gold_feature_daily` fail đối chiếu full recompute; event trễ ngày 08-12 của u05 bị mất trên Gold. | Check xóa T-97 fail; T-97 không thành tombstone và vẫn xuất hiện ở training snapshot / doc chunks. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT` thẳng thay vì `MERGE` theo khoá và không có guard so sánh LSN. | `LOOKBACK_DAYS = 0` trong `config.py`, chỉ tính ngày hiện tại nên bỏ sót event đến trễ từ Bronze. | `ticket_changes_sql` chỉ lấy `ticket_id` từ `after`, khi `op='d'` thì `after=null` nên bị lọc mất. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: dùng `MERGE INTO ... ON ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`. | `pipeline/config.py`: đổi `LOOKBACK_DAYS = 3` (bằng $\lceil\text{P99}\rceil$ đo từ `event_lateness_sql`). | `pipeline/staging.py`: dùng `COALESCE(after->>'ticket_id', before->>'ticket_id', key->>'ticket_id')`. |
| **Khái niệm trên slide** | *Silver — Có khoá*, Idempotent UPSERT, LSN Guard. | *Data về muộn*, Event Time vs Ingestion Time, Overwrite-partition. | *CDC Debezium Envelope*, Tombstone Soft Delete, Xóa phải lan. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` là bảng thực thể biến động theo dòng cần upsert theo khóa định danh, còn `gold_feature_daily` là bảng tổng hợp theo ngày nên ghi đè theo phân vùng lookback vừa nhanh, vừa tự nhiên khử trùng lặp mà không cần duy trì trạng thái phức tạp.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giữ dòng tombstone với PII đã xóa sạch vừa tuân thủ quyền riêng tư (Nghị định 13/GDPR) vừa duy trì toàn vẹn SCD2 và ngăn các đợt rerun/backfill cũ vô tình chèn lại bản ghi đã bị xóa.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến (Immutability) và khả năng tái lập (Reproducibility) tuyệt đối cho ML Governance, giúp các mô hình huấn luyện trong quá khứ luôn có thể audit và đối chiếu chính xác.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: DuckDB chạy in-process dạng vector hóa với chi phí hạ tầng bằng 0, độ trễ cực thấp cho dữ liệu vừa và nhỏ, tránh hoàn toàn chi phí vận hành và overhead phân tán không cần thiết của cụm Spark.

## 4. Hai câu hỏi suy ngẫm

1. **Xung đột giữa "Snapshot bất biến" và "Quyền được xoá dữ liệu":**
   Để dung hòa, trong môi trường sản xuất ta áp dụng kỹ thuật **Crypto-shredding** (mỗi người dùng/ticket được mã hóa bằng một khóa riêng biệt; khi nhận yêu cầu xóa, chỉ cần hủy khóa mã hóa thì toàn bộ văn bản trong các snapshot lịch sử trở thành vô nghĩa mà không phá vỡ cấu trúc file Parquet hay checksum của bảng), kết hợp đánh dấu blacklist metadata cho các model training run tiếp theo, hoặc thực hiện một quy trình Compaction/Redaction định kỳ có ghi log kiểm toán (Audit Trail) để tái sinh snapshot hợp chuẩn khi có yêu cầu pháp lý bắt buộc.
2. **Chốt PII cho tên riêng "Nguyễn Văn An":**
   Cần đặt chốt PII tại **tầng Silver** (trước khi dữ liệu được làm sạch đưa vào feature store hay RAG index). Thay vì chỉ dùng Regex đơn thuần, ta sử dụng mô hình **Named Entity Recognition (NER)** tiếng Việt (như PhoBERT-NER) kết hợp từ điển danh xưng/khách hàng từ bảng master Users để nhận diện nhãn thực thể `PER` (Person) và thay thế bằng `<NAME>` hoặc băm giả danh (Pseudonymization). Đo lường hiệu quả bằng **Precision/Recall/F1** trên tập benchmark PII nội bộ và cài đặt metric `pii_leakage_count` trong Data Quality Gate để cảnh báo/chặn pipeline nếu phát hiện rò rỉ.

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 3.96s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /home/hungnguyen/Workspace/AI_in_Action/Lab/K4-Track02-Day17-NguyenHuyHung-2A202602990-Data-Pipeline-Engineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
03:04:40  Running with dbt=1.12.5
03:04:40  Registered adapter: duckdb=1.11.0
03:04:44  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
03:04:44  Concurrency: 1 threads (target='dev')
03:04:45  1 of 19 START sql view model main.stg_events ................................... [RUN]
03:04:45  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.17s]
03:04:45  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
03:04:45  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.05s]
03:04:45  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
03:04:45  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.20s]
03:04:45  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
03:04:45  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.27s]
03:04:45  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
03:04:46  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.19s]
03:04:46  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
03:04:46  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.08s]
03:04:46  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
03:04:46  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
03:04:46  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
03:04:46  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.04s]
03:04:46  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
03:04:46  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.04s]
03:04:46  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
03:04:46  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
03:04:46  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
03:04:46  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.04s]
03:04:46  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
03:04:46  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.04s]
03:04:46  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
03:04:46  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.05s]
03:04:46  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
03:04:46  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.04s]
03:04:46  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
03:04:46  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.04s]
03:04:46  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
03:04:46  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
03:04:46  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.05s]
03:04:46  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
03:04:46  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.11s]
03:04:46  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
03:04:46  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.06s]
03:04:46  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
03:04:46  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.06s]
03:04:46  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
03:04:46  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.06s]
03:04:46  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
03:04:46  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.06s]
03:04:46  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
03:04:46  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.06s]
03:04:47  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.51s]
03:04:47  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
03:04:47  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
03:04:47  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
03:04:47  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.04s]
03:04:47  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
03:04:47  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.03s]
03:04:47  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 2.24 seconds (2.24s).
03:04:47  Completed successfully
03:04:47  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bonus Outputs

**B1 — LLM Step:**
```text
$ python -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

**B2 — Design Document:**
Tài liệu brainstorm thiết kế hệ thống pipeline cho AI Pháp lý & Hợp đồng doanh nghiệp được lưu tại [bonus/DESIGN.md](../bonus/DESIGN.md).

