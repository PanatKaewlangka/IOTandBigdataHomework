# Personal Accounting Pipeline - Project Review Guide

## 🚀 Overview
Project นี้เป็นการสร้าง Data Pipeline โดยใช้ PySpark เพื่อแปลงข้อมูลธนาคาร 10 ปี (72 ล้านแถว) ให้เป็น Insights สำหรับวิเคราะห์การใช้จ่ายของครอบครัว

## 🛠️ Step-by-Step Implementation

### Step 1: Schema Definition (`src/etl/schemas.py`)
- **Action:** กำหนด Schema ล่วงหน้า (Explicit Schema) แทนการใช้ `inferSchema=True`.
- **Reason:** เพิ่มความเร็วในการอ่านข้อมูลและป้องกันข้อผิดพลาดของประเภทข้อมูล (Data Integrity).
- **Knowledge:** `StructType`, `StructField`, `StringType`, `FloatType`.

### Step 2: Data Validation (`src/etl/validations.py`)
- **Action:** กรองข้อมูลเสีย (amount เป็น null หรือวันที่นอกเหนือช่วงปี 2016-2025).
- **Reason:** "Garbage In, Garbage Out" - ต้องล้างข้อมูลเสียทิ้งก่อนเพื่อความแม่นยำ.
- **Knowledge:** `df.filter()`, `F.col().isNotNull()`, `F.col().between()`.

### Step 3: Transformations (`src/etl/transformations.py`)
- **Action:**
  - `categorize_spending`: แบ่งเกรด spending (micro to large) โดยใช้ `abs()` ของยอดเงิน.
  - `enrich_with_lookups`: ทำ **Left Join** กับตาราง lookup.
- **Reason:**
  - Left Join สำคัญมาก: เพื่อรักษา Transactions ทั้งหมดไว้ (แม้ร้านค้าจะไม่มีในรายชื่อ lookup ก็ตาม).
- **Knowledge:** `F.when().otherwise()`, `df.join(how='left')`.

### Step 4: Testing with TDD (`tests/`)
- **Action:** เขียน Unit Test 7 ฟังก์ชัน ครอบคลุมเคสปกติ, เคส Null, และยอดติดลบ (Refunds).
- **Reason:** มั่นใจว่า Logic ถูกต้อง 100% ก่อนรันข้อมูลจริงระดับ 70 ล้านแถว.
- **Knowledge:** `pytest`, `chispa` (`assert_df_equality`).

### Step 5: Data Lifecycle & Layers (`src/etl/pipeline.py`)
- **Action:** จัดเก็บข้อมูลเป็น 3 เลเยอร์ (Raw -> Staged -> Analytics) ในรูปแบบ **Parquet**.
- **Reason:**
  - **Parquet:** เป็น Columnar format อ่านเฉพาะคอลัมน์ที่ต้องการได้เร็วมากและบีบอัดข้อมูลได้ดี.
  - **Layers:** ทำให้ข้อมูลเป็นระเบียบและสามารถรันซ่อม (Reprocess) เฉพาะส่วนได้ง่าย.
- **Knowledge:** `df.write.parquet()`, Medallion Architecture Concept.

### Step 6: Analytics & Insights (`src/etl/pipeline.py`)
- **Action:** คำนวณสรุปผลรายเดือน/รายปี และใช้ **Window Functions** เพื่อหา Top 10 Merchants.
- **Reason:** ตอบโจทย์ทางธุรกิจ (Business Questions) และค้นหาเทรนด์ (Trends).
- **Knowledge:** `groupBy()`, `agg()`, `Window.partitionBy().orderBy()`, `F.rank()`.

---

## 💡 Key Concepts for Exam (VIVA)
1. **ทำไมต้องใช้ Spark?** เพราะข้อมูลมีขนาดใหญ่ (72 ล้านแถว) Spark ประมวลผลแบบกระจาย (Distributed Computing) ทำให้รันเสร็จในเวลาไม่กี่นาที.
2. **ทำไมใช้ Parquet แทน CSV?** Parquet บีบอัดข้อมูลได้ดีกว่า และ Spark อ่านแบบ Column-based ทำให้เร็วกว่า CSV ที่เป็น Row-based.
3. **Left Join vs Inner Join ในโปรเจกต์นี้?** ถ้าใช้ Inner Join ธุรกรรมที่มีร้านค้าไม่ตรงกับ Lookup จะหายไปทันที (Data Loss) ดังนั้นต้องใช้ Left Join.
4. **Lazy Evaluation คืออะไร?** (Spark Concept) คือการที่ Spark ยังไม่ทำงานจนกว่าจะมี Action (เช่น `write` หรือ `show`) เพื่อให้ Spark สามารถวางแผนการคำนวณที่คุ้มค่าที่สุด (Query Optimization) ได้ก่อนรันจริง.
