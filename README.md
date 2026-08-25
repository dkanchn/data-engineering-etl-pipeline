# Data Engineering ETL Pipeline

โปรเจกต์ ETL Pipeline ที่จัดทำขึ้นเป็นส่วนหนึ่งของ Data Engineer Technical Assessment

โปรเจกต์นี้มีเป้าหมายเพื่อ Extract ข้อมูลดิบจาก SQLite views จากนั้นทำการ Clean และ Transform ข้อมูลด้วย Python และ Pandas ก่อน Load ข้อมูลที่ผ่านการทำความสะอาดแล้วเข้าสู่ Analytical SQLite Database เพื่อใช้สำหรับการวิเคราะห์ Customer Lifetime Value (CLV)

## Tech Stack

- Python 3.12+
- SQLite
- Pandas
- Prefect
- Pytest

## Part 1: Data Exploration

Query สำหรับการสำรวจข้อมูลทั้งหมดอยู่ในไฟล์ `exploration.sql`

เริ่มจากการตรวจสอบโครงสร้างของแต่ละ view และชนิดข้อมูลของแต่ละ column จากนั้นตรวจสอบข้อมูลจริงเพื่อค้นหา Data Quality Issues ที่อาจส่งผลต่อการทำความสะอาดและการวิเคราะห์ข้อมูล

Source views ที่ทำการสำรวจ ได้แก่:

- `vw_raw_customers`
- `vw_raw_orders`
- `vw_exchange_rates`

### Data Quality Issues ที่พบ

1. **Duplicate customer records**  
   `customer_id = 1` และ `customer_id = 2` ปรากฏใน `vw_raw_customers` คนละ 2 records โดยข้อมูลบางส่วน เช่น `email`, `phone` และ `signup_date` แตกต่างกัน  
   **Handling:** เก็บ record ที่มี `signup_date` ล่าสุดของแต่ละ `customer_id`

2. **Orphaned customer references**  
   `order_id = 106` และ `118` อ้างอิงไปยัง `customer_id = 99` ซึ่งไม่มีอยู่ใน `vw_raw_customers`  
   ปัญหานี้ถูกระบุไว้ระหว่าง Data Exploration แต่ไม่ได้ Filter ออก เนื่องจาก Assignment ไม่ได้กำหนด Cleaning Rule สำหรับกรณีนี้

3. **Invalid order amounts**  
   `order_id = 103`, `113` และ `114` มี `total_amount` เท่ากับ `-50`, `-100` และ `0` ตามลำดับ ซึ่งเป็นค่าที่ไม่ถูกต้องสำหรับ Order  
   **Handling:** Filter Orders ที่มี `total_amount <= 0` ออก

4. **Missing values**  
   พบ `currency` เป็น `NULL` ใน `order_id = 107` และ `116` และ `order_date` เป็น `NULL` ใน `order_id = 117`  
   **Handling:** Missing `currency` จะถูกถือว่าเป็น `USD` ตาม Assignment Requirement

5. **Inconsistent phone number formats**  
   `phone` มีหลายรูปแบบ เช่น `+1 (555) 123-4567`, `555-987-6543` และ `1234567890` ทำให้รูปแบบข้อมูลไม่สม่ำเสมอ  
   **Handling:** Remove non-numeric characters ออกจาก `phone`

6. **Incomplete exchange rate coverage**  
   Exchange Rate บางวันที่ไม่มีข้อมูลที่ตรงกับ `order_date` ของ Order  
   **Handling:** หากไม่พบ Exchange Rate จะถือว่า Order เป็น USD และใช้ Exchange Rate เท่ากับ `1.0` ตาม Assignment Requirement

## Part 2: Data Cleaning & ETL Pipeline

ETL Pipeline ถูก implement อยู่ในไฟล์ `pipeline.py` โดยใช้ Prefect สำหรับการ Orchestration

Pipeline ประกอบด้วย 3 ขั้นตอนหลัก:

- **Extract:** อ่านข้อมูลจาก `vw_raw_customers`, `vw_raw_orders` และ `vw_exchange_rates`
- **Transform:** Clean customer data, Filter invalid orders, Handle missing values และ Convert order amounts เป็น USD
- **Load:** เขียนข้อมูลที่ผ่านการ Clean แล้วลงใน `analytics.db` เป็นตาราง `dim_customers` และ `fct_orders`

Pipeline ใช้ Prefect `@task` และ `@flow` decorators รวมถึงมี Logging และ Error Handling ในแต่ละ Task

### Run the Pipeline

สร้างและ Activate Virtual Environment จากนั้นติดตั้ง Dependencies:

```bash
pip install -r requirements.txt
```

Run Pipeline:

```bash
python pipeline.py
```

เมื่อ Pipeline ทำงานสำเร็จ จะสร้าง:

```text
analytics.db
```

โดยมีผลลัพธ์:

```text
dim_customers = 10 records
fct_orders    = 17 records
```

## Part 3: Unit Testing

Unit Tests อยู่ในไฟล์ `test_pipeline.py` และใช้ `pytest` เป็น Testing Framework

Tests ถูกออกแบบให้ทดสอบ Transformation Logic แบบ Independent จาก Database โดยใช้ Dummy DataFrame แทนการเชื่อมต่อกับ Database จริง

ครอบคลุมการทดสอบ:

- Phone number standardization
- Customer deduplication และ data cleaning
- Order filtering และ currency conversion

### Run Tests

```bash
pytest
```

Expected result:

```text
3 passed
```

## Part 4: Analytical Query

SQL สำหรับคำนวณ Customer Lifetime Value (CLV) อยู่ในไฟล์ `clv_report.sql`

Query ใช้ข้อมูลจาก:

- `dim_customers`
- `fct_orders`

และแสดงผลลัพธ์ดังนี้:

- `customer_id`
- `full_name`
- `total_orders_placed`
- `lifetime_value_usd`
- `customer_cohort`

โดย `customer_cohort` แสดงเดือนและปีที่ลูกค้าสมัครในรูปแบบ `YYYY-MM`

ผลลัพธ์ถูกจัดเรียงตาม `lifetime_value_usd` จากมากไปน้อย

Customer ที่ไม่มี Valid Orders จะยังคงปรากฏใน Report โดยมี:

```text
total_orders_placed = 0
lifetime_value_usd = 0
```

### Run the CLV Report

หลังจาก Run ETL Pipeline แล้ว สามารถ Run:

```bash
sqlite3 analytics.db < clv_report.sql
```

## Running the Project

สามารถ Run โปรเจกต์ตามลำดับดังนี้:

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run ETL Pipeline

```bash
python pipeline.py
```

### 3. Run Unit Tests

```bash
pytest
```

### 4. Run CLV Report

```bash
sqlite3 analytics.db < clv_report.sql
```