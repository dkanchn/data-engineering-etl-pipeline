-- ดูรายชื่อคอลัมน์และชนิดข้อมูลของแต่ละ view
PRAGMA table_info(vw_raw_customers);
PRAGMA table_info(vw_raw_orders);
PRAGMA table_info(vw_exchange_rates);

-- หาจำนวนแถวของแต่ละตาราง เพื่อรู้ขนาดข้อมูลคร่าวๆ 
SELECT 'vw_raw_customers' AS table_name, COUNT(*) AS row_count
FROM vw_raw_customers

UNION ALL

SELECT 'vw_raw_orders' AS table_name, COUNT(*) AS row_count
FROM vw_raw_orders

UNION ALL

SELECT 'vw_exchange_rates' AS table_name, COUNT(*) AS row_count
FROM vw_exchange_rates;

-- ดูข้อมูลดิบ เพื่อให้เห็นภาพรวมของข้อมูล
SELECT * FROM vw_raw_customers;
SELECT * FROM vw_raw_orders;
SELECT * FROM vw_exchange_rates;

-- ---------------------------------------------------------------------
-- หลังจาก query ดู raw data จะพบความผิดปกติได้ทันที เนื่องจากข้อมูลน้อย 
-- เช่น missing values, duplicate rows, และ format ของข้อมูลไม่สม่ำเสมอ
-- ---------------------------------------------------------------------

-- ---------------------------------------------------------------------
-- ตรวจเช็คค่าที่หายไป (missing values) ในแต่ละตาราง
-- ---------------------------------------------------------------------

-- ดูแถวจริงที่มีฟิลด์สำคัญเป็น NULL (ฝั่ง orders)
SELECT * FROM vw_raw_orders
WHERE order_date IS NULL OR currency IS NULL OR total_amount IS NULL;

-- ดูแถวจริงที่ข้อมูลติดต่อหายไป (ฝั่ง customers)
SELECT * FROM vw_raw_customers
WHERE email IS NULL OR phone IS NULL;

-- ---------------------------------------------------------------------
-- ตรวจสอบ duplicate rows 
-- ---------------------------------------------------------------------

-- customer_id เดียวกัน แต่ข้อมูลในแถวไม่เหมือนกัน -> อาจเกิดจากการลงทะเบียนซ้ำ หรือระบบ insert แถวใหม่แทนที่จะ update
SELECT *
FROM vw_raw_customers
WHERE customer_id IN (
    SELECT customer_id FROM vw_raw_customers
    GROUP BY customer_id HAVING COUNT(*) > 1
)
ORDER BY customer_id;

-- order_id ซ้ำ (ตามหลักควรเป็น primary key ที่ไม่ซ้ำกัน)
SELECT order_id, COUNT(*) AS occurrences
FROM vw_raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- email ซ้ำ (ต่างจากการเช็ค customer_id ซ้ำ เพราะคนละ customer_id
-- แต่ email เดียวกันก็เป็นปัญหาได้เช่นกัน)
SELECT email, COUNT(*) AS occurrences
FROM vw_raw_customers
WHERE email IS NOT NULL
GROUP BY email
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------
-- ตรวจสอบค่าข้อมูลที่ผิดปกติ
-- ---------------------------------------------------------------------

-- คำสั่งซื้อที่มียอดเงินติดลบหรือเป็นศูนย์ (ในความเป็นจริงไม่น่าจะเกิดขึ้น)
SELECT * FROM vw_raw_orders
WHERE total_amount <= 0;

-- เบอร์โทรศัพท์ที่มี format ไม่สม่ำเสมอ
SELECT DISTINCT phone FROM vw_raw_customers;

-- ดูค่า status ทั้งหมดที่มีในระบบ เพื่อหาค่าที่ไม่คาดคิด/ค่าที่บ่งบอกข้อผิดพลาด
SELECT status, COUNT(*) AS n
FROM vw_raw_orders
GROUP BY status
ORDER BY n DESC;

-- ดูสกุลเงิน (currency) ทั้งหมดที่ถูกใช้
SELECT currency, COUNT(*) AS n
FROM vw_raw_orders
GROUP BY currency
ORDER BY n DESC;

-- orders ที่อ้างอิง customer_id ซึ่งไม่มีอยู่จริงในตาราง customers
SELECT o.order_id, o.customer_id
FROM vw_raw_orders o
LEFT JOIN vw_raw_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- ---------------------------------------------------------------------
-- ตรวจสอบความสอดคล้องกับตาราง exchange rate
-- ---------------------------------------------------------------------

-- ดูช่วงวันที่ของ orders เทียบกับช่วงวันที่ที่มีอัตราแลกเปลี่ยนให้
SELECT MIN(order_date) AS orders_min_date, MAX(order_date) AS orders_max_date
FROM vw_raw_orders;

SELECT MIN(date) AS rates_min_date, MAX(date) AS rates_max_date
FROM vw_exchange_rates;

-- orders ที่เป็นสกุลเงินอื่นนอกจาก USD แต่ไม่มีอัตราแลกเปลี่ยนตรงกับ order_date
-- (ทำให้แปลงยอดเงินเป็นสกุลกลาง เช่น USD ไม่ได้)
SELECT o.order_id, o.order_date, o.currency
FROM vw_raw_orders o
LEFT JOIN vw_exchange_rates r
       ON o.currency = r.currency
      AND o.order_date = r.date
WHERE o.currency IS NOT NULL
  AND o.currency <> 'USD'
  AND r.currency IS NULL;
  