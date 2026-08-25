import pandas as pd
import pytest

from pipeline import (
    standardize_phone,
    clean_customers,
    convert_orders_to_usd,
)


def test_standardize_phone():
    assert standardize_phone("(123) 456-7890") == "1234567890"
    assert standardize_phone("+66-81-234-5678") == "66812345678"
    assert standardize_phone(None) is None


def test_clean_customers():
    customers = pd.DataFrame(
        {
            "customer_id": [1, 1, 2],
            "full_name": ["Alice", "Alice Updated", "Bob"],
            "phone": ["(123) 456-7890", "123-999-8888", "081-222-3333"],
            "email": ["alice@example.com", " ", None],
            "signup_date": [
                "2023-01-10",
                "2023-02-15",
                "2023-03-20",
            ],
        }
    )

    result = clean_customers(customers)

    # เก็บเฉพาะข้อมูลล่าสุดของลูกค้าแต่ละราย
    assert len(result) == 2
    assert result["customer_id"].tolist() == [1, 2]

    # ตรวจสอบว่าข้อมูลของลูกค้ารายที่ 1 เป็นข้อมูลจากวันที่ล่าสุด
    customer_1 = result[result["customer_id"] == 1].iloc[0]
    assert customer_1["signup_date"] == "2023-02-15"
    assert customer_1["phone"] == "1239998888"

    # ตรวจสอบว่าอีเมลที่เป็นค่าว่างหรือ NULL ถูกแทนด้วยค่าเริ่มต้น
    customer_2 = result[result["customer_id"] == 2].iloc[0]
    assert customer_2["email"] == "unknown@domain.com"


def test_convert_orders_to_usd():
    orders = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "customer_id": [1, 1, 2, 2],
            "order_date": [
                "2023-01-10",
                "2023-01-10",
                "2023-01-11",
                "2023-01-11",
            ],
            "currency": ["USD", "EUR", None, "USD"],
            "total_amount": [100, 50, 20, -10],
        }
    )

    rates = pd.DataFrame(
        {
            "currency": ["EUR"],
            "date": ["2023-01-10"],
            "rate_to_usd": [1.1],
        }
    )

    result = convert_orders_to_usd(orders, rates)

    # ตรวจสอบว่า order ที่มีจำนวนเงินติดลบหรือเป็น system error ถูกลบออก
    assert len(result) == 3

    # ตรวจสอบว่า USD ไม่ต้องผ่านการแปลงค่าเงิน
    usd_order = result[result["order_id"] == 1].iloc[0]
    assert usd_order["usd_amount"] == 100

    # ตรวจสอบว่า EUR ถูกแปลงเป็น USD ด้วยอัตราแลกเปลี่ยนที่ถูกต้อง
    eur_order = result[result["order_id"] == 2].iloc[0]
    assert eur_order["usd_amount"] == pytest.approx(55)

    # ตรวจสอบว่าสกุลเงินที่ไม่มีค่าถูกกำหนดให้เป็น USD
    missing_currency_order = result[result["order_id"] == 3].iloc[0]
    assert missing_currency_order["currency"] == "USD"
    assert missing_currency_order["usd_amount"] == 20
    