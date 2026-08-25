from pathlib import Path
import re
import sqlite3

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
SOURCE_DB = BASE_DIR / "shopdata.db"
ANALYTICS_DB = BASE_DIR / "analytics.db"


# ---------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------

@task
def extract_customers(db_path: str) -> pd.DataFrame:
    """Extract raw customer data from the SQLite view."""
    logger = get_run_logger()

    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(
                "SELECT * FROM vw_raw_customers",
                conn,
            )

        logger.info("Extracted %d customer records.", len(df))
        return df

    except Exception:
        logger.exception("Failed to extract customer data.")
        raise


@task
def extract_orders(db_path: str) -> pd.DataFrame:
    """Extract raw order data from the SQLite view."""
    logger = get_run_logger()

    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(
                "SELECT * FROM vw_raw_orders",
                conn,
            )

        logger.info("Extracted %d order records.", len(df))
        return df

    except Exception:
        logger.exception("Failed to extract order data.")
        raise


@task
def extract_exchange_rates(db_path: str) -> pd.DataFrame:
    """Extract exchange rate data from the SQLite view."""
    logger = get_run_logger()

    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(
                "SELECT * FROM vw_exchange_rates",
                conn,
            )

        logger.info("Extracted %d exchange rate records.", len(df))
        return df

    except Exception:
        logger.exception("Failed to extract exchange rate data.")
        raise


# ---------------------------------------------------------------------
# Transform - Customers
# ---------------------------------------------------------------------

def standardize_phone(phone):
    """ Remove all non-numeric characters from a phone number."""
    if pd.isna(phone):
        return phone

    return re.sub(r"\D", "", str(phone))


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean customer data according to the assignment rules."""
    if df.empty:
        return df.copy()

    cleaned = df.copy()

    # Ensure signup_date is treated as a date for correct sorting.
    cleaned["signup_date"] = pd.to_datetime(
        cleaned["signup_date"],
        errors="coerce",
    )

    # Keep the most recent record for each customer.
    cleaned = (
        cleaned
        .sort_values(["customer_id", "signup_date"])
        .drop_duplicates(subset=["customer_id"], keep="last")
    )

    # Standardize phone numbers.
    cleaned["phone"] = cleaned["phone"].apply(standardize_phone)

    # Replace missing or blank emails.
    cleaned["email"] = (
        cleaned["email"]
        .replace(r"^\s*$", pd.NA, regex=True)
        .fillna("unknown@domain.com")
    )

    # Store dates in ISO format for SQLite consistency.
    cleaned["signup_date"] = cleaned["signup_date"].dt.strftime("%Y-%m-%d")

    return cleaned.reset_index(drop=True)


@task
def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Run customer transformation logic."""
    logger = get_run_logger()

    try:
        cleaned = clean_customers(df)

        logger.info(
            "Customer transformation complete: %d records after cleaning.",
            len(cleaned),
        )

        return cleaned

    except Exception:
        logger.exception("Failed to transform customer data.")
        raise


# ---------------------------------------------------------------------
# Transform - Orders
# ---------------------------------------------------------------------

def convert_orders_to_usd(
    orders_df: pd.DataFrame,
    rates_df: pd.DataFrame,
) -> pd.DataFrame:
    """Convert valid order amounts to USD."""
    if orders_df.empty:
        return orders_df.copy()

    orders = orders_df.copy()
    rates = rates_df.copy()

    # Remove system-error amounts.
    orders = orders[orders["total_amount"] > 0].copy()

    # Missing currency is assumed to be USD.
    orders["currency"] = (
        orders["currency"]
        .replace(r"^\s*$", pd.NA, regex=True)
        .fillna("USD")
        .str.upper()
    )

    # Convert dates to a consistent format for joining.
    orders["order_date"] = pd.to_datetime(
        orders["order_date"],
        errors="coerce",
    )

    rates["date"] = pd.to_datetime(
        rates["date"],
        errors="coerce",
    )

    # Rename to make the meaning explicit after the join.
    rates = rates.rename(
        columns={"rate_to_usd": "exchange_rate_to_usd"}
    )

    # Join exchange rates using both currency and order date.
    orders = orders.merge(
        rates[["currency", "date", "exchange_rate_to_usd"]],
        how="left",
        left_on=["currency", "order_date"],
        right_on=["currency", "date"],
    )

    # USD does not require an exchange rate.
    orders.loc[
        orders["currency"] == "USD",
        "exchange_rate_to_usd"
    ] = 1.0

    # Missing exchange rates are assumed to be USD
    # according to the assignment.
    orders["exchange_rate_to_usd"] = (
        orders["exchange_rate_to_usd"].fillna(1.0)
    )

    # Calculate the final USD amount.
    orders["usd_amount"] = (
        orders["total_amount"] * orders["exchange_rate_to_usd"]
    )

    # The helper join column is no longer needed.
    orders = orders.drop(columns=["date"])

    # Store date as ISO string for SQLite.
    orders["order_date"] = orders["order_date"].dt.strftime("%Y-%m-%d")

    return orders.reset_index(drop=True)


@task
def transform_orders(
    orders_df: pd.DataFrame,
    rates_df: pd.DataFrame,
) -> pd.DataFrame:
    """Run order cleaning and currency conversion."""
    logger = get_run_logger()

    try:
        cleaned = convert_orders_to_usd(
            orders_df,
            rates_df,
        )

        logger.info(
            "Order transformation complete: %d valid records.",
            len(cleaned),
        )

        return cleaned

    except Exception:
        logger.exception("Failed to transform order data.")
        raise


# ---------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------

@task
def load_data(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    db_path: str,
) -> None:
    """Load cleaned data into the analytics SQLite database."""
    logger = get_run_logger()

    try:
        with sqlite3.connect(db_path) as conn:
            customers_df.to_sql(
                "dim_customers",
                conn,
                if_exists="replace",
                index=False,
            )

            orders_df.to_sql(
                "fct_orders",
                conn,
                if_exists="replace",
                index=False,
            )

        logger.info(
            "Loaded %d customers into dim_customers.",
            len(customers_df),
        )
        logger.info(
            "Loaded %d orders into fct_orders.",
            len(orders_df),
        )
        logger.info("Analytics database created at %s", db_path)

    except Exception:
        logger.exception("Failed to load analytics data.")
        raise


# ---------------------------------------------------------------------
# Prefect Flow
# ---------------------------------------------------------------------

@flow(name="shopdata-etl")
def shopdata_etl(
    source_db: str = str(SOURCE_DB),
    analytics_db: str = str(ANALYTICS_DB),
) -> None:
    """Run the complete ShopData ETL pipeline."""

    customers = extract_customers(source_db)
    orders = extract_orders(source_db)
    exchange_rates = extract_exchange_rates(source_db)

    clean_customers_df = transform_customers(customers)

    clean_orders_df = transform_orders(
        orders,
        exchange_rates,
    )

    load_data(
        clean_customers_df,
        clean_orders_df,
        analytics_db,
    )


if __name__ == "__main__":
    shopdata_etl()
    