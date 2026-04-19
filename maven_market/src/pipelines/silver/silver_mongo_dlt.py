


import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, current_timestamp

# ── Step 1: Hardcoded config for verification ────────────────────────────────
CATALOG       = "maven_market_uc"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
ENV           = "dev"

# ── JSON Schemas for the "data" column (Fivetran connector) ──
# Used only when Bronze has not already flattened the document.
CUSTOMERS_JSON_SCHEMA = """
    STRUCT<
        customer_id:             INT,
        customer_acct_num:       STRING,
        first_name:              STRING,
        last_name:               STRING,
        email_address:           STRING,
        customer_address:        STRING,
        customer_city:           STRING,
        customer_state_province: STRING,
        customer_postal_code:    STRING,
        customer_country:        STRING,
        birthdate:               STRING,
        marital_status:          STRING,
        yearly_income:           STRING,
        gender:                  STRING,
        total_children:          INT,
        num_children_at_home:    INT,
        education:               STRING,
        acct_open_date:          STRING,
        member_card:             STRING,
        occupation:              STRING,
        homeowner:               STRING
    >
"""

PRODUCTS_JSON_SCHEMA = """
    STRUCT<
        product_id:           INT,
        product_brand:        STRING,
        product_name:         STRING,
        product_sku:          LONG,
        product_retail_price: DOUBLE,
        product_cost:         DOUBLE,
        product_weight:       DOUBLE,
        recyclable:           INT,
        low_fat:              INT
    >
"""



# 1. CUSTOMERS  
#

@dlt.view(name="customers_parsed_vw")
@dlt.expect_or_fail("valid_customer_pk",  "customer_id IS NOT NULL")
@dlt.expect(        "has_email",           "email_address IS NOT NULL")
@dlt.expect(        "valid_country",       "customer_country IS NOT NULL")
@dlt.expect(        "valid_gender",        "gender IN ('M', 'F')")
def customers_parsed_vw():
    df = spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")

    # Auto-detect: Fivetran generic connector writes a nested "data" JSON column.
    # A custom connector may have already flattened the document — detect and adapt.
    if "data" in df.columns and "customer_id" not in df.columns:
        df = (
            df.withColumn("_parsed", F.from_json(col("data"), CUSTOMERS_JSON_SCHEMA))
              .select("_parsed.*", "ingestion_time")
        )

    return (
        df.select(
            col("customer_id").cast("int").alias("customer_id"),
            col("customer_acct_num"),
            col("first_name"),
            col("last_name"),
            # Derived full_name – used by the name-masking UDF in governance layer
            F.concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            # ── PII columns (masked at governance view layer) ──────
            col("email_address"),
            col("customer_address"),
            col("customer_city"),
            col("customer_state_province"),
            col("customer_postal_code"),
            col("customer_country"),
            to_date(col("birthdate"),      "M/d/yyyy").alias("birthdate"),
            to_date(col("acct_open_date"), "M/d/yyyy").alias("acct_open_date"),
            # ── SCD-2 tracked (mutable) attributes ────────────────
            col("marital_status"),
            col("yearly_income"),
            col("member_card"),
            col("occupation"),
            col("homeowner"),
            # ── Stable (non-tracked) attributes ───────────────────
            col("gender"),
            col("total_children").cast("int").alias("total_children"),
            col("num_children_at_home").cast("int").alias("num_children_at_home"),
            col("education"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            F.lit(ENV).alias("environment"),
        )
    )


dlt.create_streaming_table(
    name="customers",
    comment=(
        "SCD Type-2 customer dimension (PII-bearing). "
        "Tracks changes to: email_address, customer_address, marital_status, "
        "member_card, yearly_income, occupation, homeowner. "
        "Analysts and regional managers MUST query silver.vw_customers_masked, "
        "not this base table directly."
    ),
    table_properties={
        "layer":        "silver",
        "domain":       "customers",
        "contains_pii": "true",
        "scd_type":     "2",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
)

dlt.apply_changes(
    target             = "customers",
    source             = "customers_parsed_vw",
    keys               = ["customer_id"],
    sequence_by        = col("bronze_ingestion_time"),
    stored_as_scd_type = 2,
    track_history_column_list = [
        "email_address",
        "customer_address",
        "marital_status",
        "member_card",
        "yearly_income",
        "occupation",
        "homeowner",
    ],
)



# 2. PRODUCTS  (Dimension – SCD Type-2)


@dlt.view(name="products_parsed_vw")
@dlt.expect_or_fail("valid_product_pk",   "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_retail_price", "product_retail_price > 0")
@dlt.expect_or_drop("valid_cost",         "product_cost > 0")
@dlt.expect(        "price_above_cost",   "product_retail_price > product_cost")
def products_parsed_vw():
    df = spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.products")

    if "data" in df.columns and "product_id" not in df.columns:
        df = (
            df.withColumn("_parsed", F.from_json(col("data"), PRODUCTS_JSON_SCHEMA))
              .select("_parsed.*", "ingestion_time")
        )

    return (
        df.select(
            col("product_id").cast("int").alias("product_id"),
            col("product_brand"),
            col("product_name"),
            col("product_sku").cast("long").alias("product_sku"),
            col("product_retail_price").cast("double").alias("product_retail_price"),
            col("product_cost").cast("double").alias("product_cost"),
            col("product_weight").cast("double").alias("product_weight"),
            col("recyclable").cast("boolean").alias("recyclable"),
            col("low_fat").cast("boolean").alias("low_fat"),
            # Derived margin – informational metric, not a DQ gate
            F.round(
                (col("product_retail_price").cast("double") - col("product_cost").cast("double"))
                / col("product_retail_price").cast("double") * 100,
                2,
            ).alias("margin_pct"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            F.lit(ENV).alias("environment"),
        )
    )


dlt.create_streaming_table(
    name="products",
    comment=(
        "SCD Type-2 product dimension. "
        "Tracks changes to: product_retail_price, product_cost, recyclable, low_fat."
    ),
    table_properties={
        "layer":        "silver",
        "domain":       "products",
        "contains_pii": "false",
        "scd_type":     "2",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
)

dlt.apply_changes(
    target             = "products",
    source             = "products_parsed_vw",
    keys               = ["product_id"],
    sequence_by        = col("bronze_ingestion_time"),
    stored_as_scd_type = 2,
    track_history_column_list = [
        "product_retail_price",
        "product_cost",
        "recyclable",
        "low_fat",
    ],
)
