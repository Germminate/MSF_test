# Databricks notebook source
from pyspark.sql.functions import year, sum as _sum, row_number, count
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Customer Spending Habits Analysis
# Load the tables
transactions = spark.table("customer.transaction.fact_transaction_data")
membership = spark.table("customer.customer_information.dim_membership_details")

# Join transactions with membership details
customer_spending_habits = (
    transactions
    .join(membership, "customer_id", "inner")
    .select(
        "customer_id",
        "payment_method",
        "membership_tier",
        "membership_tier_last_updated",
        "tier_history_membership_tier",
        "tier_history_highest_spend_category",
        "highest_spend_category",
        "total_amount",
        "transaction_datetime"
    )
)

# Aggregate spending
customer_summary = (
    customer_spending_habits
    .groupBy(
        "customer_id",
        "payment_method",
        "membership_tier",
        "highest_spend_category"
    )
    .agg(
        _sum("total_amount").alias("total_spent"),
        count("*").alias("transaction_count")
    )
    .orderBy("total_spent", ascending=False)
)

# COMMAND ----------

mart_path = "customer.customer_information.mart_customer_spending_summary"

customer_summary.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable(mart_path)

# COMMAND ----------


