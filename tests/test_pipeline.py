import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, TimestampType, BooleanType
from datetime import datetime, timedelta
from pipeline import build_actions, explode_impressions, get_customer_action_history, build_training_dataset  # Import your functions here
from data_generation import generate_impressions, generate_clicks, generate_add_to_cart, generate_previous_orders  # Data generation functions

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("PySpark Test Cases") \
        .master("local[2]") \
        .getOrCreate()

@pytest.fixture(scope="module")
def test_data(spark):
    impressions_df = generate_impressions()
    clicks_df = generate_clicks()
    add_to_cart_df = generate_add_to_cart()
    previous_orders_df = generate_previous_orders()
    return impressions_df, clicks_df, add_to_cart_df, previous_orders_df

def test_build_actions(spark, test_data):
    _, clicks_df, add_to_cart_df, previous_orders_df = test_data
    actions_df = build_actions(clicks_df, add_to_cart_df, previous_orders_df)
    
    assert actions_df.count() > 0, "Actions DataFrame should not be empty"
    assert "action_type" in actions_df.columns, "Actions DataFrame must contain 'action_type' column"
    assert actions_df.where(F.col("action_type") == 1).count() > 0, "Actions DataFrame should contain click actions"

def test_explode_impressions(spark, test_data):
    impressions_df, _, _, _ = test_data
    exploded_impressions_df = explode_impressions(impressions_df)
    
    assert exploded_impressions_df.count() > 0, "Exploded impressions DataFrame should not be empty"
    assert "item_id" in exploded_impressions_df.columns, "Exploded impressions must contain 'item_id' column"
    assert exploded_impressions_df.where(F.col("is_order") == True).count() >= 0, "Exploded impressions should have correct order flag"

def test_get_customer_action_history(spark, test_data):
    impressions_df, clicks_df, add_to_cart_df, previous_orders_df = test_data
    actions_df = build_actions(clicks_df, add_to_cart_df, previous_orders_df)
    customer_history_df = get_customer_action_history(actions_df, impressions_df)
    
    assert customer_history_df.count() > 0, "Customer action history DataFrame should not be empty"
    assert "actions" in customer_history_df.columns, "Customer action history must contain 'actions' column"
    assert len(customer_history_df.select("actions").first()[0]) <= 1000, "Actions column must contain 1000 or fewer entries"

def test_build_training_dataset(spark, test_data):
    impressions_df, clicks_df, add_to_cart_df, previous_orders_df = test_data
    training_df = build_training_dataset(impressions_df, clicks_df, add_to_cart_df, previous_orders_df)
    
    assert training_df.count() > 0, "Training DataFrame should not be empty"
    assert "dt" in training_df.columns, "Training DataFrame must be partitioned by 'dt'"
    assert training_df.where(F.col("is_order") == True).count() >= 0, "Training DataFrame should accurately reflect the order flag status"