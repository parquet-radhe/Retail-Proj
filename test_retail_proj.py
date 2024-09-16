import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config
from pyspark.sql.functions import *


@pytest.mark.skip("work in process")
def test_read_customers_df(spark):
    #spark = get_spark_session("LOCAL")                             # Need to use when not using fixture
    customers_count = read_customers(spark, "LOCAL").count()
    assert customers_count == 9426

@pytest.mark.skip("work in process")
def test_read_orders_df(spark):
    ##spark = get_spark_session("LOCAL")
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 9426

@pytest.mark.transformation
def test_filter_closed_orders(spark):
    ##spark = get_spark_session("LOCAL")
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 1881

def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"

@pytest.mark.transformation
def test_count_orders_state(spark,expected_results):
    customer_df = read_customers(spark,"LOCAL")
    actual_results = count_orders_state(customer_df).orderBy(desc('count'),asc('state'))
    assert actual_results.collect() == expected_results.collect() 

@pytest.mark.skip
def test_check_closed_count(spark):
        orders_df = read_orders(spark, "LOCAL")
        filtered_count = filter_orders_generic(orders_df, "CLOSED").count()
        assert filtered_count == 1881

@pytest.mark.parametrize(
    "status, count",
    [("CLOSED",1881),("COMPLETED",3775),("REJECTED",1926),("CANCELLED",1844)]

)
def test_check_count(spark,status,count):
        orders_df = read_orders(spark, "LOCAL")
        filtered_count = filter_orders_generic(orders_df, status).count()
        assert filtered_count == count