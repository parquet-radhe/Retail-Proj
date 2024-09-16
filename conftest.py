### Insted of writing spark session again and again - spark = get_spark_session("LOCAL"), we can use fixtures
import pytest
from lib.Utils import get_spark_session
from pyspark.sql.functions import *

@pytest.fixture
def spark():
    #return get_spark_session("LOCAL")    ## This is not releasing the resources. We have ro release the resources also.and
    # Instead of just returning spark session we have to do below ops.
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_results(spark):
    "gives the expected result"
    result_schema = "state string, count int"
    return spark.read\
            .format("csv")\
            .schema(result_schema)\
            .load("data/test_results/state_aggregate.csv").orderBy(desc('count'),asc('state'))
