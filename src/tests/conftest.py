#https://stackoverflow.com/questions/40975360/testing-spark-with-pytest-cannot-run-spark-in-local-mode
import pytest
from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os

os.environ["SPARK_HOME"]="C:\spark-2.2.0-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\winutils"

@pytest.fixture(scope="session")
def spark_context(request):
    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
    request.addfinalizer(lambda: sc.stop())
    sc = SparkContext(conf=conf)
    return sc

@pytest.fixture(scope="session")
def hive_context(spark_context):
    return HiveContext(spark_context)

@pytest.fixture(scope="session")
def streaming_context(spark_context):
    return StreamingContext(spark_context, 1)