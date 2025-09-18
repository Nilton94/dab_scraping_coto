import pyspark
from .spark_session import get_spark
from pyspark.dbutils import DBUtils

def get_dbutils():
    
    dbutils = DBUtils(get_spark())
    return dbutils