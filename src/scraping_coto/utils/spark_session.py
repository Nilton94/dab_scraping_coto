from pyspark.sql import SparkSession

def get_spark() -> SparkSession:
    """
        Create a new Databricks Connect session.
    """
    
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    
    except ImportError:
        return SparkSession.builder.getOrCreate()