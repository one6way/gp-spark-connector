from pyspark.sql import SparkSession
from pyspark import SparkConf
import psycopg2
from typing import Dict, Optional

class GreenplumConnector:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        spark_master: str = "local[*]"
    ):
        """
        Initialize Greenplum connector with connection parameters
        
        Args:
            host: Greenplum host
            port: Greenplum port
            database: Database name
            user: Database user
            password: Database password
            spark_master: Spark master URL (default: local[*])
        """
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }
        
        # Initialize Spark session
        conf = SparkConf()
        conf.set("spark.jars", "postgresql-42.6.0.jar")
        
        self.spark = (SparkSession.builder
                     .appName("Greenplum-Spark-Connector")
                     .config(conf=conf)
                     .master(spark_master)
                     .getOrCreate())

    def read_table(self, table_name: str, schema: Optional[str] = "public") -> "pyspark.sql.DataFrame":
        """
        Read data from Greenplum table into Spark DataFrame
        
        Args:
            table_name: Name of the table to read
            schema: Database schema (default: public)
            
        Returns:
            Spark DataFrame containing the table data
        """
        jdbc_url = f"jdbc:postgresql://{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
        
        return (self.spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", f"{schema}.{table_name}")
                .option("user", self.connection_params['user'])
                .option("password", self.connection_params['password'])
                .option("driver", "org.postgresql.Driver")
                .load())

    def write_table(
        self,
        df: "pyspark.sql.DataFrame",
        table_name: str,
        schema: str = "public",
        mode: str = "overwrite"
    ) -> None:
        """
        Write Spark DataFrame to Greenplum table
        
        Args:
            df: Spark DataFrame to write
            table_name: Target table name
            schema: Database schema (default: public)
            mode: Write mode (append/overwrite) (default: overwrite)
        """
        jdbc_url = f"jdbc:postgresql://{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{schema}.{table_name}") \
            .option("user", self.connection_params['user']) \
            .option("password", self.connection_params['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()

    def execute_query(self, query: str) -> "pyspark.sql.DataFrame":
        """
        Execute custom SQL query and return results as Spark DataFrame
        
        Args:
            query: SQL query to execute
            
        Returns:
            Spark DataFrame with query results
        """
        jdbc_url = f"jdbc:postgresql://{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
        
        return (self.spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("query", query)
                .option("user", self.connection_params['user'])
                .option("password", self.connection_params['password'])
                .option("driver", "org.postgresql.Driver")
                .load())
