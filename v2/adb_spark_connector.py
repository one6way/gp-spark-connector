from pyspark.sql import SparkSession
from pyspark import SparkConf
import psycopg2
from typing import Dict, Optional, List, Union
from retry import retry
import structlog
from prometheus_client import Counter, Histogram
import yaml
from cryptography.fernet import Fernet
import json
from datetime import datetime

logger = structlog.get_logger()

class ADBSparkConnector:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        spark_master: str = "local[*]",
        batch_size: int = 10000,
        max_retries: int = 3,
        enable_ssl: bool = False,
        enable_caching: bool = True,
        config_file: Optional[str] = None
    ):
        """
        Расширенный коннектор для ADB/Greenplum с дополнительными возможностями
        
        Args:
            host: ADB/Greenplum хост
            port: Порт
            database: База данных
            user: Пользователь
            password: Пароль
            spark_master: Spark master URL
            batch_size: Размер пакета для загрузки
            max_retries: Максимальное количество попыток
            enable_ssl: Использовать SSL
            enable_caching: Включить кэширование
            config_file: Путь к файлу конфигурации
        """
        self.connection_params = self._encrypt_credentials({
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        })
        
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.enable_ssl = enable_ssl
        self.enable_caching = enable_caching
        
        # Загрузка конфигурации
        self.config = self._load_config(config_file) if config_file else {}
        
        # Инициализация метрик
        self.read_counter = Counter('adb_spark_reads_total', 'Total number of read operations')
        self.write_counter = Counter('adb_spark_writes_total', 'Total number of write operations')
        self.operation_latency = Histogram('adb_spark_operation_seconds', 'Operation latency in seconds')
        
        # Инициализация Spark с расширенными настройками
        conf = SparkConf()
        conf.set("spark.jars", "postgresql-42.6.0.jar")
        if enable_ssl:
            conf.set("spark.ssl.enabled", "true")
        
        self.spark = (SparkSession.builder
                     .appName("ADB-Spark-Connector-v2")
                     .config(conf=conf)
                     .master(spark_master)
                     .getOrCreate())
        
        logger.info("ADB Spark Connector initialized", 
                   host=host, 
                   database=database,
                   ssl_enabled=enable_ssl,
                   caching_enabled=enable_caching)

    def _encrypt_credentials(self, creds: Dict) -> Dict:
        """Шифрование учетных данных"""
        key = Fernet.generate_key()
        f = Fernet(key)
        return {k: f.encrypt(str(v).encode()).decode() for k, v in creds.items()}

    def _load_config(self, config_file: str) -> Dict:
        """Загрузка конфигурации из YAML файла"""
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    @retry(tries=3, delay=1, backoff=2)
    def read_table(
        self, 
        table_name: str, 
        schema: Optional[str] = "public",
        partition_column: Optional[str] = None,
        where_clause: Optional[str] = None
    ) -> "pyspark.sql.DataFrame":
        """
        Чтение данных из таблицы с поддержкой партиционирования
        
        Args:
            table_name: Имя таблицы
            schema: Схема
            partition_column: Колонка для партиционирования
            where_clause: Условие WHERE
        """
        start_time = datetime.now()
        try:
            jdbc_url = f"jdbc:postgresql://{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
            
            read_options = {
                "url": jdbc_url,
                "dbtable": f"{schema}.{table_name}",
                "user": self.connection_params['user'],
                "password": self.connection_params['password'],
                "driver": "org.postgresql.Driver",
                "fetchsize": str(self.batch_size)
            }
            
            if partition_column:
                read_options["partitionColumn"] = partition_column
                read_options["lowerBound"] = "0"
                read_options["upperBound"] = "1000000"
                read_options["numPartitions"] = "10"

            df = self.spark.read.format("jdbc").options(**read_options)
            
            if where_clause:
                df = df.filter(where_clause)

            if self.enable_caching:
                df = df.cache()

            self.read_counter.inc()
            return df.load()

        except Exception as e:
            logger.error("Error reading table", 
                        table=table_name, 
                        schema=schema, 
                        error=str(e))
            raise
        finally:
            duration = (datetime.now() - start_time).total_seconds()
            self.operation_latency.observe(duration)

    def write_table(
        self,
        df: "pyspark.sql.DataFrame",
        table_name: str,
        schema: str = "public",
        mode: str = "overwrite",
        batch_size: Optional[int] = None
    ) -> None:
        """
        Запись DataFrame в таблицу с поддержкой пакетной загрузки
        
        Args:
            df: Spark DataFrame
            table_name: Имя таблицы
            schema: Схема
            mode: Режим записи (append/overwrite)
            batch_size: Размер пакета
        """
        start_time = datetime.now()
        try:
            jdbc_url = f"jdbc:postgresql://{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
            
            write_options = {
                "url": jdbc_url,
                "dbtable": f"{schema}.{table_name}",
                "user": self.connection_params['user'],
                "password": self.connection_params['password'],
                "driver": "org.postgresql.Driver",
                "batchsize": str(batch_size or self.batch_size)
            }

            df.write \
                .format("jdbc") \
                .options(**write_options) \
                .mode(mode) \
                .save()

            self.write_counter.inc()

        except Exception as e:
            logger.error("Error writing table", 
                        table=table_name, 
                        schema=schema, 
                        error=str(e))
            raise
        finally:
            duration = (datetime.now() - start_time).total_seconds()
            self.operation_latency.observe(duration)

    def execute_query(
        self, 
        query: str,
        cache_result: bool = False
    ) -> "pyspark.sql.DataFrame":
        """
        Выполнение произвольного SQL-запроса
        
        Args:
            query: SQL-запрос
            cache_result: Кэшировать результат
        """
        start_time = datetime.now()
        try:
            jdbc_url = f"jdbc:postgresql://{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
            
            df = (self.spark.read
                  .format("jdbc")
                  .option("url", jdbc_url)
                  .option("query", query)
                  .option("user", self.connection_params['user'])
                  .option("password", self.connection_params['password'])
                  .option("driver", "org.postgresql.Driver")
                  .load())

            if cache_result and self.enable_caching:
                df = df.cache()

            return df

        except Exception as e:
            logger.error("Error executing query", 
                        query=query, 
                        error=str(e))
            raise
        finally:
            duration = (datetime.now() - start_time).total_seconds()
            self.operation_latency.observe(duration)

    def get_metrics(self) -> Dict:
        """Получение метрик производительности"""
        return {
            "reads": self.read_counter._value.get(),
            "writes": self.write_counter._value.get(),
            "avg_latency": self.operation_latency.observe()._sum.get() / self.operation_latency.observe()._count.get()
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.spark:
            self.spark.stop()
