from adb_spark_connector import ADBSparkConnector

# Пример использования расширенной версии коннектора
connector = ADBSparkConnector(
    host="your_adb_host",
    port=5432,
    database="your_database",
    user="your_username",
    password="your_password",
    enable_ssl=True,
    enable_caching=True,
    batch_size=50000
)

# Чтение данных с партиционированием
df = connector.read_table(
    table_name="large_table",
    partition_column="id",
    where_clause="date >= '2023-01-01'"
)

# Обработка данных с помощью Spark
processed_df = df.filter(df.amount > 1000) \
                 .groupBy("category") \
                 .agg({"amount": "sum"})

# Запись результатов с пакетной загрузкой
connector.write_table(
    df=processed_df,
    table_name="results_table",
    mode="append",
    batch_size=10000
)

# Выполнение сложного SQL-запроса с кэшированием
query = """
    WITH monthly_stats AS (
        SELECT 
            date_trunc('month', date) as month,
            category,
            SUM(amount) as total_amount,
            COUNT(*) as transactions
        FROM large_table
        WHERE date >= '2023-01-01'
        GROUP BY 1, 2
    )
    SELECT 
        month,
        category,
        total_amount,
        transactions,
        total_amount / transactions as avg_transaction
    FROM monthly_stats
    WHERE total_amount > 1000000
    ORDER BY month, total_amount DESC
"""
result_df = connector.execute_query(query, cache_result=True)

# Получение метрик производительности
metrics = connector.get_metrics()
print("Performance metrics:", metrics)
