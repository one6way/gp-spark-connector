# Spark-ADB/Greenplum Connector

Коннектор для интеграции Apache Spark с AnalyticDB (ADB) и Greenplum. Позволяет эффективно читать данные из ADB/Greenplum в Spark, выполнять обработку и записывать результаты обратно.

## О коннекторе

Этот коннектор разработан специально для:
- Работы с Alibaba Cloud AnalyticDB for PostgreSQL (ADB)
- Работы с Greenplum
- Интеграции с Apache Spark для распределенной обработки данных

Основные сценарии использования:
- ETL процессы с использованием Spark и ADB/Greenplum
- Аналитика больших данных
- Пакетная обработка данных
- Интеграция данных между различными системами

## Возможности

- Простой интерфейс для чтения и записи данных между ADB/Greenplum и PySpark
- Поддержка пользовательских SQL-запросов
- Настраиваемые параметры подключения
- Типобезопасная реализация с обработкой ошибок
- Поддержка параллельной обработки данных

## Требования

- Python 3.7+
- PySpark 3.4.1
- psycopg2-binary 2.9.9
- pandas 2.1.1
- PostgreSQL JDBC драйвер (postgresql-42.6.0.jar)

## Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/one6way/gp-spark-connector.git
```

2. Установите зависимости:
```bash
pip install -r requirements.txt
```

3. Скачайте PostgreSQL JDBC драйвер:
```bash
curl -O https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
```

## Использование

```python
from greenplum_connector import GreenplumConnector

# Инициализация коннектора
connector = GreenplumConnector(
    host="ваш_хост_greenplum",
    port=5432,
    database="ваша_база_данных",
    user="пользователь",
    password="пароль"
)

# Чтение данных из таблицы Greenplum
df = connector.read_table("имя_таблицы")

# Запись данных в Greenplum
connector.write_table(df, "выходная_таблица", mode="overwrite")

# Выполнение произвольного SQL-запроса
result_df = connector.execute_query("SELECT * FROM таблица WHERE условие > 100")
```

Более подробные примеры можно найти в файле `example_usage.py`.

## Конфигурация

Коннектор поддерживает следующие параметры:

- `host`: Адрес хоста Greenplum
- `port`: Порт Greenplum (по умолчанию: 5432)
- `database`: Имя базы данных
- `user`: Имя пользователя
- `password`: Пароль
- `spark_master`: URL мастер-узла Spark (по умолчанию: "local[*]")

## Как это работает

1. **Чтение данных**:
   - Формируется JDBC URL для подключения к Greenplum
   - Данные загружаются в Spark DataFrame через JDBC
   - Поддерживается параллельное чтение для повышения производительности

2. **Обработка данных**:
   - Данные обрабатываются с использованием всех возможностей Spark
   - Поддерживаются все операции Spark DataFrame (фильтрация, группировка, агрегация)

3. **Запись данных**:
   - Результаты обработки записываются обратно в Greenplum
   - Поддерживаются режимы append (добавление) и overwrite (перезапись)

## Примеры использования

### Базовый пример:
```python
# Чтение данных
df = connector.read_table("sales_data")

# Обработка
processed_df = df.filter(df.amount > 1000) \
                 .groupBy("category") \
                 .agg({"amount": "sum"})

# Запись результатов
connector.write_table(processed_df, "sales_summary")
```

### Пример с SQL-запросом:
```python
query = """
    SELECT 
        category,
        SUM(amount) as total_amount,
        COUNT(*) as transactions
    FROM sales_data
    WHERE date >= '2023-01-01'
    GROUP BY category
    HAVING SUM(amount) > 1000000
"""
result_df = connector.execute_query(query)
```

## Производительность

- Параллельная обработка данных с помощью Spark
- Эффективная передача данных через JDBC
- Оптимизированные операции чтения и записи

## Участие в разработке

Если у вас есть предложения по улучшению коннектора или вы нашли ошибку:
1. Создайте Issue в репозитории
2. Предложите Pull Request с исправлениями
3. Опишите подробно предлагаемые изменения

## Лицензия

MIT License
