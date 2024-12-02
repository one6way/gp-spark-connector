# Spark-ADB Connector v2.0

Расширенная версия коннектора для интеграции Apache Spark с AnalyticDB (ADB) и Greenplum.

## Новые возможности в версии 2.0

1. **Улучшенная производительность**:
   - Партиционирование при чтении/записи данных
   - Пакетная загрузка с настраиваемым размером
   - Кэширование часто используемых данных

2. **Безопасность**:
   - SSL-подключения
   - Шифрование учетных данных
   - Безопасное хранение конфигурации

3. **Мониторинг**:
   - Метрики производительности
   - Расширенное логирование
   - Отслеживание времени выполнения операций

4. **Отказоустойчивость**:
   - Автоматические повторные попытки при сбоях
   - Настраиваемое количество попыток
   - Логирование ошибок

## Требования

- Python 3.7+
- PySpark 3.4.1
- Дополнительные зависимости указаны в requirements.txt

## Установка

```bash
pip install -r requirements.txt
```

## Использование

```python
from adb_spark_connector import ADBSparkConnector

# Инициализация с расширенными настройками
connector = ADBSparkConnector(
    host="ваш_хост",
    port=5432,
    database="база_данных",
    user="пользователь",
    password="пароль",
    enable_ssl=True,
    enable_caching=True,
    batch_size=50000
)

# Чтение с партиционированием
df = connector.read_table(
    table_name="таблица",
    partition_column="id",
    where_clause="date >= '2023-01-01'"
)

# Запись с пакетной загрузкой
connector.write_table(
    df=processed_df,
    table_name="результаты",
    mode="append",
    batch_size=10000
)
```

## Отличия от версии 1.0

1. **Производительность**:
   - Добавлено партиционирование
   - Оптимизирована пакетная загрузка
   - Добавлено кэширование

2. **Безопасность**:
   - Добавлена поддержка SSL
   - Реализовано шифрование
   - Улучшена работа с учетными данными

3. **Мониторинг**:
   - Добавлены метрики
   - Расширено логирование
   - Добавлен трекинг производительности

4. **Надежность**:
   - Добавлены повторные попытки
   - Улучшена обработка ошибок
   - Добавлено восстановление после сбоев

## Примеры

Смотрите `example_usage_v2.py` для подробных примеров использования.

## Метрики производительности

Коннектор собирает следующие метрики:
- Количество операций чтения/записи
- Среднее время выполнения операций
- Статистика по ошибкам

## Конфигурация

Поддерживается настройка через:
- Параметры инициализации
- Конфигурационный файл (YAML)
- Переменные окружения

## Совместимость

Версия 2.0 полностью совместима с:
- AnalyticDB (ADB)
- Greenplum
- PostgreSQL
- Apache Spark 3.x