# Greenplum-PySpark Connector

A Python connector for seamless data integration between Greenplum (ADB) and PySpark.

## Features

- Easy-to-use interface for reading and writing data between Greenplum and PySpark
- Support for custom SQL queries
- Configurable connection parameters
- Type-safe implementation with proper error handling

## Requirements

- Python 3.7+
- PySpark 3.4.1
- psycopg2-binary 2.9.9
- pandas 2.1.1
- PostgreSQL JDBC driver (postgresql-42.6.0.jar)

## Installation

1. Clone the repository
2. Install the required dependencies:
```bash
pip install -r requirements.txt
```
3. Download PostgreSQL JDBC driver:
```bash
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
```

## Usage

```python
from greenplum_connector import GreenplumConnector

# Initialize connector
connector = GreenplumConnector(
    host="your_greenplum_host",
    port=5432,
    database="your_database",
    user="your_username",
    password="your_password"
)

# Read data from Greenplum table
df = connector.read_table("example_table")

# Write data to Greenplum
connector.write_table(df, "output_table", mode="overwrite")

# Execute custom query
result_df = connector.execute_query("SELECT * FROM example_table WHERE column > 100")
```

See `example_usage.py` for more detailed examples.

## Configuration

The connector supports the following configuration parameters:

- `host`: Greenplum host address
- `port`: Greenplum port (default: 5432)
- `database`: Database name
- `user`: Database username
- `password`: Database password
- `spark_master`: Spark master URL (default: "local[*]")

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.
