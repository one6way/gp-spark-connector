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

# Perform Spark transformations
transformed_df = df.filter(df.column > 100).select("column1", "column2")

# Write transformed data back to Greenplum
connector.write_table(transformed_df, "transformed_table", mode="overwrite")

# Execute custom query
query = """
    SELECT column1, column2
    FROM example_table
    WHERE column3 > 100
    GROUP BY column1, column2
"""
result_df = connector.execute_query(query)
