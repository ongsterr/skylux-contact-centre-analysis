import os
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


def get_connection():
    ctx = snowflake.connector.connect(
        account=os.getenv("SF_ACCOUNT"),
        # authenticator="oauth",
        token=os.getenv("SF_ACCESS_TOKEN"),
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_ACCESS_TOKEN"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        role=os.getenv("SF_ROLE"),
    )
    return ctx


def query_snowflake(query: str):
    warehouse = os.getenv("SF_WAREHOUSE")

    ctx = get_connection()
    cursor = ctx.cursor()
    cursor.execute(f"USE WAREHOUSE {warehouse}")
    cursor.execute(query)
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=colnames)

    cursor.close()
    ctx.close()

    return df


def load_into_snowflake(df: pd.DataFrame, table_name: str):
    conn = get_connection()

    df.columns = df.columns.str.upper()

    warehouse = os.getenv("SF_WAREHOUSE")
    database = os.getenv("SF_DATABASE")
    schema = os.getenv("SF_SCHEMA")

    conn.cursor().execute(f"USE WAREHOUSE {warehouse}")
    conn.cursor().execute(f"USE DATABASE {database}")
    conn.cursor().execute(f"USE SCHEMA {schema}")

    # Write DataFrame to existing table in Snowflake
    # Table must already exist
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        quote_identifiers=False,  # Set to True if your table name or columns have special chars
    )

    print(f"Data loaded successfully: {success}, Number of chunks: {nchunks}, Number of rows: {nrows}")
