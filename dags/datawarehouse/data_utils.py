from airflow.providers.postgres.hooks.postgres import PostgresHook

from psycopg2.extras import RealDictCursor

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="POSTGRES_DB_YT_ELT"
                        ,database = "elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn,cur


def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()


def create_schema(schema):
    conn,cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"

    cur.execute(schema_sql)
    conn.commit()

    close_conn_cursor(conn,cur)


def create_table(schema,table):
    conn,cur = get_conn_cursor()
    if schema == "staging":
        table_sql = f"""
                       CREATE TABLE IF NOT EXISTS {schema}.{table}(
                       "Video_ID" VARCHAR(255) PRIMARY KEY,
                       "Video_Title" VARCHAR(255) NOT NULL,
                       "Upload_Date" TIMESTAMP NOT NULL,
                       "Duration" VARCHAR(20) NOT NULL,
                       "Video_Views" INT,
                       "Likes_Count" INT,
                       "Comments_Count" INT 
                       );
                    """
    else:
        table_sql = f"""
                       CREATE TABLE IF NOT EXISTS {schema}.{table}(
                       "Video_ID" VARCHAR(255) PRIMARY KEY,
                       "Video_Title" VARCHAR(255) NOT NULL,
                       "Upload_Date" TIMESTAMP NOT NULL,
                       "Duration" TIME NOT NULL,
                       "Video_Type" VARCHAR(10) NOT NULL,
                       "Video_Views" INT,
                       "Likes_Count" INT,
                       "Comments_Count" INT 
                       );
                    """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn,cur)

def get_video_ids(cur,schema,table):
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()
    video_ids = [row["Video_ID"] for row in ids]
    return video_ids