from airflow import DAG
import pendulum
from datetime import datetime,timedelta
from api.video_stats import get_playlist_id,get_video_ids,extract_video_data,save_to_json
from datawarehouse.dwh_loading import staging_table,core_table
local_tz = pendulum.timezone("UTC")


default_args = {
    "owner":"dataengineer",
    "depends_on_past":False,
    "email_on_failure":False,
    "email_on_retry":False,
    "email":"kavish.mjn@yahoo.com",
    #"retries":1,
    #"retrY_delay":timedelta(minutes=5),
    "max_active_runs":1,
    "dagrun_timeout":timedelta(minutes=10),
    "start_date":datetime(2025,1,1 ,tzinfo=local_tz)
    }

with DAG(
    dag_id = 'produce_json',
    default_args=default_args,
    description='A simple DAG to extract youtube video stats and save it as json file',
    schedule='@daily',
    catchup=False
) as dag:
    #DEFINE TASKS
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json = save_to_json(video_data)

    #DEFINE DEPENDENCIES
    playlist_id >> video_ids >> video_data >> save_to_json



with DAG(
    dag_id = 'update_db',
    default_args=default_args,
    description='DAG to process json file,insert data in stage and core',
    schedule='@daily',
    catchup=False
) as dag:
    #DEFINE TASKS
    update_staging = staging_table()
    update_core = core_table()

    update_staging>>update_core
