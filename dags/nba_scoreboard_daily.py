from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from nba_api.stats.endpoints import scoreboardv2
import pandas as pd
import os

DATA_DIR = os.path.expanduser("~/airflow/data")
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_yesterday_scores():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    sb = scoreboardv2.ScoreboardV2(game_data=yesterday, day_offset=0)
    games = sb.game_header.get_data_frame()

    output_path = os.path.join(DATA_DIR, "yesterday_scores.csv")
    games.to_csv(output_path, index=False)
    print(f"Saved {len(games)} games to {output_path}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "nba_daily_scoreboard",
    default_args=default_args,
    description="Fetch yesterday's NBA scores and save to CSV",
    schedule_interval="0 6 * * *",  # everyday at 6 AM
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["nba", "quickstart"],
) as dag:
    fetch_scores_task = PythonOperator(
        task_id="fetch_scores",
        python_callable=fetch_yesterday_scores,
    )
    fetch_scores_task