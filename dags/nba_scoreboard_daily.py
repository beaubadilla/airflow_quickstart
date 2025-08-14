from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from nba_api.stats.endpoints import scoreboardv2
import pandas as pd
import os
import sqlite3

DATA_DIR = os.path.expanduser("~/airflow/data")
DB_PATH = os.path.join(DATA_DIR, "nba_data.db")
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_yesterday_scores():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    sb = scoreboardv2.ScoreboardV2(game_date=yesterday, day_offset=0)
    games = sb.game_header.get_data_frame()

    if games_df.empty:
        print(f"No games found for {yesterday}. Skipping DB insert.")
        return

    # Connect to SQLite DB
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS games (
        game_id TEXT PRIMARY_KEY,
        game_date TEXT,
        home_team_id INTEGER,
        visitor_team_id INTEGER,
        home_team_score INTEGER,
        visitor_team_score INTEGER,
    )
    """)

    # Extract data from API response
    insert_data = []
    for _, row in games_df.iterrows():
        insert_data.append((
            row["GAME_ID"],
            yesterday,
            row["HOME_TEAM_ID"],
            row["VISITOR_TEAM_ID"],
            row["PTS_HOME"],
            row["PTS_VISITOR"],
        ))
    
    # insert or ignore dupes
    cursor.executemany("""
    INSERT OR IGNORE INTO games
    (game_id, game_date, home_team_id, visitor_team_id, home_team_score, visitor_team_score)
    VALUES (?, ?, ?, ?, ?, ?)
    """, insert_data)
    conn.commit()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "nba_daily_scoreboard",
    default_args=default_args,
    description="Fetch yesterday's NBA scores and save to SQLite",
    schedule_interval="0 6 * * *",  # everyday at 6 AM
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["nba", "quickstart"],
) as dag:
    fetch_scores_task = PythonOperator(
        task_id="fetch_and_store_scores",
        python_callable=fetch_yesterday_scores,
    )
    fetch_scores_task