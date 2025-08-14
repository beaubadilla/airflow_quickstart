from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import json
from nba_api.stats.endpoints import scoreboardv2
import pandas as pd
import os
import sqlite3

DATA_DIR = os.path.expanduser("~/airflow/data")
DB_PATH = os.path.join(DATA_DIR, "nba_data.db")
os.makedirs(DATA_DIR, exist_ok=True)

def check_for_games():
    """Return True if games were played yesterday, else False"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday = "2025-03-14"  # For testing purposes
    sb = scoreboardv2.ScoreboardV2(game_date=yesterday, day_offset=0)
    games_df = sb.game_header.get_data_frame()

    has_games = not games_df.empty
    if not has_games:
        print(f"No games found for {yesterday}. Skipping DB insert.")
    return has_games


def store_raw_and_processed_scores():
    """Fetch yesterday's scores, store raw JSON + processed table in SQLite"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday = "2025-03-14"  # For testing purposes
    sb = scoreboardv2.ScoreboardV2(game_date=yesterday, day_offset=0)

    # Raw JSON storage
    raw_data = sb.get_dict()

    # processed dataframe
    games_df = sb.game_header.get_data_frame()
    scores_df = sb.line_score.get_data_frame()

    # merge scores
    merged_games = []
    for _, game in games_df.iterrows():
        game_id = game["GAME_ID"]
        home_id = game["HOME_TEAM_ID"]
        visitor_id = game["VISITOR_TEAM_ID"]

        home_score = scores_df.loc[scores_df["TEAM_ID"] == home_id, "PTS"].values
        visitor_score = scores_df.loc[scores_df["TEAM_ID"] == visitor_id, "PTS"].values
        
        home_score = int(home_score[0]) if home_score[0] > 0 else None
        visitor_score = int(visitor_score[0]) if visitor_score[0] > 0 else None

        merged_games.append((
            game_id,
            yesterday,
            home_id,
            visitor_id,
            home_score,
            visitor_score
        ))

    # Connect to SQLite DB
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # create raw data table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_scoreboard (
        fetch_date TEXT,
        game_date TEXT,
        json_data TEXT
    )
    """)
    cursor.execute("""
    INSERT INTO raw_scoreboard (fetch_date, game_date, json_data)
    VALUES (?, ?, ?)
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        yesterday,
        json.dumps(raw_data)
    ))

    # Create processed table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS games (
        game_id TEXT PRIMARY_KEY,
        game_date TEXT,
        home_team_id INTEGER,
        visitor_team_id INTEGER,
        home_team_score INTEGER,
        visitor_team_score INTEGER
    )
    """)
    
    # insert or ignore dupes
    cursor.executemany("""
    INSERT OR IGNORE INTO games
    (game_id, game_date, home_team_id, visitor_team_id, home_team_score, visitor_team_score)
    VALUES (?, ?, ?, ?, ?, ?)
    """, merged_games)

    conn.commit()
    conn.close()

    print(f"Inserted raw + {len(merged_games)} processed game rows for {yesterday} into {DB_PATH}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1)
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
    check_games_task = ShortCircuitOperator(
        task_id="check_for_games",
        python_callable=check_for_games
    )

    store_scores_task = PythonOperator(
        task_id="store_raw_and_processed_scores",
        python_callable=store_raw_and_processed_scores,
    )
    check_games_task >> store_scores_task