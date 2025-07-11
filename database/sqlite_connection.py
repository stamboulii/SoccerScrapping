import sqlite3
import pandas as pd
import json
from pathlib import Path
from typing import Optional
from loguru import logger
from config import DATABASE_PATH
from core.exceptions import DatabaseConnectionError

EXPORT_DIR = Path("data/exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

class SQLiteManager:
    def __init__(self, db_path: str = str(DATABASE_PATH)):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = None
        self.connect()

    def connect(self):
        try:
            self.connection = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite database: {e}")
            raise DatabaseConnectionError("Failed to connect to SQLite") from e

    def create_tables(self):
        schema_sql = """
        -- Countries table
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            code TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Competitions table
        CREATE TABLE IF NOT EXISTS competitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            country_id INTEGER,
            type TEXT,
            tier INTEGER,
            season TEXT,
            start_date DATE,
            end_date DATE,
            website_url TEXT,
            logo_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (country_id) REFERENCES countries(id)
        );

        -- Clubs table
        CREATE TABLE IF NOT EXISTS clubs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            short_name TEXT,
            country_id INTEGER,
            city TEXT,
            founded_year INTEGER,
            stadium_name TEXT,
            stadium_capacity INTEGER,
            website_url TEXT,
            logo_url TEXT,
            colors TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (country_id) REFERENCES countries(id)
        );

        -- Players table
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            age INTEGER,
            club TEXT,
            first_name TEXT,
            last_name TEXT,
            date_of_birth DATE,
            nationality_id INTEGER,
            height_cm INTEGER,
            weight_kg INTEGER,
            position TEXT,
            preferred_foot TEXT,
            photo_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (nationality_id) REFERENCES countries(id)
        );

        -- Club seasons
        CREATE TABLE IF NOT EXISTS club_seasons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            club_id INTEGER,
            player_id INTEGER,
            season TEXT,
            jersey_number INTEGER,
            position TEXT,
            is_active BOOLEAN DEFAULT 1,
            transfer_date DATE,
            transfer_fee REAL,
            contract_end_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (club_id) REFERENCES clubs(id),
            FOREIGN KEY (player_id) REFERENCES players(id),
            UNIQUE(club_id, player_id, season)
        );

        -- Matches table
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            competition_id INTEGER,
            home_club_id INTEGER,
            away_club_id INTEGER,
            match_date TIMESTAMP,
            matchday INTEGER,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT,
            attendance INTEGER,
            referee TEXT,
            stadium TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (competition_id) REFERENCES competitions(id),
            FOREIGN KEY (home_club_id) REFERENCES clubs(id),
            FOREIGN KEY (away_club_id) REFERENCES clubs(id)
        );

        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_players_name ON players(name);
        CREATE INDEX IF NOT EXISTS idx_clubs_name ON clubs(name);
        CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
        """
        try:
            cursor = self.connection.cursor()
            cursor.executescript(schema_sql)
            self.connection.commit()
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def execute_query(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        try:
            return pd.read_sql_query(query, self.connection, params=params)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        try:
            df.to_sql(table_name, self.connection, if_exists=if_exists, index=False)
            logger.info(f"Inserted {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error inserting DataFrame into {table_name}: {e}")
            raise

    def bulk_insert_players(self, players_data):
        try:
            with self.connection:
                self.connection.executemany(
                    "INSERT INTO players (name, age, club) VALUES (?, ?, ?)",
                    players_data
                )
            logger.info(f"✅ Bulk inserted {len(players_data)} players.")
        except Exception as e:
            logger.error(f"❌ Bulk insert failed: {e}")

    def get_table_stats(self) -> dict:
        tables = ['countries', 'competitions', 'clubs', 'players', 'matches']
        stats = {}
        for table in tables:
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cursor.fetchone()[0]
            except:
                stats[table] = 0
        return stats

    def export_table_to_csv(self, table_name: str):
        df = self.execute_query(f"SELECT * FROM {table_name}")
        if not df.empty:
            path = EXPORT_DIR / f"{table_name}.csv"
            df.to_csv(path, index=False)
            logger.info(f"✅ Exported {table_name} to CSV → {path}")
        else:
            logger.warning(f"⚠️ No data to export from {table_name}")

    def export_table_to_json(self, table_name: str):
        df = self.execute_query(f"SELECT * FROM {table_name}")
        if not df.empty:
            path = EXPORT_DIR / f"{table_name}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Exported {table_name} to JSON → {path}")
        else:
            logger.warning(f"⚠️ No data to export from {table_name}")

    def export_table_to_excel(self, table_name: str):
        df = self.execute_query(f"SELECT * FROM {table_name}")
        if not df.empty:
            path = EXPORT_DIR / f"{table_name}.xlsx"
            df.to_excel(path, index=False)
            logger.info(f"✅ Exported {table_name} to Excel → {path}")
        else:
            logger.warning(f"⚠️ No data to export from {table_name}")

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

# Global instance
db_manager = SQLiteManager()

def init_database():
    logger.info("Initializing SQLite database...")
    db_manager.create_tables()
    logger.info("Database initialization completed")

def generate_sample_data():
    logger.info("Generating sample data...")

    countries_df = pd.DataFrame([
        {'name': 'England', 'code': 'ENG'},
        {'name': 'Spain', 'code': 'ESP'},
        {'name': 'Germany', 'code': 'GER'},
        {'name': 'Italy', 'code': 'ITA'},
        {'name': 'France', 'code': 'FRA'},
        {'name': 'Brazil', 'code': 'BRA'},
        {'name': 'Argentina', 'code': 'ARG'},
    ])
    db_manager.insert_dataframe(countries_df, 'countries', if_exists='replace')

    competitions_df = pd.DataFrame([
        {'name': 'Premier League', 'country_id': 1, 'type': 'league', 'tier': 1, 'season': '2023-24'},
        {'name': 'La Liga', 'country_id': 2, 'type': 'league', 'tier': 1, 'season': '2023-24'},
        {'name': 'Bundesliga', 'country_id': 3, 'type': 'league', 'tier': 1, 'season': '2023-24'},
        {'name': 'Serie A', 'country_id': 4, 'type': 'league', 'tier': 1, 'season': '2023-24'},
        {'name': 'Ligue 1', 'country_id': 5, 'type': 'league', 'tier': 1, 'season': '2023-24'},
    ])
    db_manager.insert_dataframe(competitions_df, 'competitions', if_exists='replace')

    clubs_df = pd.DataFrame([
        {'name': 'Manchester City', 'short_name': 'Man City', 'country_id': 1, 'city': 'Manchester'},
        {'name': 'Real Madrid', 'short_name': 'Real Madrid', 'country_id': 2, 'city': 'Madrid'},
        {'name': 'Bayern Munich', 'short_name': 'Bayern', 'country_id': 3, 'city': 'Munich'},
        {'name': 'AC Milan', 'short_name': 'Milan', 'country_id': 4, 'city': 'Milan'},
        {'name': 'Paris Saint-Germain', 'short_name': 'PSG', 'country_id': 5, 'city': 'Paris'},
    ])
    db_manager.insert_dataframe(clubs_df, 'clubs', if_exists='replace')

    logger.info("Sample data generated successfully")
