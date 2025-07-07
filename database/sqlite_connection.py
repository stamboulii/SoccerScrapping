import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional
from loguru import logger

class SQLiteManager:
    def __init__(self, db_path: str = "data/soccer.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = None
        self.connect()
    
    def connect(self):
        """Connect to SQLite database"""
        try:
            self.connection = sqlite3.connect(str(self.db_path))
            self.connection.row_factory = sqlite3.Row  # Enable column access by name
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite database: {e}")
            raise
    
    def create_tables(self):
        """Create all tables using SQLite-compatible schema"""
        schema_sql = """
        -- Countries table
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            code TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Competitions/Leagues table
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
            colors TEXT, -- JSON string for colors
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (country_id) REFERENCES countries(id)
        );

        -- Players table
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
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

        -- Create indexes
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
        """Execute a query and return results as pandas DataFrame"""
        try:
            return pd.read_sql_query(query, self.connection, params=params)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """Insert pandas DataFrame into database table"""
        try:
            df.to_sql(table_name, self.connection, if_exists=if_exists, index=False)
            logger.info(f"Inserted {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error inserting DataFrame into {table_name}: {e}")
            raise
    
    def get_table_stats(self) -> dict:
        """Get row counts for all tables"""
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
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

# Global database manager instance
db_manager = SQLiteManager()

def init_database():
    """Initialize database - create tables and test connection"""
    logger.info("Initializing SQLite database...")
    db_manager.create_tables()
    logger.info("Database initialization completed")

def generate_sample_data():
    """Generate sample data for testing"""
    logger.info("Generating sample data...")
    
    # Sample countries
    countries_data = [
        {'name': 'England', 'code': 'ENG'},
        {'name': 'Spain', 'code': 'ESP'},
        {'name': 'Germany', 'code': 'GER'},
        {'name': 'Italy', 'code': 'ITA'},
        {'name': 'France', 'code': 'FRA'},
        {'name': 'Brazil', 'code': 'BRA'},
        {'name': 'Argentina', 'code': 'ARG'},
    ]
    
    countries_df = pd.DataFrame(countries_data)
    db_manager.insert_dataframe(countries_df, 'countries', if_exists='replace')
    
    # Sample competitions
    competitions_data = [
        {'name': 'Premier League', 'country_id': 1, 'type': 'league', 'tier': 1, 'season': '2023-24'},
        {'name': 'La Liga', 'country_id': 2, 'type': 'league', 'tier': 1, 'season': '2023-24'},
        {'name': 'Bundesliga', 'country_id': 3, 'type': 'league', 'tier': 1, 'season': '2023-24'},
        {'name': 'Serie A', 'country_id': 4, 'type': 'league', 'tier': 1, 'season': '2023-24'},
        {'name': 'Ligue 1', 'country_id': 5, 'type': 'league', 'tier': 1, 'season': '2023-24'},
    ]
    
    competitions_df = pd.DataFrame(competitions_data)
    db_manager.insert_dataframe(competitions_df, 'competitions', if_exists='replace')
    
    # Sample clubs
    clubs_data = [
        {'name': 'Manchester City', 'short_name': 'Man City', 'country_id': 1, 'city': 'Manchester'},
        {'name': 'Real Madrid', 'short_name': 'Real Madrid', 'country_id': 2, 'city': 'Madrid'},
        {'name': 'Bayern Munich', 'short_name': 'Bayern', 'country_id': 3, 'city': 'Munich'},
        {'name': 'AC Milan', 'short_name': 'Milan', 'country_id': 4, 'city': 'Milan'},
        {'name': 'Paris Saint-Germain', 'short_name': 'PSG', 'country_id': 5, 'city': 'Paris'},
    ]
    
    clubs_df = pd.DataFrame(clubs_data)
    db_manager.insert_dataframe(clubs_df, 'clubs', if_exists='replace')
    
    logger.info("Sample data generated successfully")