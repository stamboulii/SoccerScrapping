from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
import pandas as pd
from config import DATABASE_URL
from models.base import Base
from models import countries, competitions, clubs, players, matches  # Assurez-vous que tous les modèles sont importés

# Créer l'engine PostgreSQL
engine = create_engine(DATABASE_URL, echo=False)

# Créer une session
SessionLocal = scoped_session(sessionmaker(bind=engine))

class PostgresDBManager:
    def __init__(self):
        self.engine = engine
        self.Session = SessionLocal

    def execute_query(self, query: str) -> pd.DataFrame:
        """Exécute une requête SQL et retourne un DataFrame."""
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df

    def get_table_stats(self) -> dict:
        """Retourne le nombre de lignes pour les tables principales."""
        stats = {}
        tables = ['countries', 'competitions', 'clubs', 'players', 'matches']
        for table in tables:
            try:
                df = self.execute_query(f"SELECT COUNT(*) AS count FROM {table}")
                stats[table] = int(df.iloc[0]['count'])
            except Exception:
                stats[table] = 'error'
        return stats

    def bulk_insert_players(self, data: list[tuple]):
        """Insère une liste de joueurs [(name, age, club_name), ...]"""
        with self.Session() as session:
            for name, age, club in data:
                session.add(players.Player(name=name, age=age, club=club))
            session.commit()

# Initialise les tables (à exécuter une seule fois si non gérées par Alembic)
def init_database():
    Base.metadata.create_all(bind=engine)

# Insère quelques données d’exemple
def generate_sample_data():
    from models.countries import Country
    from models.competitions import Competition
    from models.clubs import Club
    from models.players import Player

    with SessionLocal() as session:
        # Exemple simple
        france = Country(name="France", code="FR")
        epl = Competition(name="Premier League", country="England")
        city = Club(name="Manchester City", country="England", league="Premier League")
        haaland = Player(name="Erling Haaland", age=23, club="Manchester City")

        session.add_all([france, epl, city, haaland])
        session.commit()

# Instance globale
db_manager = PostgresDBManager()
