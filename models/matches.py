from sqlalchemy import Column, Integer, String, Date
from models.base import Base

class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True, index=True)
    home_team = Column(String(100), nullable=False)
    away_team = Column(String(100), nullable=False)
    date = Column(Date, nullable=False)
