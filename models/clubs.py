from sqlalchemy import Column, Integer, String
from models.base import Base

class Club(Base):
    __tablename__ = "clubs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)
    league = Column(String(100), nullable=False)
