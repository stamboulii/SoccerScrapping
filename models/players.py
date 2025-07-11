from sqlalchemy import Column, Integer, String
from models.base import Base

class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    age = Column(Integer, nullable=False)
    club = Column(String(100), nullable=True)  # Peut devenir une FK plus tard

    def __repr__(self):
        return f"<Player(name='{self.name}', age={self.age}, club='{self.club}')>"
