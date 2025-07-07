from pydantic import BaseModel, Field
from typing import Optional

class Country(BaseModel):
    id: Optional[int]
    name: str
    code: Optional[str]

class PlayerCreate(BaseModel):
    name: str = Field(..., example="Kylian Mbappé")
    age: int = Field(..., gt=0, example=25)
    club: str = Field(..., example="Paris Saint-Germain")

class Player(PlayerCreate):
    id: Optional[int]
