from pydantic import BaseModel
from typing import Optional

class PlayerIn(BaseModel):
    name: str
    age: Optional[int] = None
    club: Optional[str] = None

class PlayerOut(PlayerIn):
    id: int

    class Config:
        orm_mode = True
