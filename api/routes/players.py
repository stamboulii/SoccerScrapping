from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database.deps import get_db
from models.players import Player
from api.schemas.player_schema import PlayerIn, PlayerOut

router = APIRouter()

@router.get("/", response_model=list[PlayerOut])
def list_players(db: Session = Depends(get_db)):
    return db.query(Player).limit(100).all()

@router.post("/", response_model=PlayerOut)
def create_player(player: PlayerIn, db: Session = Depends(get_db)):
    db_player = Player(**player.dict())
    db.add(db_player)
    db.commit()
    db.refresh(db_player)
    return db_player
