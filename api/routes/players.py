from fastapi import APIRouter, HTTPException
from api.models.schemas import Player, PlayerCreate
from database.sqlite_connection import db_manager

router = APIRouter()

@router.get("/", response_model=list[Player])
def get_players():
    try:
        df = db_manager.execute_query("SELECT * FROM players LIMIT 100")
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=Player)
def create_player(player: PlayerCreate):
    try:
        insert_sql = """
        INSERT INTO players (name, age, club)
        VALUES (?, ?, ?)
        """
        db_manager.connection.execute(insert_sql, (player.name, player.age, player.club))
        db_manager.connection.commit()
        return player
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
