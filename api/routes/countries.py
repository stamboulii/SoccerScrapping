from fastapi import APIRouter, HTTPException
from api.models.schemas import Country
from database.sqlite_connection import db_manager

router = APIRouter()

@router.get("/", response_model=list[Country])
def get_all_countries():
    query = "SELECT * FROM countries"
    try:
        df = db_manager.execute_query(query)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
