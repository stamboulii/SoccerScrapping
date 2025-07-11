from fastapi import FastAPI
from api.routes import players  # import other routers similarly
from database.session import engine
from models.base import Base

# Create tables in dev/test environments
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Soccer Data API")

app.include_router(players.router, prefix="/players", tags=["Players"])

@app.get("/")
def read_root():
    return {"message": "Welcome to the PostgreSQL‑powered API"}
