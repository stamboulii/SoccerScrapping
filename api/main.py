from fastapi import FastAPI
from api.routes import countries, players

app = FastAPI(title="Soccer Data API", version="1.0")

# Register routes
app.include_router(countries.router, prefix="/countries", tags=["Countries"])
app.include_router(players.router, prefix="/players", tags=["Players"])

@app.get("/")
def root():
    return {"message": "Welcome to the Soccer Data API"}
