# ── config.py ─────────────────────────────────────────────────────────
from pathlib import Path
from pydantic_settings import BaseSettings          # ✅ nouveau chemin
from pydantic import Field                          # Field est toujours dans pydantic v2

class Settings(BaseSettings):
    DATABASE_URL: str = Field(
        default="postgresql+psycopg2://postgres:548651@localhost:5432/soccer_db",
        env="DATABASE_URL",
    )

    # logging
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    LOG_TO_CONSOLE: bool = Field(default=True)
    LOG_TO_FILE: bool = Field(default=False)
    LOG_FILE_NAME: Path = Path("logs/scraper.log")

settings = Settings()

# Pour accès direct dans vos modules
DATABASE_URL   = settings.DATABASE_URL
LOG_LEVEL      = settings.LOG_LEVEL
LOG_TO_CONSOLE = settings.LOG_TO_CONSOLE
LOG_TO_FILE    = settings.LOG_TO_FILE
LOG_FILE_NAME  = settings.LOG_FILE_NAME
