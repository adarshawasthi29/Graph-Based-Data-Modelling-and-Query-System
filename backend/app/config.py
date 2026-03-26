from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# backend/app/config.py -> project root (where .env usually lives)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Allow both project-root `.env` and backend-local `.ENV`/`.env`
_BACKEND_DIR = Path(__file__).resolve().parents[1]
_ENV_CANDIDATES = [
    _PROJECT_ROOT / ".env",
    _BACKEND_DIR / ".env",
    _BACKEND_DIR / ".ENV",
]
_ENV_FILES = [str(p) for p in _ENV_CANDIDATES if p.exists()]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILES if _ENV_FILES else str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    neo4j_uri: str = Field(validation_alias="NEO4J_URI")
    neo4j_username: str = Field(validation_alias="NEO4J_USERNAME")
    neo4j_password: str = Field(validation_alias="NEO4J_PASSWORD")
    neo4j_database: str = Field(default="neo4j", validation_alias="NEO4J_DATABASE")

    llm_provider: Literal["openai", "groq"] = Field(
        default="openai", validation_alias="LLM_PROVIDER"
    )
    openai_api_key: Optional[str] = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", validation_alias="OPENAI_MODEL")

    groq_api_key: Optional[str] = Field(default=None, validation_alias="GROQ_API_KEY")
    groq_model: str = Field(
        default="llama-3.3-70b-versatile", validation_alias="GROQ_MODEL"
    )

    graph_cypher_top_k: int = Field(default=25, validation_alias="GRAPH_CYPHER_TOP_K")

    cors_origins: str = Field(
        default="http://localhost:5173,http://127.0.0.1:5173",
        validation_alias="CORS_ORIGINS",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
