from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/marketplace"

    class Config:
        env_prefix = "MARKETPLACE_"
        env_file = ".env"
