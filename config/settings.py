from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

ROOT = Path(__file__).parent.parent


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379/1"
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    proxies_file: str = "data/proxies.txt"
    log_level: str = "INFO"
    dry_run: bool = False
    alchemy_keys: str = ""  # comma-separated

    def get_alchemy_keys(self) -> list[str]:
        return [k.strip() for k in self.alchemy_keys.split(",") if k.strip()]

    class Config:
        env_file = ROOT / ".env"
        case_sensitive = False


@lru_cache
def get_settings() -> Settings:
    return Settings()


@lru_cache
def get_thresholds() -> dict[str, Any]:
    with open(ROOT / "config" / "thresholds.yaml") as f:
        return yaml.safe_load(f)
