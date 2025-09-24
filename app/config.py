import logging
import os

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict


def setup_logging(log_level: str) -> None:
    logger = logging.getLogger()
    logger.setLevel(log_level.upper())

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level.upper())

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)


def load_config(config_file_name: str) -> dict[str, dict]:
    current_dir = os.path.dirname(__file__)
    profiles_path = os.path.join(current_dir, config_file_name)
    with open(profiles_path) as profiles_file:
        return yaml.safe_load(profiles_file)


class BasicAuthSettings(BaseSettings):
    username: str
    password: str


class DBSettings(BaseSettings):
    url: str
    model_config = SettingsConfigDict(env_prefix="DB_")


class QueueSettings(BaseSettings):
    broker_url: str
    result_backend: str
    model_config = SettingsConfigDict(env_prefix="QU_")


class LCTSettings(BaseSettings):
    auth: BasicAuthSettings
    db: DBSettings
    queue: QueueSettings

    @classmethod
    def from_yaml(cls, file_name: str) -> "LCTSettings":
        cfg = load_config(file_name)

        basic_auth_config = cfg.get("auth", {})
        db_config = cfg.get("db", {})
        queue_config = cfg.get("queue", {})

        return cls(
            auth=BasicAuthSettings(
                username=basic_auth_config.get("username", "no_username"),
                password=basic_auth_config.get("password", "no_password"),
            ),
            db=DBSettings(
                url=db_config.get("url", "no_url"),
            ),
            queue=QueueSettings(
                broker_url=queue_config.get("broker_url", "no_broker_url"),
                result_backend=queue_config.get("result_backend", "no_result_backend"),
            ),
        )


lct_config_path = os.getenv("LCT_CONFIG_PATH") or "lct_config_local.yaml"
lct_log_level = os.getenv("LCT_LOG_LEVEL") or "INFO"
setup_logging(lct_log_level)
lct_settings = LCTSettings.from_yaml(lct_config_path)
print(f"lct_settings: {lct_settings}")
