# following https://pydantic-docs.helpmanual.io/usage/settings/

# import configparser
# import os
import logging
from pathlib import Path
from typing import Any, List, Optional

from pydantic import BaseModel, BaseSettings


class SettingsService(BaseModel):
    """settings for a service"""

    host: str = "http://0.0.0.0"

    port: int = 8090

    @property
    def url(self) -> str:
        return f"{self.host}:{self.port}"


class SettingsLabelStudio(SettingsService):

    port: int = 8089

    api_key: str = "please provide an api key"

    """project description"""
    description: str = "entity resolution"


class SettingsModel(BaseModel):

    """dedupe vs record-linkage"""

    dedupe: bool = True

    """block learner sample size (per learning loop)"""
    n: int = 5000

    """maximum length of blocking scheme conjunctions"""
    k: int = 3

    """maximum number of comparisons;"""
    max_compare: int = 1_000_000

    """maximum number of comparisons"""
    n_covered: int = 500_000

    """number of cpus to use"""
    cpus: int = 1

    """path to model"""
    path_model: Path = Path("./.dedupe/model")


class SettingsDB(BaseModel):
    """Other project settings"""

    """path to database"""
    path_database: str = (
        "postgresql+psycopg2://username:password@0.0.0.0:8000/db"
    )

    """database schema"""
    db_schema: str = "dedupe"


class Settings(BaseSettings):
    """project settings"""

    """entity attribute names"""
    attributes: list = ["name", "addr"]

    """name of the project, a unique identifier"""
    name: str = "default"

    """path to folder to store the config file"""
    folder: Path = Path("./.dedupe")

    """model settings"""
    model: SettingsModel = SettingsModel()

    """other project settings"""
    db: SettingsDB = SettingsDB()

    """label studio settings"""
    label_studio: SettingsLabelStudio = SettingsLabelStudio()

    """fast api settings"""
    fast_api: SettingsService = SettingsService(port=8090)

    class Config:
        env_file = ".env"
        env_nested_delimiter = "__"
        env_prefix = "oagdedupe_"
