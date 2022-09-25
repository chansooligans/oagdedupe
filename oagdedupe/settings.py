# following https://pydantic-docs.helpmanual.io/usage/settings/

# import configparser
# import os
import logging
from typing import Any, List, Optional
from pydantic import BaseSettings, BaseModel
from pathlib import Path


class SettingsService(BaseModel):
    """settings for a service"""

    host: str = "http://172.22.39.26"

    port: int = 8089

    @property
    def url(self) -> str:
        return f"{self.host}:{self.port}"


class SettingsLabelStudio(SettingsService):

    api_key: str = "please provide an api key"

    """project description"""
    description: str = "entity resolution"


class SettingsOther(BaseModel):
    """Other project settings"""

    """dedupe vs record-linkage"""
    dedupe: bool = True

    """block learner sample size (per learning loop)"""
    n: int = 5000

    """maximum length of blocking scheme conjunctions"""
    k: int = 3

    """maximum number of comparisons"""
    max_compare: int = 1_000_000

    """entity attribute names"""
    attributes: Optional[List[str]] = ["name", "addr"]

    """number of cpus to use"""
    cpus: int = 1

    """path to database"""
    path_database: str = "postgresql+psycopg2://username:password@172.22.39.26:8000/db"

    """database schema"""
    db_schema: str = "dedupe"

    """path to model"""
    path_model: Path = Path("./.dedupe/model")

    """label studio settings"""
    label_studio: SettingsLabelStudio = SettingsLabelStudio()

    """fast api settings"""
    fast_api: SettingsService = SettingsService(port=8090)


DEFAULT: SettingsOther = SettingsOther()


class Settings(BaseSettings):
    """project settings"""

    """name of the project, a unique identifier"""
    name: str = "default"

    """path to folder to store the config file"""
    folder: Path = Path("./.dedupe")

    """other project settings"""
    other: Optional[SettingsOther] = SettingsOther()

    class Config:
        env_prefix = "deduper_"

    @property
    def path(self) -> Path:
        return self.folder / f"deduper_settings_{self.name}.json"

    def save(self) -> None:
        """save settings to file"""

        logging.info(f"saving settings for {self.name} to {self.path}")

        # create the settings folder if it doesn't exist
        if not self.folder.is_dir():
            self.folder.mkdir()

        with open(self.path, "w") as f:
            f.write(self.json(indent=2))

    def read(self) -> None:
        """read settings from file"""

        logging.info(f"reading settings for {self.name} from {self.path}")
        self.other = Settings.parse_file(self.path).other

    def sync(self) -> None:
        """read settings if settings file exists and default settings provided, else save

        assume that if default settings are provided and a settings file exists the user wants to read settings from file, not reset settings to default
        """

        if self.other == SettingsOther() and self.path.is_file():
            self.read()
        else:
            self.save()

    def set(self, name: str, value: Any):  # -> Settings:
        """set a setting in self.other and save settings to keep the file updated

        Parameters
        ----------
        name : str
            name of the SettingsOther attribute to set
        value : Any
            value to set to

        Returns
        -------
        Settings
            self after setting, to allow for composed calls to `set` (i.e. settings.set().set()...)
        """
        self.other.__setattr__(name, value)
        self.save()
        return self


def get_settings_from_env() -> Settings:
    """Get settings from environment variables, see https://pydantic-docs.helpmanual.io/usage/settings/

    Returns
    -------
    Settings
        the settings as read from file whose name and location are read from environment variables
    """
    settings = Settings()
    settings.read()
    return settings
