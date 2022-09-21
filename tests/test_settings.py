from dedupe.settings import (
    Settings,
    SettingsOther,
    SettingsService,
    SettingsLabelStudio,
)
from pytest import fixture
from pytest import MonkeyPatch
import os
from pathlib import Path


@fixture
def settings(tmp_path) -> Settings:
    return Settings(
        name="test",
        folder=tmp_path,
        other=SettingsOther(
            n=5000,
            k=3,
            cpus=15,
            attributes=["name", "addr"],
            path_database=tmp_path / "test.db",
            db_schema="dedupe",
            path_model="test_model",
            label_studio=SettingsLabelStudio(
                port=8089,
                api_key="test_api_key",
                description="test project",
            ),
            fast_api=SettingsService(port=8003),
        ),
    )


def test_path(settings: Settings) -> None:
    assert settings.path == settings.folder / f"deduper_settings_{settings.name}.json"


def test_save(settings: Settings) -> None:
    settings.save()
    assert settings.path.is_file()


def test_read(settings: Settings) -> None:
    settings.save()
    settings_new = Settings(name=settings.name, folder=settings.folder)
    assert settings_new.other == SettingsOther()
    settings_new.read()
    assert settings_new.other == settings.other


# def test_sync(settings: Settings) -> None:
#     if settings.path.is_file():
#         settings.path.unlink()
#     settings.sync()
#     assert settings.path.is_file()
#     settings_new = Settings(name=settings.name, folder=settings.folder)
#     settings_new.sync()
#     assert settings_new.other == settings.other


# def test_set(settings: Settings) -> None:
#     assert settings.other is not None
#     settings.set("cpus", 1)
#     assert settings.other.cpus == 1
#     settings.set("cpus", 10)
#     assert settings.other.cpus == 10

def test_get_settings_from_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(os, "environ", {"DEDUPER_NAME": "name from env", "DEDUPER_FOLDER": "folder/from/env"})
    settings = Settings()
    assert settings.name == "name from env"
    assert settings.folder == Path("folder/from/env")