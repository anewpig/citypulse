import json
from pathlib import Path


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_json(file_path: Path, data: dict) -> None:
    ensure_directory(file_path.parent)
    with file_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def load_json(file_path: Path) -> dict:
    with file_path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    return data


def list_json_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob("*.json"))