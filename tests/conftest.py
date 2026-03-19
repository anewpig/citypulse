import json
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def write_json_file():
    def _write(path: Path, data: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return _write


@pytest.fixture
def sample_todo_records():
    return [
        {"userId": 1, "id": 1, "title": "buy milk", "completed": True},
        {"userId": 1, "id": 2, "title": "write tests", "completed": False},
        {"userId": 2, "id": 3, "title": "review pr", "completed": True},
        {"userId": 2, "id": 4, "title": "fix bug", "completed": True},
        {"userId": 2, "id": 5, "title": "deploy app", "completed": False},
    ]


@pytest.fixture
def sample_user_records():
    return [
        {
            "id": 1,
            "name": "Alice Chen",
            "username": "alice",
            "email": "alice@example.com",
            "phone": "0900-111-111",
            "website": "alice.dev",
            "company": {"name": "Acme Labs"},
            "address": {
                "city": "Taipei",
                "zipcode": "100",
                "geo": {"lat": "25.03", "lng": "121.56"},
            },
        },
        {
            "id": 2,
            "name": "Bob Lin",
            "username": "bob",
            "email": "bob@example.com",
            "phone": "0900-222-222",
            "website": "bob.dev",
            "company": {"name": "Beta Works"},
            "address": {
                "city": "Taichung",
                "zipcode": "400",
                "geo": {"lat": "24.15", "lng": "120.67"},
            },
        },
    ]


@pytest.fixture
def merged_df(sample_todo_records, sample_user_records):
    todo_df = pd.DataFrame(
        [
            {
                "todo_id": record["id"],
                "user_id": record["userId"],
                "title": record["title"],
                "completed": record["completed"],
            }
            for record in sample_todo_records
        ]
    )
    todo_df["title_length"] = todo_df["title"].str.len()
    todo_df["completed_text"] = todo_df["completed"].map(
        {True: "done", False: "not_done"}
    )

    user_df = pd.DataFrame(
        [
            {
                "user_id": user["id"],
                "user_name": user["name"],
                "username": user["username"],
                "email": user["email"],
                "phone": user["phone"],
                "website": user["website"],
                "company_name": user["company"]["name"],
                "city": user["address"]["city"],
                "zipcode": user["address"]["zipcode"],
                "geo_lat": user["address"]["geo"]["lat"],
                "geo_lng": user["address"]["geo"]["lng"],
            }
            for user in sample_user_records
        ]
    )

    return pd.merge(
        left=todo_df,
        right=user_df,
        how="left",
        on="user_id",
        validate="many_to_one",
    )