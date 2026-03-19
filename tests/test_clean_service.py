import pandas as pd
import pytest
from pandas.errors import MergeError

from app.services import clean_service


def test_clean_todo_data_reads_raw_json_and_builds_expected_columns(
    tmp_path,
    monkeypatch,
    sample_todo_records,
    write_json_file,
):
    todo_raw_dir = tmp_path / "raw" / "todos"

    for record in sample_todo_records:
        write_json_file(todo_raw_dir / f"todo_{record['id']}.json", record)

    monkeypatch.setattr(clean_service, "TODO_RAW_DIR", todo_raw_dir)

    df = clean_service.clean_todo_data()

    assert not df.empty
    assert list(df["todo_id"]) == [1, 2, 3, 4, 5]
    assert list(df["user_id"]) == [1, 1, 2, 2, 2]
    assert list(df["completed_text"]) == [
        "done",
        "not_done",
        "done",
        "done",
        "not_done",
    ]
    assert df.loc[df["todo_id"] == 1, "title_length"].item() == len("buy milk")
    assert set(df.columns) == {
        "todo_id",
        "user_id",
        "title",
        "completed",
        "title_length",
        "completed_text",
    }


def test_clean_user_data_flattens_nested_json(
    tmp_path,
    monkeypatch,
    sample_user_records,
    write_json_file,
):
    user_raw_file = tmp_path / "raw" / "users" / "users.json"
    write_json_file(user_raw_file, {"users": sample_user_records})

    monkeypatch.setattr(clean_service, "USER_RAW_FILE", user_raw_file)

    df = clean_service.clean_user_data()

    assert not df.empty
    assert len(df) == 2
    assert list(df["user_id"]) == [1, 2]
    assert list(df["user_name"]) == ["Alice Chen", "Bob Lin"]
    assert list(df["company_name"]) == ["Acme Labs", "Beta Works"]
    assert list(df["city"]) == ["Taipei", "Taichung"]
    assert df.loc[df["user_id"] == 1, "geo_lat"].item() == "25.03"
    assert set(df.columns) == {
        "user_id",
        "user_name",
        "username",
        "email",
        "phone",
        "website",
        "company_name",
        "city",
        "zipcode",
        "geo_lat",
        "geo_lng",
    }


def test_merge_todo_user_data_merges_user_fields_into_todo_rows():
    todo_df = pd.DataFrame(
        [
            {"todo_id": 1, "user_id": 1, "title": "a", "completed": True},
            {"todo_id": 2, "user_id": 1, "title": "b", "completed": False},
            {"todo_id": 3, "user_id": 2, "title": "c", "completed": True},
        ]
    )

    user_df = pd.DataFrame(
        [
            {"user_id": 1, "user_name": "Alice", "company_name": "Acme", "city": "Taipei"},
            {"user_id": 2, "user_name": "Bob", "company_name": "Beta", "city": "Taichung"},
        ]
    )

    merged_df = clean_service.merge_todo_user_data(todo_df, user_df)

    assert len(merged_df) == 3
    assert list(merged_df["user_name"]) == ["Alice", "Alice", "Bob"]
    assert list(merged_df["city"]) == ["Taipei", "Taipei", "Taichung"]


def test_merge_todo_user_data_raises_when_user_key_is_duplicated():
    todo_df = pd.DataFrame(
        [
            {"todo_id": 1, "user_id": 1, "title": "a", "completed": True},
            {"todo_id": 2, "user_id": 2, "title": "b", "completed": False},
        ]
    )

    duplicated_user_df = pd.DataFrame(
        [
            {"user_id": 1, "user_name": "Alice A"},
            {"user_id": 1, "user_name": "Alice B"},
            {"user_id": 2, "user_name": "Bob"},
        ]
    )

    with pytest.raises(MergeError):
        clean_service.merge_todo_user_data(todo_df, duplicated_user_df)