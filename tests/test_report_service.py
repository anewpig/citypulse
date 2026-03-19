import json

import pytest

from app.services import report_service


def test_build_todo_count_by_user_report(merged_df):
    report_df = report_service.build_todo_count_by_user_report(merged_df)

    assert not report_df.empty
    assert list(report_df.columns) == ["user_id", "user_name", "todo_count"]

    top_row = report_df.iloc[0]
    assert top_row["user_name"] == "Bob Lin"
    assert top_row["todo_count"] == 3

    alice_row = report_df[report_df["user_name"] == "Alice Chen"].iloc[0]
    assert alice_row["todo_count"] == 2


def test_build_completion_rate_by_user_report(merged_df):
    report_df = report_service.build_completion_rate_by_user_report(merged_df)

    assert not report_df.empty
    assert set(report_df.columns) == {
        "user_id",
        "user_name",
        "total_todos",
        "completed_todos",
        "completion_rate",
    }

    alice_row = report_df[report_df["user_name"] == "Alice Chen"].iloc[0]
    bob_row = report_df[report_df["user_name"] == "Bob Lin"].iloc[0]

    assert alice_row["total_todos"] == 2
    assert alice_row["completed_todos"] == 1
    assert alice_row["completion_rate"] == pytest.approx(0.5)

    assert bob_row["total_todos"] == 3
    assert bob_row["completed_todos"] == 2
    assert bob_row["completion_rate"] == pytest.approx(0.6667)


def test_build_summary_report(merged_df):
    summary = report_service.build_summary_report(merged_df)

    assert summary["total_todos"] == 5
    assert summary["total_users"] == 2
    assert summary["total_cities"] == 2
    assert summary["total_companies"] == 2
    assert summary["completed_todos"] == 3
    assert summary["overall_completion_rate"] == 0.6
    assert summary["top_user_by_todo_count"]["user_name"] == "Bob Lin"
    assert summary["top_user_by_todo_count"]["todo_count"] == 3
    assert summary["top_city_by_todo_count"]["city"] == "Taichung"
    assert summary["top_city_by_todo_count"]["todo_count"] == 3


def test_run_report_pipeline_writes_all_report_files(tmp_path, monkeypatch, merged_df):
    report_dir = tmp_path / "reports"

    monkeypatch.setattr(
        report_service,
        "TODO_COUNT_BY_USER_FILE",
        report_dir / "todo_count_by_user.csv",
    )
    monkeypatch.setattr(
        report_service,
        "TODO_COUNT_BY_CITY_FILE",
        report_dir / "todo_count_by_city.csv",
    )
    monkeypatch.setattr(
        report_service,
        "COMPLETION_RATE_BY_USER_FILE",
        report_dir / "completion_rate_by_user.csv",
    )
    monkeypatch.setattr(
        report_service,
        "COMPLETION_RATE_BY_COMPANY_FILE",
        report_dir / "completion_rate_by_company.csv",
    )
    monkeypatch.setattr(
        report_service,
        "SUMMARY_REPORT_FILE",
        report_dir / "summary_report.json",
    )

    results = report_service.run_report_pipeline(merged_df)

    assert (report_dir / "todo_count_by_user.csv").exists()
    assert (report_dir / "todo_count_by_city.csv").exists()
    assert (report_dir / "completion_rate_by_user.csv").exists()
    assert (report_dir / "completion_rate_by_company.csv").exists()
    assert (report_dir / "summary_report.json").exists()

    assert not results["todo_count_by_user_df"].empty
    assert not results["completion_rate_by_user_df"].empty
    assert results["summary_report"]["total_todos"] == 5

    summary_json = json.loads(
        (report_dir / "summary_report.json").read_text(encoding="utf-8")
    )
    assert summary_json["top_user_by_todo_count"]["user_name"] == "Bob Lin"
    assert summary_json["overall_completion_rate"] == 0.6