import pandas as pd

from app.config import (
    COMPLETION_RATE_BY_COMPANY_FILE,
    COMPLETION_RATE_BY_USER_FILE,
    SUMMARY_REPORT_FILE,
    TODO_COUNT_BY_CITY_FILE,
    TODO_COUNT_BY_USER_FILE,
)
from app.logger import setup_logger
from app.utils.file_utils import save_json
from app.services.clean_service import save_dataframe
from app.config import FINAL_USER_SUMMARY_FILE

logger = setup_logger()


def build_todo_count_by_user_report(merged_df: pd.DataFrame) -> pd.DataFrame:
    if merged_df.empty:
        logger.warning("merged_df 是空的，無法產生 todo_count_by_user_report")
        return pd.DataFrame()

    report_df = (
        merged_df.groupby(["user_id", "user_name"], as_index=False)
        .agg(todo_count=("todo_id", "count"))
        .sort_values(by="todo_count", ascending=False)
        .reset_index(drop=True)
    )

    logger.info(f"已產生 todo_count_by_user_report，共 {len(report_df)} 筆")
    return report_df


def build_todo_count_by_city_report(merged_df: pd.DataFrame) -> pd.DataFrame:
    if merged_df.empty:
        logger.warning("merged_df 是空的，無法產生 todo_count_by_city_report")
        return pd.DataFrame()

    report_df = (
        merged_df.groupby("city", as_index=False)
        .agg(todo_count=("todo_id", "count"))
        .sort_values(by="todo_count", ascending=False)
        .reset_index(drop=True)
    )

    logger.info(f"已產生 todo_count_by_city_report，共 {len(report_df)} 筆")
    return report_df


def build_completion_rate_by_user_report(merged_df: pd.DataFrame) -> pd.DataFrame:
    if merged_df.empty:
        logger.warning("merged_df 是空的，無法產生 completion_rate_by_user_report")
        return pd.DataFrame()

    report_df = (
        merged_df.groupby(["user_id", "user_name"], as_index=False)
        .agg(
            total_todos=("todo_id", "count"),
            completed_todos=("completed", "sum"),
        )
    )

    report_df["completion_rate"] = (
        report_df["completed_todos"] / report_df["total_todos"]
    ).round(4)

    report_df = report_df.sort_values(
        by=["completion_rate", "completed_todos"],
        ascending=[False, False],
    ).reset_index(drop=True)

    logger.info(f"已產生 completion_rate_by_user_report，共 {len(report_df)} 筆")
    return report_df


def build_completion_rate_by_company_report(merged_df: pd.DataFrame) -> pd.DataFrame:
    if merged_df.empty:
        logger.warning("merged_df 是空的，無法產生 completion_rate_by_company_report")
        return pd.DataFrame()

    report_df = (
        merged_df.groupby("company_name", as_index=False)
        .agg(
            total_todos=("todo_id", "count"),
            completed_todos=("completed", "sum"),
        )
    )

    report_df["completion_rate"] = (
        report_df["completed_todos"] / report_df["total_todos"]
    ).round(4)

    report_df = report_df.sort_values(
        by=["completion_rate", "completed_todos"],
        ascending=[False, False],
    ).reset_index(drop=True)

    logger.info(f"已產生 completion_rate_by_company_report，共 {len(report_df)} 筆")
    return report_df


def build_summary_report(merged_df: pd.DataFrame) -> dict:
    if merged_df.empty:
        logger.warning("merged_df 是空的，無法產生 summary_report")
        return {}

    total_todos = int(merged_df["todo_id"].count())
    total_users = int(merged_df["user_id"].nunique())
    total_cities = int(merged_df["city"].nunique())
    total_companies = int(merged_df["company_name"].nunique())
    completed_todos = int(merged_df["completed"].sum())
    overall_completion_rate = round(completed_todos / total_todos, 4)

    top_user_row = (
        merged_df.groupby("user_name", as_index=False)
        .agg(todo_count=("todo_id", "count"))
        .sort_values(by="todo_count", ascending=False)
        .iloc[0]
    )

    top_city_row = (
        merged_df.groupby("city", as_index=False)
        .agg(todo_count=("todo_id", "count"))
        .sort_values(by="todo_count", ascending=False)
        .iloc[0]
    )

    summary = {
        "total_todos": total_todos,
        "total_users": total_users,
        "total_cities": total_cities,
        "total_companies": total_companies,
        "completed_todos": completed_todos,
        "overall_completion_rate": overall_completion_rate,
        "top_user_by_todo_count": {
            "user_name": top_user_row["user_name"],
            "todo_count": int(top_user_row["todo_count"]),
        },
        "top_city_by_todo_count": {
            "city": top_city_row["city"],
            "todo_count": int(top_city_row["todo_count"]),
        },
    }

    logger.info("已產生 summary_report")
    return summary


def run_report_pipeline(merged_df: pd.DataFrame) -> dict:
    todo_count_by_user_df = build_todo_count_by_user_report(merged_df)
    todo_count_by_city_df = build_todo_count_by_city_report(merged_df)
    completion_rate_by_user_df = build_completion_rate_by_user_report(merged_df)
    completion_rate_by_company_df = build_completion_rate_by_company_report(merged_df)
    summary_report = build_summary_report(merged_df)

    save_dataframe(todo_count_by_user_df, TODO_COUNT_BY_USER_FILE)
    save_dataframe(todo_count_by_city_df, TODO_COUNT_BY_CITY_FILE)
    save_dataframe(completion_rate_by_user_df, COMPLETION_RATE_BY_USER_FILE)
    save_dataframe(completion_rate_by_company_df, COMPLETION_RATE_BY_COMPANY_FILE)

    final_user_summary_df = build_final_user_summary_table(merged_df)
    save_dataframe(final_user_summary_df, FINAL_USER_SUMMARY_FILE)

    if summary_report:
        save_json(SUMMARY_REPORT_FILE, summary_report)
        logger.info(f"已輸出 summary JSON：{SUMMARY_REPORT_FILE}")

    return {
        "todo_count_by_user_df": todo_count_by_user_df,
        "todo_count_by_city_df": todo_count_by_city_df,
        "completion_rate_by_user_df": completion_rate_by_user_df,
        "completion_rate_by_company_df": completion_rate_by_company_df,
        "summary_report": summary_report,
        "final_user_summary_df": final_user_summary_df,
    }

def build_final_user_summary_table(merged_df: pd.DataFrame) -> pd.DataFrame:
    if merged_df.empty:
        logger.warning("merged_df 是空的，無法產生 final_user_summary_table")
        return pd.DataFrame()

    report_df = (
        merged_df.groupby(
            ["user_id", "user_name", "city", "company_name"],
            as_index=False
        )
        .agg(
            total_todos=("todo_id", "count"),
            completed_todos=("completed", "sum"),
        )
    )

    report_df["completion_rate"] = (
        report_df["completed_todos"] / report_df["total_todos"]
    ).round(4)

    report_df = report_df.sort_values(
        by=["completion_rate", "completed_todos"],
        ascending=[False, False],
    ).reset_index(drop=True)

    logger.info(f"已產生 final_user_summary_table，共 {len(report_df)} 筆")
    return report_df

def build_final_user_summary_table(merged_df: pd.DataFrame) -> pd.DataFrame:
    if merged_df.empty:
        logger.warning("merged_df 是空的，無法產生 final_user_summary_table")
        return pd.DataFrame()

    report_df = (
        merged_df.groupby(
            ["user_id", "user_name", "city", "company_name"],
            as_index=False
        )
        .agg(
            total_todos=("todo_id", "count"),
            completed_todos=("completed", "sum"),
        )
    )

    report_df["completion_rate"] = (
        report_df["completed_todos"] / report_df["total_todos"]
    ).round(4)

    report_df = report_df.sort_values(
        by=["completion_rate", "completed_todos"],
        ascending=[False, False],
    ).reset_index(drop=True)

    logger.info(f"已產生 final_user_summary_table，共 {len(report_df)} 筆")
    return report_df