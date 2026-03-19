import pandas as pd

from app.config import (
    MERGED_PROCESSED_FILE,
    TODO_PROCESSED_FILE,
    TODO_RAW_DIR,
    USER_PROCESSED_FILE,
    USER_RAW_FILE,
)
from app.logger import setup_logger
from app.utils.file_utils import ensure_directory, list_json_files, load_json

logger = setup_logger()


def clean_todo_data() -> pd.DataFrame:
    json_files = list_json_files(TODO_RAW_DIR)

    if not json_files:
        logger.warning("沒有找到 raw todo JSON 檔")
        return pd.DataFrame()

    logger.info(f"開始清理 todo raw JSON，共找到 {len(json_files)} 個檔案")

    records = []

    for file_path in json_files:
        data = load_json(file_path)

        record = {
            "todo_id": data.get("id"),
            "user_id": data.get("userId"),
            "title": data.get("title"),
            "completed": data.get("completed"),
        }
        records.append(record)

    df = pd.DataFrame(records)
    df = df.sort_values(by="todo_id").reset_index(drop=True)
    df["title_length"] = df["title"].str.len()
    df["completed_text"] = df["completed"].map({True: "done", False: "not_done"})

    logger.info(f"todo 清理完成，DataFrame 共 {len(df)} 筆資料")
    return df


def clean_user_data() -> pd.DataFrame:
    if not USER_RAW_FILE.exists():
        logger.warning("沒有找到 raw users JSON 檔")
        return pd.DataFrame()

    logger.info("開始清理 users raw JSON")

    raw_data = load_json(USER_RAW_FILE)
    users = raw_data.get("users", [])

    if not users:
        logger.warning("users raw JSON 裡沒有資料")
        return pd.DataFrame()

    records = []

    for user in users:
        company = user.get("company", {}) or {}
        address = user.get("address", {}) or {}
        geo = address.get("geo", {}) or {}

        record = {
            "user_id": user.get("id"),
            "user_name": user.get("name"),
            "username": user.get("username"),
            "email": user.get("email"),
            "phone": user.get("phone"),
            "website": user.get("website"),
            "company_name": company.get("name"),
            "city": address.get("city"),
            "zipcode": address.get("zipcode"),
            "geo_lat": geo.get("lat"),
            "geo_lng": geo.get("lng"),
        }
        records.append(record)

    df = pd.DataFrame(records)
    df = df.sort_values(by="user_id").reset_index(drop=True)

    logger.info(f"users 清理完成，DataFrame 共 {len(df)} 筆資料")
    return df


def merge_todo_user_data(todo_df: pd.DataFrame, user_df: pd.DataFrame) -> pd.DataFrame:
    if todo_df.empty:
        logger.warning("todo_df 是空的，無法 merge")
        return pd.DataFrame()

    if user_df.empty:
        logger.warning("user_df 是空的，無法 merge")
        return pd.DataFrame()

    logger.info("開始 merge todo 與 user 資料")

    merged_df = pd.merge(
        left=todo_df,
        right=user_df,
        how="left",
        on="user_id",
        validate="many_to_one",
    )

    logger.info(f"merge 完成，merged DataFrame 共 {len(merged_df)} 筆資料")
    return merged_df


def save_dataframe(df: pd.DataFrame, output_file) -> None:
    if df.empty:
        logger.warning(f"DataFrame 是空的，略過輸出：{output_file}")
        return

    ensure_directory(output_file.parent)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    logger.info(f"已輸出 CSV：{output_file}")


def run_clean_and_merge_pipeline() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    todo_df = clean_todo_data()
    user_df = clean_user_data()
    merged_df = merge_todo_user_data(todo_df, user_df)

    save_dataframe(todo_df, TODO_PROCESSED_FILE)
    save_dataframe(user_df, USER_PROCESSED_FILE)
    save_dataframe(merged_df, MERGED_PROCESSED_FILE)

    return todo_df, user_df, merged_df