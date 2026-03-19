import argparse
import asyncio

from app.config import DEFAULT_CONCURRENCY, DEFAULT_MAX_RETRIES
from app.logger import setup_logger
from app.services.clean_service import run_clean_and_merge_pipeline
from app.services.fetch_service import fetch_all_raw_data
from app.services.report_service import run_report_pipeline

logger = setup_logger()


def parse_args():
    parser = argparse.ArgumentParser(description="CityPulse Multi-Source Data Pipeline")

    parser.add_argument("--start-id", type=int, required=True, help="起始 todo id")
    parser.add_argument("--end-id", type=int, required=True, help="結束 todo id")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="最大同時併發數",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="單筆請求最大重試次數",
    )

    return parser.parse_args()


async def async_main(start_id: int, end_id: int, concurrency: int, max_retries: int):
    if start_id > end_id:
        logger.error("start-id 不能大於 end-id")
        print("錯誤：start-id 不能大於 end-id")
        return

    if concurrency <= 0:
        logger.error("concurrency 必須大於 0")
        print("錯誤：concurrency 必須大於 0")
        return

    if max_retries <= 0:
        logger.error("max-retries 必須大於 0")
        print("錯誤：max-retries 必須大於 0")
        return

    logger.info("=== 第一步：抓取 todos + users raw data ===")
    todos, users = await fetch_all_raw_data(
        start_id=start_id,
        end_id=end_id,
        concurrency=concurrency,
        max_retries=max_retries,
    )

    if not todos:
        logger.error("todo 抓取失敗，程式結束")
        print("todo 抓取失敗，程式結束")
        return

    if not users:
        logger.error("user 抓取失敗，程式結束")
        print("user 抓取失敗，程式結束")
        return

    print(f"成功抓到 todos: {len(todos)} 筆")
    print(f"成功抓到 users: {len(users)} 筆")
    print()

    logger.info("=== 第二步：清理並 merge 資料 ===")
    todo_df, user_df, merged_df = run_clean_and_merge_pipeline()

    if todo_df.empty or user_df.empty or merged_df.empty:
        logger.error("清理或 merge 後沒有資料，程式結束")
        print("清理或 merge 後沒有資料，程式結束")
        return

    logger.info("=== 第三步：產出統計報表 ===")
    report_results = run_report_pipeline(merged_df)

    print("=== merged_df 預覽 ===")
    print(merged_df.head())
    print()

    print("=== todo_count_by_user 報表預覽 ===")
    print(report_results["todo_count_by_user_df"].head())
    print()

    print("=== completion_rate_by_user 報表預覽 ===")
    print(report_results["completion_rate_by_user_df"].head())
    print()

    print("=== final_user_summary 預覽 ===")
    print(report_results["final_user_summary_df"].head())
    print()

    print("=== summary_report ===")
    print(report_results["summary_report"])
    print()

    print("流程完成")
    print("已輸出 processed 檔案：")
    print("- data/processed/todos_cleaned.csv")
    print("- data/processed/users_cleaned.csv")
    print("- data/processed/todo_user_merged.csv")
    print()
    print("已輸出 reports 檔案：")
    print("- data/reports/todo_count_by_user.csv")
    print("- data/reports/todo_count_by_city.csv")
    print("- data/reports/completion_rate_by_user.csv")
    print("- data/reports/completion_rate_by_company.csv")
    print("- data/reports/summary_report.json")
    print("- data/reports/final_user_summary.csv")


def main():
    args = parse_args()
    asyncio.run(
        async_main(
            start_id=args.start_id,
            end_id=args.end_id,
            concurrency=args.concurrency,
            max_retries=args.max_retries,
        )
    )


if __name__ == "__main__":
    main()