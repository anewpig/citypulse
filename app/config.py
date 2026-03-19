from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
TODO_RAW_DIR = RAW_DIR / "todos"
USER_RAW_DIR = RAW_DIR / "users"
USER_RAW_FILE = USER_RAW_DIR / "users.json"

PROCESSED_DIR = DATA_DIR / "processed"
TODO_PROCESSED_FILE = PROCESSED_DIR / "todos_cleaned.csv"
USER_PROCESSED_FILE = PROCESSED_DIR / "users_cleaned.csv"
MERGED_PROCESSED_FILE = PROCESSED_DIR / "todo_user_merged.csv"

REPORT_DIR = DATA_DIR / "reports"
TODO_COUNT_BY_USER_FILE = REPORT_DIR / "todo_count_by_user.csv"
TODO_COUNT_BY_CITY_FILE = REPORT_DIR / "todo_count_by_city.csv"
COMPLETION_RATE_BY_USER_FILE = REPORT_DIR / "completion_rate_by_user.csv"
COMPLETION_RATE_BY_COMPANY_FILE = REPORT_DIR / "completion_rate_by_company.csv"
SUMMARY_REPORT_FILE = REPORT_DIR / "summary_report.json"

OUTPUT_DIR = DATA_DIR / "output"

LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "citypulse.log"

API_BASE_URL = "https://jsonplaceholder.typicode.com"
REQUEST_TIMEOUT = 10.0

DEFAULT_CONCURRENCY = 5
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0
FINAL_USER_SUMMARY_FILE = REPORT_DIR / "final_user_summary.csv"