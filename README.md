# CityPulse

一個用來練習 Python 工程能力的多資料源資料管線專案。  
此專案會從公開 API 擷取 `todos` 與 `users` 資料，進行非同步抓取、原始資料保存、資料清理、資料整合與統計報表輸出。

---

## 專案目標

這個專案的重點不只是「抓到資料」，而是完整練習一個比較像正式專案的開發流程，包括：

- Python 專案結構設計
- function / module / service / client 拆分
- API 串接
- async 非同步批次抓取
- semaphore 併發限制
- retry 重試機制
- raw / processed / reports 分層
- pandas 清理與 merge
- logging
- CLI 參數操作
- pytest 測試

---

## 功能特色

### 1. 多資料源抓取
- 抓取 `todos`
- 抓取 `users`

### 2. 非同步資料抓取
- 使用 `httpx.AsyncClient`
- 使用 `asyncio`
- 支援批次抓取 todo 資料

### 3. 穩定性設計
- semaphore 控制最大併發數
- retry 重試機制
- timeout / network error / HTTP error 處理
- logging 記錄執行流程

### 4. 資料分層
- `raw`：原始 JSON
- `processed`：清理後 CSV
- `reports`：統計報表

### 5. pandas 資料處理
- JSON 轉 DataFrame
- 巢狀 JSON 展平
- merge 多資料源
- groupby / agg 產出統計報表

---

## 專案結構

```text
citypulse/
├── app/
│   ├── __init__.py
│   ├── config.py
│   ├── logger.py
│   ├── clients/
│   │   ├── __init__.py
│   │   ├── todo_client.py
│   │   └── user_client.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── fetch_service.py
│   │   ├── clean_service.py
│   │   └── report_service.py
│   └── utils/
│       ├── __init__.py
│       └── file_utils.py
├── data/
│   ├── raw/
│   │   ├── todos/
│   │   └── users/
│   ├── processed/
│   └── reports/
├── logs/
├── tests/
│   ├── conftest.py
│   ├── test_clean_service.py
│   └── test_report_service.py
├── .gitignore
├── main.py
├── pytest.ini
├── README.md
└── requirements.txt