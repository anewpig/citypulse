import asyncio

import httpx

from app.clients.todo_client import fetch_todo
from app.clients.user_client import fetch_users
from app.config import (
    API_BASE_URL,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_DELAY,
    REQUEST_TIMEOUT,
    TODO_RAW_DIR,
    USER_RAW_FILE,
)
from app.logger import setup_logger
from app.utils.file_utils import save_json

logger = setup_logger()


async def fetch_todo_with_retry(
    client: httpx.AsyncClient,
    todo_id: int,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> dict:
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"todo {todo_id} 第 {attempt} 次嘗試抓取")
            todo = await fetch_todo(client, todo_id)
            return todo

        except httpx.TimeoutException as e:
            logger.warning(f"todo {todo_id} timeout：{e}")

        except httpx.NetworkError as e:
            logger.warning(f"todo {todo_id} network error：{e}")

        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            logger.warning(f"todo {todo_id} HTTP 錯誤：{status_code}")

            if 400 <= status_code < 500:
                logger.error(f"todo {todo_id} 是 4xx 錯誤，不重試")
                return {}

        except Exception as e:
            logger.error(f"todo {todo_id} 未知錯誤：{e}")
            return {}

        if attempt < max_retries:
            sleep_seconds = retry_delay * (2 ** (attempt - 1))
            logger.info(f"todo {todo_id} 將在 {sleep_seconds:.1f} 秒後重試")
            await asyncio.sleep(sleep_seconds)

    logger.error(f"todo {todo_id} 已達最大重試次數，放棄抓取")
    return {}


async def fetch_users_with_retry(
    client: httpx.AsyncClient,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> list[dict]:
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"users 第 {attempt} 次嘗試抓取")
            users = await fetch_users(client)
            return users

        except httpx.TimeoutException as e:
            logger.warning(f"users timeout：{e}")

        except httpx.NetworkError as e:
            logger.warning(f"users network error：{e}")

        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            logger.warning(f"users HTTP 錯誤：{status_code}")

            if 400 <= status_code < 500:
                logger.error("users 是 4xx 錯誤，不重試")
                return []

        except Exception as e:
            logger.error(f"users 未知錯誤：{e}")
            return []

        if attempt < max_retries:
            sleep_seconds = retry_delay * (2 ** (attempt - 1))
            logger.info(f"users 將在 {sleep_seconds:.1f} 秒後重試")
            await asyncio.sleep(sleep_seconds)

    logger.error("users 已達最大重試次數，放棄抓取")
    return []


async def fetch_and_save_todo(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    todo_id: int,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> dict:
    async with semaphore:
        logger.info(f"todo {todo_id} 進入 semaphore，開始處理")

        todo = await fetch_todo_with_retry(
            client=client,
            todo_id=todo_id,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

        if not todo:
            logger.warning(f"todo {todo_id} 最終抓取失敗")
            return {}

        output_file = TODO_RAW_DIR / f"todo_{todo_id}.json"
        save_json(output_file, todo)

        logger.info(f"todo {todo_id} 成功抓取並已儲存到：{output_file}")
        return todo


async def fetch_and_save_users(
    client: httpx.AsyncClient,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> list[dict]:
    users = await fetch_users_with_retry(
        client=client,
        max_retries=max_retries,
        retry_delay=retry_delay,
    )

    if not users:
        logger.warning("users 最終抓取失敗")
        return []

    save_json(USER_RAW_FILE, {"users": users})
    logger.info(f"users 成功抓取並已儲存到：{USER_RAW_FILE}")
    return users


async def batch_fetch_and_save_todos(
    client: httpx.AsyncClient,
    start_id: int,
    end_id: int,
    concurrency: int,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> list[dict]:
    logger.info(
        f"開始 async 批次抓取 todo，範圍：{start_id} ~ {end_id}，"
        f"最大併發：{concurrency}，最大重試：{max_retries}"
    )

    semaphore = asyncio.Semaphore(concurrency)

    tasks = [
        asyncio.create_task(
            fetch_and_save_todo(
                client=client,
                semaphore=semaphore,
                todo_id=todo_id,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )
        )
        for todo_id in range(start_id, end_id + 1)
    ]

    results = await asyncio.gather(*tasks)
    todos = [todo for todo in results if todo]

    logger.info(f"todo 批次抓取結束，共成功抓到 {len(todos)} 筆資料")
    return todos


async def fetch_all_raw_data(
    start_id: int,
    end_id: int,
    concurrency: int,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> tuple[list[dict], list[dict]]:
    async with httpx.AsyncClient(
        base_url=API_BASE_URL,
        timeout=REQUEST_TIMEOUT,
    ) as client:
        todo_task = asyncio.create_task(
            batch_fetch_and_save_todos(
                client=client,
                start_id=start_id,
                end_id=end_id,
                concurrency=concurrency,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )
        )

        user_task = asyncio.create_task(
            fetch_and_save_users(
                client=client,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )
        )

        todos, users = await asyncio.gather(todo_task, user_task)

    logger.info(
        f"raw data 抓取完成：todos={len(todos)} 筆，users={len(users)} 筆"
    )
    return todos, users