import httpx


async def fetch_todo(client: httpx.AsyncClient, todo_id: int) -> dict:
    response = await client.get(f"/todos/{todo_id}")
    response.raise_for_status()
    return response.json()