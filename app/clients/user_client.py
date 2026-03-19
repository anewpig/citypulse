import httpx


async def fetch_users(client: httpx.AsyncClient) -> list[dict]:
    response = await client.get("/users")
    response.raise_for_status()
    return response.json()