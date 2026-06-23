import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_returns_200_when_services_up(client: AsyncClient):
    r = await client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "healthy"
    assert data["services"]["mongodb"] == "healthy"
    assert data["services"]["redis"] == "healthy"
