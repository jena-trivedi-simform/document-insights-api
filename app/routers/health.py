import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.cache import get_redis
from app.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check() -> JSONResponse:
    services: dict[str, str] = {}
    overall = "healthy"

    try:
        await get_db().command("ping")
        services["mongodb"] = "healthy"
    except Exception as exc:
        logger.error("MongoDB health check failed: %s", exc)
        services["mongodb"] = "unhealthy"
        overall = "degraded"

    try:
        await get_redis().ping()
        services["redis"] = "healthy"
    except Exception as exc:
        logger.error("Redis health check failed: %s", exc)
        services["redis"] = "unhealthy"
        overall = "degraded"

    http_status = 200 if overall == "healthy" else 503
    return JSONResponse(
        status_code=http_status,
        content={"status": overall, "services": services},
    )
