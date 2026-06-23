from enum import Enum


class DocumentStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

    @classmethod
    def active_statuses(cls) -> list["DocumentStatus"]:
        """Statuses that count toward a user's active-job rate limit."""
        return [cls.QUEUED, cls.PROCESSING]
