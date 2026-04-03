import logging
from typing import Any

from fastapi import APIRouter, Body

from repo.partner import add_partner
from router.utils import get_rq_queue


router = APIRouter()
logger = logging.getLogger("app")

@router.post("/contacts")
async def sync_contacts(
    payload: Any = Body(...),
) -> dict[str, object]:
    job = get_rq_queue().enqueue(add_partner, dict(payload))
    logger.info(
        "Queued partner sync job_id=%s customer_id=%s",
        job.id,
        payload.get("id") if isinstance(payload, dict) else None,
    )
    return {"status": "accepted", "job_id": job.id}
