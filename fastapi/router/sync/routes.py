import logging
from typing import Any

from fastapi import APIRouter, Body

from repo.partner import add_partner
from router.utils import get_rq_queue


router = APIRouter()

@router.post("/contacts")
async def sync_contacts(
    payload: Any = Body(...),
) -> dict[str, object]:
    job = get_rq_queue().enqueue(add_partner, dict(payload))
    return {"status": "accepted", "job_id": job.id}
