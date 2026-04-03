import logging
import random
from typing import Any

from odoo_connect.explore import explore

from config import get_odoo_client
from config.logger import setup_job_logging

logger = logging.getLogger("app")
TRACKING_FIELDS = ["email", "phone", "bvn", "customer_id"]


def get_env():
    return get_odoo_client()


def run_risk_assessment(p_customer_id: int) -> Any:
    if p_customer_id <= 0:
        raise ValueError("p_customer_id must be a positive integer")
    env = get_env()
    partner_model = env["res.partner"]
    return partner_model.action_compute_risk_score_with_plan([p_customer_id])


def run_risk_assessment_safe(partner_id: int, customer_id: str | None) -> None:
    try:
        run_risk_assessment(partner_id)
    except Exception:
        logger.exception(
            "Risk assessment failed for partner_id=%s customer_id=%s",
            partner_id,
            customer_id,
        )


def capture_changes(change: dict) -> Any:
    env = get_env()
    change_model = env["change.data.capture"]
    change_id = change_model.create(change)
    return change_id


def get_tracked_partner_fields() -> list[dict[str, Any]]:
    env = get_env()
    field_model = env["ir.model.fields"]
    field_ids = field_model.search(
        [
            ("model", "=", "res.partner"),
            ("tracking", "!=", None),
            ("tracking", "!=", 0),
            ("name", "in", TRACKING_FIELDS),
        ]
    )
    if not field_ids:
        return []
    return field_model.read(field_ids, ["name", "field_description"])


def normalize_comparison_value(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        if len(value) == 2 and not isinstance(value[0], (list, tuple, dict)):
            return value[0]
        return list(value)
    return value


def stringify_change_value(value: Any) -> str:
    if value in (None, False):
        return ""
    if isinstance(value, (list, tuple)):
        if len(value) == 2:
            return str(value[1])
        return ", ".join(str(item) for item in value)
    return str(value)


def capture_tracked_partner_changes(
    partner_id: int,
    old_record: dict[str, Any],
    new_record: dict[str, Any],
    tracked_fields: list[dict[str, Any]],
) -> None:
    for tracked_field in tracked_fields:
        field_name = tracked_field.get("name")
        if not field_name or field_name not in new_record:
            continue

        old_value = normalize_comparison_value(old_record.get(field_name))
        new_value = normalize_comparison_value(new_record.get(field_name))
        if old_value == new_value:
            continue

        change = {
            "name": f"Change in {field_name}",
            "model": "res.partner",
            "res_id": partner_id,
            "res_name": new_record.get("name", ""),
            "field_name": field_name,
            "old_val": stringify_change_value(old_record.get(field_name)),
            "new_val": stringify_change_value(new_record.get(field_name)),
        }
        capture_changes(change)


def build_partner_record(payload: dict[str, Any]) -> dict[str, Any]:
    customer_id = payload.get("id")
    email = payload.get("email") or (
        f"{payload.get('first_name', 'unknown').lower()}."
        f"{payload.get('last_name', 'unknown').lower()}@example.com"
    )
    return {
        "name": f"{payload.get('first_name')} {payload.get('last_name')}",
        "customer_id": customer_id,
        "firstname": payload.get("first_name"),
        "lastname": payload.get("last_name"),
        "phone": str(payload.get("phone_work")),
        "mobile": str(payload.get("phone_mobile")),
        "bvn": customer_id,
        "email": email,
        "dob": payload.get("birthdate")[:10] if payload.get("birthdate") else None,
        "registration_date": (
            payload.get("date_entered")[:10] if payload.get("date_entered") else None
        ),
    }


def build_create_partner_extra_fields() -> dict[str, Any]:
    country_ids = get_country_ids()
    sector_ids = get_sector_ids()
    education_level_ids = get_education_level_ids()
    branch_ids = get_branch_ids()
    branch_id = random.choice(branch_ids) if branch_ids else None
    return {
        "branch_id": branch_id,
        "region_id": get_branch_region_id(branch_id),
        "origin": "prod",
        "country_id": random.choice(country_ids) if country_ids else None,
        "sector_id": random.choice(sector_ids) if sector_ids else None,
        "education_level_id": (
            random.choice(education_level_ids) if education_level_ids else None
        ),
    }


def action_add_pep(p_customer_id: int) -> Any:
    if p_customer_id <= 0:
        raise ValueError("p_customer_id must be a positive integer")

    env = get_env()
    partner_model = env["res.partner"]
    return partner_model.action_add_pep([p_customer_id])


def get_total_partners():
    env = get_env()
    partner_model = env["res.partner"]
    return partner_model.search_count([])


def get_branch_ids():
    env = get_env()
    branches = explore(env["res.branch"])
    branch_ids = []
    for b in branches.search([]):
        branch_ids.append(b.id)
    return branch_ids


def get_branch(id):
    env = get_env()
    return explore(env["res.branch"]).search([("id", "=", id)], limit=1)


def get_branch_region_id(branch_id):
    if not branch_id:
        return None
    env = get_env()
    branch_model = env["res.branch"]
    branch_data = branch_model.read([branch_id], ["region_id"])
    if not branch_data:
        return None
    region_value = branch_data[0].get("region_id")
    if isinstance(region_value, (list, tuple)):
        return region_value[0] if region_value else None
    return region_value


def get_education_level_ids():
    env = get_env()
    education_levels = explore(env["res.education.level"])
    education_level_ids = []
    for e in education_levels.search([]):
        education_level_ids.append(e.id)
    return education_level_ids


def get_sector_ids():
    env = get_env()
    sectors = explore(env["res.partner.sector"])
    sector_ids = []
    for s in sectors.search([]):
        sector_ids.append(s.id)
    return sector_ids


def get_country_ids():
    env = get_env()
    countries = explore(env["res.country"])
    country_ids = []
    for c in countries.search([("code", "=", "NG")]):  # only NG
        country_ids.append(c.id)
    return country_ids


def get_partner_with_customer_id(customer_id):
    env = get_env()
    return explore(env["res.partner"]).search(
        [("customer_id", "=", customer_id)], limit=1
    )


def get_partner(partner_id):
    env = get_env()
    return explore(env["res.partner"]).search([("id", "=", partner_id)], limit=1)


def refresh(model, res_id):
    env = get_env()
    rec = explore(env[model]).search([("id", "=", res_id)], limit=1)
    if rec:
        rec.invalidate_cache()


def add_partner(payload: dict) -> int:
    setup_job_logging()
    customer_id = payload.get("id")
    try:
        logger.info("Starting partner sync for customer_id=%s", customer_id)
        env = get_env()
        partner_model = env["res.partner"]
        partner_record = build_partner_record(payload)
        existing_partner_ids = (
            partner_model.search([("customer_id", "=", customer_id)], limit=1)
            if customer_id
            else []
        )
        if existing_partner_ids:
            tracked_fields = get_tracked_partner_fields()
            tracked_field_names = [
                field["name"]
                for field in tracked_fields
                if field.get("name") in partner_record
            ]
            old_record = {}
            if tracked_field_names:
                old_records = partner_model.read(
                    existing_partner_ids, tracked_field_names
                )
                old_record = old_records[0] if old_records else {}

            partner_model.write(existing_partner_ids, partner_record)
            partner_id = existing_partner_ids[0]
            if tracked_fields and old_record:
                capture_tracked_partner_changes(
                    partner_id=partner_id,
                    old_record=old_record,
                    new_record=partner_record,
                    tracked_fields=tracked_fields,
                )
            run_risk_assessment_safe(partner_id, customer_id)
            logger.info(
                "Updated partner from sync customer_id=%s partner_id=%s",
                customer_id,
                partner_id,
            )
            return partner_id

        create_record = {**partner_record, **build_create_partner_extra_fields()}
        create_record["origin"] = "prod"
        partner_id = partner_model.create(create_record)
        run_risk_assessment_safe(partner_id, customer_id)
        logger.info(
            "Created partner from sync customer_id=%s partner_id=%s",
            customer_id,
            partner_id,
        )
        return partner_id
    except Exception:
        logger.exception("Partner sync failed for customer_id=%s", customer_id)
        raise
