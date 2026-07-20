import os
import asyncio
import uuid
import traceback
import httpx
import asyncpg
from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
from dotenv import load_dotenv
from bolna.helpers.utils import store_file
from bolna.prompts import *
from bolna.helpers.logger_config import configure_logger
from bolna.models import *
from bolna.llms import LiteLLM
from bolna.agent_manager.assistant_manager import AssistantManager

load_dotenv()
logger = configure_logger(__name__)

# --- CRM call-transcript webhook ---------------------------------------------
# Posts the finished conversation to the koi-crm /api/calls/end endpoint so it
# can run its post-call AI analysis (intent, risk, summary). Opt-in via env —
# if either var is unset, the webhook is skipped entirely rather than failing.
KOI_CRM_URL = os.getenv("KOI_CRM_URL", "http://localhost:3000")
KOI_CRM_ORGANIZATION_ID = os.getenv("KOI_CRM_ORGANIZATION_ID")
CALLS_WEBHOOK_SECRET = os.getenv("CALLS_WEBHOOK_SECRET")


async def send_call_transcript_to_crm(task_output: dict):
    """Fire-and-log POST of the finished call to koi-crm. Never raises —
    a webhook failure must not affect call teardown."""
    if not KOI_CRM_ORGANIZATION_ID or not CALLS_WEBHOOK_SECRET:
        logger.info("KOI_CRM_ORGANIZATION_ID/CALLS_WEBHOOK_SECRET not set, skipping calls/end webhook")
        return

    messages = task_output.get("messages") or []
    transcript = [
        {"role": m.get("role"), "content": m.get("content")}
        for m in messages
        if m.get("role") != "system" and m.get("content")
    ]
    if not transcript:
        logger.info("No conversation turns to send to CRM, skipping calls/end webhook")
        return

    payload = {
        "organizationId": KOI_CRM_ORGANIZATION_ID,
        "phone": task_output.get("from_number"),
        "duration": task_output.get("conversation_time"),
        "direction": "inbound",
        "recordingUrl": task_output.get("recording_url"),
        "transcript": transcript,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{KOI_CRM_URL.rstrip('/')}/api/calls/end",
                json=payload,
                headers={"x-webhook-secret": CALLS_WEBHOOK_SECRET},
            )
            if resp.status_code >= 300:
                logger.error(f"calls/end webhook returned {resp.status_code}: {resp.text}")
            else:
                logger.info(f"Sent call transcript to CRM: {resp.status_code}")
    except Exception as e:
        logger.error(f"Failed to send call transcript to CRM: {e}")


# --- Appointment availability check (koi-crm's Neon DB) ----------------------
# Called mid-call by the agent's api_tools function-calling. Read-only — never
# books anything, just answers "is this doctor/slot available, and if not,
# what's the closest alternative".
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
_neon_pool = None
ALT_SEARCH_DAYS = 7
PREFETCH_WINDOW_DAYS = 7
PREFETCH_CACHE_TTL_S = 600  # a call's lifetime; refreshed on every new call
IST = ZoneInfo("Asia/Kolkata")


async def get_neon_pool():
    global _neon_pool
    if _neon_pool is None:
        _neon_pool = await asyncpg.create_pool(NEON_DATABASE_URL, min_size=1, max_size=5)
    return _neon_pool


def _parse_hhmm(s):
    h, m = s.split(":")
    return dt_time(int(h), int(m))


def _org_from_agent_config(agent_config):
    try:
        return agent_config["tasks"][0]["tools_config"]["api_tools"]["tools_params"][
            "check_appointment_availability"]["param"]["organization_id"]
    except (KeyError, IndexError, TypeError):
        return None


async def prefetch_availability_window(org_id):
    """Fired at call start: pull everything the availability check needs for the
    next PREFETCH_WINDOW_DAYS into redis, so the mid-call tool answers from cache
    (~0ms) instead of round-tripping to Neon (~250ms/RTT to us-east-1). Runs while
    the greeting/info-capture phases play, so its own latency is off-call-path."""
    if not NEON_DATABASE_URL:
        return
    try:
        today = datetime.now(IST).date()
        window_end = today + timedelta(days=PREFETCH_WINDOW_DAYS)
        pool = await get_neon_pool()
        async with pool.acquire() as conn:
            doctors = await conn.fetch(
                'SELECT id, "displayName", specialization, "defaultDuration", "bufferMinutes" '
                'FROM "TeamMember" WHERE "organizationId"=$1 AND "isActive"=true',
                org_id,
            )
            ids = [d["id"] for d in doctors]
            scheds = await conn.fetch(
                'SELECT "teamMemberId", "dayOfWeek", "startTime", "endTime" '
                'FROM "TeamMemberSchedule" WHERE "teamMemberId"=ANY($1) AND "isActive"=true',
                ids,
            )
            excs = await conn.fetch(
                'SELECT "teamMemberId", date, "isBlocked", "startTime", "endTime" '
                'FROM "TeamMemberException" WHERE "teamMemberId"=ANY($1) AND date BETWEEN $2 AND $3',
                ids, today, window_end,
            )
            appts = await conn.fetch(
                'SELECT "teamMemberId", "startTime", "endTime" FROM "Appointment" '
                "WHERE \"teamMemberId\"=ANY($1) AND status NOT IN ('CANCELLED', 'RESCHEDULED') "
                'AND "startTime"::date BETWEEN $2 AND $3',
                ids, today, window_end,
            )

        schedules, exceptions, appointments = {}, {}, {}
        for s in scheds:
            schedules.setdefault(s["teamMemberId"], {})[s["dayOfWeek"]] = [s["startTime"], s["endTime"]]
        for e in excs:
            exceptions.setdefault(e["teamMemberId"], {})[e["date"].isoformat()] = {
                "isBlocked": e["isBlocked"], "startTime": e["startTime"], "endTime": e["endTime"],
            }
        for a in appts:
            appointments.setdefault(a["teamMemberId"], []).append(
                [a["startTime"].isoformat(), a["endTime"].isoformat()]
            )

        cache = {
            "window_start": today.isoformat(),
            "window_end": window_end.isoformat(),
            "doctors": [dict(d) for d in doctors],
            "schedules": schedules,
            "exceptions": exceptions,
            "appointments": appointments,
        }
        await redis_client.set(f"avail_cache:{org_id}", json.dumps(cache), ex=PREFETCH_CACHE_TTL_S)
        logger.info(f"Prefetched availability window for {org_id}: {len(doctors)} doctors, {today} to {window_end}")
    except Exception as e:
        logger.error(f"Availability prefetch failed for {org_id} (tool will fall back to live Neon queries): {e}")


def _slot_free_cached(cache, doctor, requested_date, requested_time):
    """Pure in-memory version of _slot_free_for, against the prefetched window."""
    tm_id = doctor["id"]
    duration = doctor["defaultDuration"] or 30
    buffer_min = doctor["bufferMinutes"] or 0

    exc = cache["exceptions"].get(tm_id, {}).get(requested_date.isoformat())
    if exc and exc["isBlocked"]:
        return False, "doctor is off that day"

    if exc and exc["startTime"] and exc["endTime"]:
        work_start, work_end = _parse_hhmm(exc["startTime"]), _parse_hhmm(exc["endTime"])
    else:
        sched = cache["schedules"].get(tm_id, {}).get(requested_date.strftime("%A").upper())
        if not sched:
            return False, "doctor doesn't work that day"
        work_start, work_end = _parse_hhmm(sched[0]), _parse_hhmm(sched[1])

    req_start_dt = datetime.combine(requested_date, requested_time)
    req_end_dt = req_start_dt + timedelta(minutes=duration + buffer_min)
    if requested_time < work_start or req_end_dt.time() > work_end:
        return False, "outside working hours"

    for start_s, end_s in cache["appointments"].get(tm_id, []):
        appt_start, appt_end = datetime.fromisoformat(start_s), datetime.fromisoformat(end_s)
        if appt_start.date() != requested_date:
            continue
        if req_start_dt < appt_end and appt_start < req_end_dt:
            return False, "slot already booked"

    return True, "available"


# Single round trip instead of 3 sequential ones (exception + schedule + conflicts).
# Each round trip to Neon costs real network latency on top of query time, so cutting
# 3 awaits down to 1 matters more than the query itself being trivial.
_SLOT_QUERY = """
SELECT
    e."isBlocked" AS exc_blocked,
    e."startTime" AS exc_start,
    e."endTime" AS exc_end,
    s."startTime" AS sched_start,
    s."endTime" AS sched_end,
    COALESCE(
        (SELECT json_agg(json_build_object('startTime', a."startTime", 'endTime', a."endTime"))
         FROM "Appointment" a
         WHERE a."teamMemberId" = $1
           AND a.status NOT IN ('CANCELLED', 'RESCHEDULED')
           AND a."startTime"::date = $2),
        '[]'
    ) AS conflicts
FROM (SELECT 1) AS dummy
LEFT JOIN "TeamMemberException" e ON e."teamMemberId" = $1 AND e.date = $2
LEFT JOIN "TeamMemberSchedule" s ON s."teamMemberId" = $1 AND s."dayOfWeek" = $3 AND s."isActive" = true
"""


async def _slot_free_for(pool, doctor, requested_date, requested_time):
    """Check one doctor's availability for one date+time. Returns (bool, reason).

    Acquires its own connection so callers can run several of these concurrently
    via asyncio.gather rather than one at a time.
    """
    tm_id = doctor["id"]
    duration = doctor["defaultDuration"] or 30
    buffer_min = doctor["bufferMinutes"] or 0
    day_of_week = requested_date.strftime("%A").upper()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(_SLOT_QUERY, tm_id, requested_date, day_of_week)

    if row["exc_blocked"]:
        return False, "doctor is off that day"

    if row["exc_start"] and row["exc_end"]:
        work_start, work_end = _parse_hhmm(row["exc_start"]), _parse_hhmm(row["exc_end"])
    elif row["sched_start"] and row["sched_end"]:
        work_start, work_end = _parse_hhmm(row["sched_start"]), _parse_hhmm(row["sched_end"])
    else:
        return False, "doctor doesn't work that day"

    req_start_dt = datetime.combine(requested_date, requested_time)
    req_end_dt = req_start_dt + timedelta(minutes=duration + buffer_min)
    if requested_time < work_start or req_end_dt.time() > work_end:
        return False, "outside working hours"

    conflicts = json.loads(row["conflicts"]) if isinstance(row["conflicts"], str) else (row["conflicts"] or [])
    for appt in conflicts:
        appt_start = datetime.fromisoformat(appt["startTime"])
        appt_end = datetime.fromisoformat(appt["endTime"])
        if req_start_dt < appt_end and appt_start < req_end_dt:
            return False, "slot already booked"

    return True, "available"


async def _find_alternative(pool, doctor, requested_date, requested_time):
    """Same doctor, same time-of-day, next few days — checked concurrently rather
    than one day at a time, since each check is now a single independent round trip."""
    candidate_dates = [requested_date + timedelta(days=offset) for offset in range(1, ALT_SEARCH_DAYS + 1)]
    results = await asyncio.gather(*[_slot_free_for(pool, doctor, d, requested_time) for d in candidate_dates])
    for alt_date, (ok, _) in zip(candidate_dates, results):
        if ok:
            return {"doctor": doctor["displayName"], "date": alt_date.isoformat(), "time": requested_time.strftime("%H:%M")}
    return None


class AvailabilityRequest(BaseModel):
    organization_id: str
    doctor_name: Optional[str] = None
    issue: Optional[str] = None
    date: str  # YYYY-MM-DD
    time: str  # HH:MM, 24h


def _check_from_cache(cache, req, requested_date, requested_time):
    """Serve the availability check entirely from the prefetched window. Returns the
    response dict, or None if this request can't be answered from cache."""
    window_start = datetime.strptime(cache["window_start"], "%Y-%m-%d").date()
    window_end = datetime.strptime(cache["window_end"], "%Y-%m-%d").date()
    if not (window_start <= requested_date <= window_end):
        return None

    all_doctors = cache["doctors"]
    doctors, name_matched = [], False
    if req.doctor_name:
        needle = req.doctor_name.lower()
        doctors = [d for d in all_doctors if needle in (d["displayName"] or "").lower()]
        name_matched = bool(doctors)
    if not doctors:
        doctors = list(all_doctors)
        if req.issue and doctors:
            issue_lower = req.issue.lower()
            matched = [d for d in doctors if d["specialization"] and d["specialization"].lower() in issue_lower]
            if matched:
                doctors = matched
            else:
                physicians = [d for d in doctors if d["specialization"] and "physician" in d["specialization"].lower()]
                if physicians:
                    doctors = physicians
    if not doctors:
        return {"available": False, "message": "No matching doctor found for this clinic."}

    for doctor in doctors:
        ok, _ = _slot_free_cached(cache, doctor, requested_date, requested_time)
        if ok:
            return {
                "available": True,
                "doctor": {"name": doctor["displayName"], "specialization": doctor["specialization"]},
                "date": req.date,
                "time": req.time,
                "message": f"{doctor['displayName']} is available on {req.date} at {req.time}.",
            }

    # Alternatives: same doctors, same time-of-day, later days within the cached window.
    alternatives = []
    for doctor in doctors:
        for offset in range(1, ALT_SEARCH_DAYS + 1):
            alt_date = requested_date + timedelta(days=offset)
            if alt_date > window_end:
                break
            ok, _ = _slot_free_cached(cache, doctor, alt_date, requested_time)
            if ok:
                alternatives.append(
                    {"doctor": doctor["displayName"], "date": alt_date.isoformat(), "time": req.time}
                )
                break

    if name_matched:
        for doctor in all_doctors:
            if doctor in doctors:
                continue
            ok, _ = _slot_free_cached(cache, doctor, requested_date, requested_time)
            if ok:
                alternatives.append({
                    "doctor": doctor["displayName"],
                    "specialization": doctor["specialization"],
                    "date": req.date,
                    "time": req.time,
                })

    return {
        "available": False,
        "message": "Requested slot is not available.",
        "alternatives": alternatives[:3],
    }


async def _check_availability_impl(req: AvailabilityRequest):
    try:
        requested_date = datetime.strptime(req.date, "%Y-%m-%d").date()
        requested_time = _parse_hhmm(req.time)
    except ValueError:
        return {"available": False, "message": "Could not understand the requested date/time."}

    # Cache-first: answer from the window prefetched at call start when possible.
    # Out-of-window dates (or a missing/expired cache) fall through to live Neon.
    try:
        raw = await redis_client.get(f"avail_cache:{req.organization_id}")
        if raw:
            result = _check_from_cache(json.loads(raw), req, requested_date, requested_time)
            if result is not None:
                logger.info("Availability served from prefetched cache")
                return result
    except Exception as e:
        logger.warning(f"Availability cache read failed, falling back to Neon: {e}")

    pool = await get_neon_pool()

    doctors = []
    name_matched = False
    if req.doctor_name:
        async with pool.acquire() as conn:
            doctors = await conn.fetch(
                'SELECT id, "displayName", specialization, "defaultDuration", "bufferMinutes" '
                'FROM "TeamMember" WHERE "organizationId"=$1 AND "isActive"=true AND "displayName" ILIKE $2',
                req.organization_id, f"%{req.doctor_name}%",
            )
        name_matched = bool(doctors)

    if not doctors:
        # No doctor named, or the named lookup found nothing (script mismatch,
        # mishearing, typo) — fall back to matching by issue/specialization
        # across all active doctors rather than dead-ending the caller.
        async with pool.acquire() as conn:
            doctors = await conn.fetch(
                'SELECT id, "displayName", specialization, "defaultDuration", "bufferMinutes" '
                'FROM "TeamMember" WHERE "organizationId"=$1 AND "isActive"=true',
                req.organization_id,
            )
        if req.issue and doctors:
            issue_lower = req.issue.lower()
            matched = [d for d in doctors if d["specialization"] and d["specialization"].lower() in issue_lower]
            if matched:
                doctors = matched
            else:
                physicians = [d for d in doctors if d["specialization"] and "physician" in d["specialization"].lower()]
                if physicians:
                    doctors = physicians

    if not doctors:
        return {"available": False, "message": "No matching doctor found for this clinic."}

    # Check all candidate doctors concurrently (independent round trips) rather than
    # one at a time, then take the first available in the original priority order.
    checks = await asyncio.gather(*[_slot_free_for(pool, d, requested_date, requested_time) for d in doctors])
    for doctor, (ok, reason) in zip(doctors, checks):
        if ok:
            return {
                "available": True,
                "doctor": {"name": doctor["displayName"], "specialization": doctor["specialization"]},
                "date": req.date,
                "time": req.time,
                "message": f"{doctor['displayName']} is available on {req.date} at {req.time}.",
            }

    # Requested slot not available with any matching doctor — look for alternatives:
    # (1) same doctor(s), same time-of-day, next few days
    # (2) any other active doctor free at the exact requested date/time
    alt_results = await asyncio.gather(*[_find_alternative(pool, d, requested_date, requested_time) for d in doctors])
    alternatives = [alt for alt in alt_results if alt]

    if name_matched:
        async with pool.acquire() as conn:
            other_doctors = await conn.fetch(
                'SELECT id, "displayName", specialization, "defaultDuration", "bufferMinutes" '
                'FROM "TeamMember" WHERE "organizationId"=$1 AND "isActive"=true AND "displayName" NOT ILIKE $2',
                req.organization_id, f"%{req.doctor_name}%",
            )
        if other_doctors:
            other_checks = await asyncio.gather(
                *[_slot_free_for(pool, d, requested_date, requested_time) for d in other_doctors]
            )
            for doctor, (ok, _) in zip(other_doctors, other_checks):
                if ok:
                    alternatives.append({
                        "doctor": doctor["displayName"],
                        "specialization": doctor["specialization"],
                        "date": req.date,
                        "time": req.time,
                    })

    return {
        "available": False,
        "message": "Requested slot is not available.",
        "alternatives": alternatives[:3],
    }


redis_pool = redis.ConnectionPool.from_url(os.getenv("REDIS_URL"), decode_responses=True)
redis_client = redis.Redis.from_pool(redis_pool)
active_websockets: List[WebSocket] = []

app = FastAPI()

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)


@app.on_event("startup")
async def _warm_neon_pool():
    # Without this, the first check_appointment_availability call of the process
    # pays the full asyncpg connection-establishment cost on top of the tool-call
    # round-trip, showing up as an 10s+ latency spike on some caller's live turn.
    if NEON_DATABASE_URL:
        try:
            await get_neon_pool()
            logger.info("Neon connection pool warmed at startup")
        except Exception as e:
            logger.error(f"Failed to warm Neon pool at startup: {e}")


@app.post("/tools/check-availability")
async def check_availability(req: AvailabilityRequest):
    return await _check_availability_impl(req)


class CreateAgentPayload(BaseModel):
    agent_config: AgentModel
    agent_prompts: Optional[Dict[str, Dict[str, str]]]


@app.get("/agent/{agent_id}")
async def get_agent(agent_id: str):
    """Fetches an agent's information by ID."""
    try:
        agent_data = await redis_client.get(agent_id)
        if not agent_data:
            raise HTTPException(status_code=404, detail="Agent not found")

        return json.loads(agent_data)

    except Exception as e:
        logger.error(f"Error fetching agent {agent_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/agent")
async def create_agent(agent_data: CreateAgentPayload):
    agent_uuid = str(uuid.uuid4())
    data_for_db = agent_data.agent_config.model_dump()
    data_for_db["assistant_status"] = "seeding"
    agent_prompts = agent_data.agent_prompts
    logger.info(f"Data for DB {data_for_db}")

    if len(data_for_db["tasks"]) > 0:
        logger.info("Setting up follow up tasks")
        for index, task in enumerate(data_for_db["tasks"]):
            if task["task_type"] == "extraction":
                extraction_prompt_llm = os.getenv("EXTRACTION_PROMPT_GENERATION_MODEL")
                extraction_prompt_generation_llm = LiteLLM(model=extraction_prompt_llm, max_tokens=2000)
                extraction_prompt = await extraction_prompt_generation_llm.generate(
                    messages=[
                        {"role": "system", "content": EXTRACTION_PROMPT_GENERATION_PROMPT},
                        {
                            "role": "user",
                            "content": data_for_db["tasks"][index]["tools_config"]["llm_agent"]["extraction_details"],
                        },
                    ]
                )
                data_for_db["tasks"][index]["tools_config"]["llm_agent"]["extraction_json"] = extraction_prompt

    stored_prompt_file_path = f"{agent_uuid}/conversation_details.json"
    await asyncio.gather(
        redis_client.set(agent_uuid, json.dumps(data_for_db)),
        store_file(file_key=stored_prompt_file_path, file_data=agent_prompts, local=True),
    )

    return {"agent_id": agent_uuid, "state": "created"}


@app.put("/agent/{agent_id}")
async def edit_agent(agent_id: str, agent_data: CreateAgentPayload = Body(...)):
    """Edits an existing agent based on the provided agent_id."""
    try:
        existing_data = await redis_client.get(agent_id)
        if not existing_data:
            raise HTTPException(status_code=404, detail="Agent not found")

        existing_data = json.loads(existing_data)

        new_data = agent_data.agent_config.model_dump()
        new_data["assistant_status"] = "updated"
        agent_prompts = agent_data.agent_prompts

        logger.info(f"Updating Agent {agent_id}: {new_data}")

        for index, task in enumerate(new_data.get("tasks", [])):
            if task.get("task_type") == "extraction":
                extraction_prompt_llm = os.getenv("EXTRACTION_PROMPT_GENERATION_MODEL")
                if not extraction_prompt_llm:
                    raise HTTPException(status_code=500, detail="Extraction model not configured")

                extraction_prompt_generation_llm = LiteLLM(model=extraction_prompt_llm, max_tokens=2000)
                extraction_details = task["tools_config"]["llm_agent"].get("extraction_details", "")

                extraction_prompt = await extraction_prompt_generation_llm.generate(
                    messages=[
                        {"role": "system", "content": EXTRACTION_PROMPT_GENERATION_PROMPT},
                        {"role": "user", "content": extraction_details},
                    ]
                )

                new_data["tasks"][index]["tools_config"]["llm_agent"]["extraction_json"] = extraction_prompt

        stored_prompt_file_path = f"{agent_id}/conversation_details.json"
        await asyncio.gather(
            redis_client.set(agent_id, json.dumps(new_data)),
            store_file(file_key=stored_prompt_file_path, file_data=agent_prompts, local=True),
        )

        return {"agent_id": agent_id, "state": "updated"}

    except Exception as e:
        logger.error(f"Error updating agent {agent_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.delete("/agent/{agent_id}")
async def delete_agent(agent_id: str):
    """Deletes an agent by ID."""
    try:
        agent_exists = await redis_client.exists(agent_id)
        if not agent_exists:
            raise HTTPException(status_code=404, detail="Agent not found")

        await redis_client.delete(agent_id)
        return {"agent_id": agent_id, "state": "deleted"}

    except Exception as e:
        logger.error(f"Error deleting agent {agent_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/all")
async def get_all_agents():
    """Fetches all agents stored in Redis."""
    try:
        agent_keys = await redis_client.keys("*")

        if not agent_keys:
            return {"agents": []}
        agents_data = []
        for key in agent_keys:
            try:
                data = await redis_client.get(key)
                agents_data.append(data)
            except Exception as e:
                logger.error(f"An error occurred with key {key}: {e}")

        agents = [{"agent_id": key, "data": json.loads(data)} for key, data in zip(agent_keys, agents_data) if data]

        return {"agents": agents}

    except Exception as e:
        logger.error(f"Error fetching all agents: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


#############################################################################################
# Websocket
#############################################################################################
@app.websocket("/chat/v1/{agent_id}")
async def websocket_endpoint(agent_id: str, websocket: WebSocket, user_agent: str = Query(None)):
    logger.info("Connected to ws")
    await websocket.accept()
    active_websockets.append(websocket)
    agent_config, context_data = None, None
    try:
        retrieved_agent_config = await redis_client.get(agent_id)
        logger.info(f"Retrieved agent config: {retrieved_agent_config}")
        agent_config = json.loads(retrieved_agent_config)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail="Agent not found")

    # Warm the availability cache in parallel with the greeting — by the time the
    # caller asks for a slot, the check answers from redis instead of Neon.
    prefetch_org = _org_from_agent_config(agent_config)
    if prefetch_org:
        asyncio.create_task(prefetch_availability_window(prefetch_org))

    assistant_manager = AssistantManager(agent_config, websocket, agent_id)

    try:
        async for index, task_output in assistant_manager.run(local=True):
            logger.info(task_output)
            if index == 0:
                await send_call_transcript_to_crm(task_output)
    except WebSocketDisconnect:
        active_websockets.remove(websocket)
    except Exception as e:
        traceback.print_exc()
        logger.error(f"error in executing {e}")
