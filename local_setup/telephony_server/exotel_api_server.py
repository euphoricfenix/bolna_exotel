from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Header, HTTPException
import httpx
import os
import asyncio
import json
import websockets
import redis.asyncio as redis
import xmltodict
from urllib.parse import urljoin, parse_qs
from dotenv import load_dotenv

app = FastAPI()
load_dotenv()

EXOTEL_SID      = os.environ["EXOTEL_SID"]
EXOTEL_API_KEY  = os.environ["EXOTEL_API_KEY"]
EXOTEL_API_TOKEN= os.environ["EXOTEL_API_TOKEN"]
EXOTEL_REGION   = os.environ["EXOTEL_REGION"]  # or api.exotel.com
EXOPHONE        = os.environ["EXOPHONE"]
APP_ID          = os.environ["EXOTEL_APP_ID"]
HOST_BASE       = os.environ["PUBLIC_BASE_URL"]  # e.g. https://your-host.example.com
REDIS_URL       = os.environ.get("REDIS_URL", "redis://redis:6379/0")
rds = redis.from_url(REDIS_URL, decode_responses=True)

# --- Inbound-call routing config ---------------------------------------------
# Fallback agent_id to use for inbound calls when no other routing info is
# available (single-agent / single-number setups). Optional.
DEFAULT_INBOUND_AGENT_ID = os.environ.get("DEFAULT_INBOUND_AGENT_ID")

# Optional JSON map of {"<exophone_number>": "<agent_id>"} for installs that
# route different inbound Exophone numbers to different agents. This is the
# PRIMARY inbound routing strategy (see note in stream_ws below) — it relies
# only on the 'to' field in Exotel's start event, which is always reliably
# present, unlike custom_parameters (see note on query params further down).
# Example: EXOPHONE_AGENT_MAP={"0123456789": "agent-uuid-1", "0987654321": "agent-uuid-2"}
try:
    EXOPHONE_AGENT_MAP = json.loads(os.environ.get("EXOPHONE_AGENT_MAP", "{}"))
except json.JSONDecodeError:
    print("Warning: EXOPHONE_AGENT_MAP is not valid JSON, ignoring it")
    EXOPHONE_AGENT_MAP = {}

# Fallback Bolna websocket host for non-ngrok / production deployments, e.g.
# wss://bolna.yourdomain.com . When set, this is used directly and ngrok
# tunnel discovery is skipped entirely (see resolve_bolna_host below) —
# ngrok discovery iv-only and would otherwise burn through Exotel's
# 10-second handshake timeout on every single call in production, since it
# always fails outside the local docker-compose stack.
BOLNA_WS_HOST = os.environ.get("BOLNA_WS_HOST")


async def populate_ngrok_tunnels():
    """Local-dev-only helper: resolves telephony/bolna public URLs via the
    ngrok container's local API. Will fail (by design) in any deployment
    that isn't the local_setup docker-compose stack.

    Uses httpx.AsyncClient (not requests) so this never blocks the event
    loop — important since this can now run per-call on the inbound path,
    not just on the outbound /exotel/call trigger.
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get("http://ngrok:4040/api/tunnels")  # ngrok interface
    except httpx.RequestError as e:
        print(f"ngrok tunnel discovery unavailable: {e}")
        return None, None

    telephony_url, bolna_url = None, None

    if response.status_code == 200:
        data = response.json()

        for tunnel in data['tunnels']:
            if tunnel['name'] == 'exotel-app':
                telephony_url = tunnel['public_url']
            elif tunnel['name'] == 'bolna-app':
                bolna_url = tunnel['public_url'].replace('https:', 'wss:')

        return telephony_url, bolna_url
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")
        return None, None


async def resolve_bolna_host():
    """Best-effort resolution of the Bolna websocket host, used as a fallback
    when Redis has no routing entry (i.e. inbound calls).

    Production-safe ordering: a configured BOLNA_WS_HOST always wins and
    skips ngrok discovery entirely (which only ever succeeds inside the
    local docker-compose dev stack). This matters because Exotel allows
    only 10 seconds for the whole WS handshake to respond — every wasted
    second trying ngrok in prod eats into that budget for no benefit.
    """
    if BOLNA_WS_HOST:
        return BOLNA_WS_HOST
    _, bolna_host = await populate_ngrok_tunnels()
    return bolna_host


@app.post("/exotel/call")
async def start_call(request: Request):
    """Outbound trigger: Bolna -> Exotel REST API -> places a call.
    Unchanged from the original outbound flow (aside from the ngrok helper
    now being awaited, since it's async)."""
    try:
        call_details = await request.json()
        agent_id = call_details.get('agent_id')
        if not agent_id:
            raise HTTPException(status_code=404, detail="Agent not provided")

        if "recipient_phone_number" not in call_details:
            raise HTTPException(status_code=404, detail="Recipient phone number not provided")

        telephony_host, bolna_host = await populate_ngrok_tunnels()

        print(f'telephony_host: {telephony_host}')
        print(f'bolna_host: {bolna_host}')

        to_number = call_details['recipient_phone_number']

        # NOTE: Exotel docs use http:// for my.exotel.com
        url = f"http://my.exotel.com/{EXOTEL_SID}/exoml/start_voice/{APP_ID}"

        form = {
            "From": to_number,
            "CallerId": EXOPHONE,
            "Url": url,
            "StatusCallback": f"{HOST_BASE}/exotel/status-callback",
        }
        auth = (EXOTEL_API_KEY, EXOTEL_API_TOKEN)
        api = f"https://{EXOTEL_REGION}/v1/Accounts/{EXOTEL_SID}/Calls/connect.json"

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(api, data=form, auth=auth)
            text = resp.text
            try:
                data = resp.json()
            except Exception:
                data = xmltodict.parse(text)

        # Extract CallSid from either JSON or XML-ish structure
        call_sid = None
        try:
            call_sid = data["Call"]["Sid"]
        except Exception:
            try:
                call_sid = data["TwilioResponse"]["Call"]["Sid"]
            except Exception:
                pass

        # Persist routing for the WS stream
        if call_sid and agent_id:
            ttl = 600  # 10 min
            await rds.setex(f"exotel:call:{call_sid}:agent_id", ttl, agent_id)
            if bolna_host:
                await rds.setex(f"exotel:call:{call_sid}:bolna_host", ttl, bolna_host)

        return data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Exception occurred in start_call: {e}")
        print(f'telephony_host: {telephony_host}')
        print(f'bolna_host: {bolna_host}')
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.post("/exotel/status-callback")
async def status_callback(request: Request):
    form = await request.form()
    # Common fields include Status, CallSid, DateUpdated, RecordingUrl (if any)
    # Log & ack idempotently
    print("Exotel StatusCallback:", dict(form))
    return {"ok": True}


@app.api_route("/exotel/inbound-connect", methods=["GET", "POST"])
async def inbound_connect(request: Request):
    """Optional dynamic-mode endpoint for the Voicebot Applet's URL field.

    This is only needed if you configure the Applet with this HTTPS URL
    instead of a static wss:// URL. Exotel will call this and expects a
    JSON body of the form {"url": "wss://..."} per their docs. It lets you
    resolve agent_id per-call (e.g. by the dialed Exophone number) before
    handing back the websocket URL.

    If you instead use the simpler static method (recommended — see
    /exotel/stream below), you don't need this route at all: just point the
    Voicebot Applet directly at wss://<host>/exotel/stream and Exotel will
    pass the dialed 'to' number through on the 'start' event, which gets
    resolved via EXOPHONE_AGENT_MAP / DEFAULT_INBOUND_AGENT_ID below.
    """
    try:
        if request.method == "POST":
            try:
                payload = await request.json()
            except Exception:
                form = await request.form()
                payload = dict(form)
        else:
            payload = dict(request.query_params)

        # Exotel sends the dialed number under varying keys depending on
        # context; check t common ones.
        to_number = (
            payload.get("To")
            or payload.get("to")
            or payload.get("CallTo")
            or payload.get("CalledTo")
        )

        agent_id = EXOPHONE_AGENT_MAP.get(to_number) if to_number else None
        agent_id = agent_id or DEFAULT_INBOUND_AGENT_ID

        if not agent_id:
            raise HTTPException(
                status_code=500,
                detail="No agent_id could be resolved for this inbound call. "
                       "Set DEFAULT_INBOUND_AGENT_ID or EXOPHONE_AGENT_MAP.",
            )

        host = HOST_BASE.replace("https:", "wss:").replace("http:", "ws:")
        ws_url = f"{host}/exotel/stream?agent_id={agent_id}"

        return {"url": ws_url}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Exception occurred in inbound_connect: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.websocket("/exotel/stream")
async def stream_ws(ws: WebSocket):
    """Bi-directional bridge: Exotel Voicebot <-> Bolna chat WS.

    Handles both outbound calls (routing pre-populated in Redis by
    /exotel/call) and inbound calls (no Redis entry exists yet, since
    Exotel opens this socket directly per the Voicebot Applet config
    without ever hitting an HTTP route on this server first).
    """
    await ws.accept()

    # agent_id can arrive as a query param on the websocket URL itself, e.g.
    # wss://host/exotel/stream?agent_id=XYZ — this is how the static
    # Voicebot Applet method can pass routing info, and Exotel's own docs
    # confirm query params on a static URL get echoed back as
    # start.custom_parameters. NOTE: third-party integrators have reported
    # custom query params occasionally getting stripped in transit, so this
    # is treated as a secondary signal — EXOPHONE_AGENT_MAP (keyed off the
    # 'to' number, which always arrives reliably in the start event) is the
    # primary, more robust inbound routing strategy below.
    qsnt_id = None
    try:
        qs = parse_qs(ws.scope.get("query_string", b"").decode())
        qs_agent_id = (qs.get("agent_id") or [None])[0]
    except Exception as e:
        print("Error parsing query string on WS connect:", e)

    # --- 1) Read until 'start' (buffer early events like 'connected') ---
    prestart_buf = []
    start_evt, call_sid = None, None
    try:
        while True:
            raw = await ws.receive_text()
            evt = json.loads(raw)
            prestart_buf.append(raw)
            if evt.get("event") == "start":
                start_evt = evt
                s = evt.get("start") or {}
                call_sid = s.get("call_sid") or s.get("CallSid") or s.get("callSid")
                break
    except WebSocketDisconnect:
        return
    except Exception as e:
        print("WS pre-start error:", e)
        await ws.close(code=1011)
        return

    if not call_sid:
        await ws.send_text(json.dumps({"error": "No call_sid in start event"}))
        await ws.close(code=4400)
        return

    start_info = (start_evt.get("start") or {}) if start_evt else {}
    custom_params = start_info.get("custom_parameters") or {}
    to_number = start_info.get("to")

    # --- 2) Resolve routing: Redis first (outbound calls), then fall back
    #         to inbound-call sources in priority order. ---
    try:
        agent_id   = await rds.get(f"exotel:call:{call_sid}:agent_id")
        bolna_host = await rds.get(f"exotel:call:{call_sid}:bolna_host")
    except Exception as e:
        print("Redis error:", e)
        agent_id, bolna_host = None, None

    if not agent_id:
        # Inbound call: no /exotel/call ever ran for this call_sid, so
        # there's nothing in Redis. Resolve agent_id from, in priority
        # order:
        #   a) a per-Exophone mapping, keyed by the dialed 'to' number —
        #      PRIMARY strategy, since 'to' always arrives reliably in the
        #      start event (unlike custom_parameters, see note above)
        #   b) custom_parametersn the 'start' event (set via the
        #      Voicebot Applet's static wss:// URL, e.g. ?agent_id=XYZ)
        #   c) the agent_id query param on this websocket connection itself
        #   d) a single default inbound agent
        agent_id = (
            (EXOPHONE_AGENT_MAP.get(to_number) if to_number else None)
            or custom_params.get("agent_id")
            or qs_agent_id
            or DEFAULT_INBOUND_AGENT_ID
        )
        if agent_id:
            print(f"Inbound call {call_sid}: resolved agent_id={agent_id} "
                  f"(to={to_number})")

    if not bolna_host:
        bolna_host = await resolve_bolna_host()

    if not agent_id or not bolna_host:
        await ws.send_text(json.dumps({
            "error": "No route for this call (agent_id/bolna_host missing)"
        }))
        await ws.close(code=4404)
        return

    bolna_ws_url = f"{bolna_host.rstrip('/')}/chat/v1/{agent_id}"

    # --- 3) Connect to Bolna ---
    try:
        bolna = await websockets.connect(bolna_ws_url)
    except Exception as e:
        await ws.send_text(json.dumps({"error": f"Failed to connect to Bolna WS: {e}"}))
        await ws.close(code=1011)
        return

    # Forward buffered 'connected'/'start' to Bolna first (helps input handler init stream_id)
    for raw in prestart_buf:
        try:
            await bolna.send(raw)
        except Exception as e:
            print("Forward prestart error:", e)

    # --- 4) Define proxy loops (bi-directional for Voicebot) ---
    async def exotel_to_bolna():
        try:
            while True:
                msg = await ws.receive()
                if msg.get("text") is not None:
                    await bolna.send(msg["text"])
                elif msg.get("bytes") is not None:
                    # Exotel should send text JSON; forward bytes only if your Bolna handler expects it.
                    await bolna.send(msg["bytes"])
        except WebSocketDisconnect:
            pass
        except Exception as e:
            print("exotel_to_bolna error:", e)
        finally:
            try: await bolna.close()
            except: pass

    async def bolna_to_exotel():
        # Exotel expects text JSON frames: media/mark/clear. Drop binaries.
        try:
            while True:
                msg = await bolna.recv()
                if isinstance(msg, (bytes, bytearray)):
                    continue
                await ws.send_text(msg)
        except Exception as e:
            print("bolna_to_exotel error:", e)
        finally:
            try: await ws.close()
            except: pass

    # --- 5) Run both until one side closes ---
    t1 = asyncio.create_task(exotel_to_bolna())
    t2 = asyncio.create_task(bolna_to_exotel())
    done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)

    for t in pending:
        t.cancel()
        try: await t
        except asyncio.CancelledError: pass
    try: await bolna.close()
    except: pass
    try: await ws.close()
    except: pass
