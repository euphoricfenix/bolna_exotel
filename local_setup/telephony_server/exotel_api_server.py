
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Header, HTTPException
import httpx
import os
import asyncio
import json
import websockets
import requests
import redis.asyncio as redis
import xmltodict
from urllib.parse import urljoin
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

def populate_ngrok_tunnels():
    response = requests.get("http://ngrok:4040/api/tunnels")  # ngrok interface
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

@app.post("/exotel/call")
async def start_call(request: Request):
    try:
        call_details = await request.json()
        agent_id = call_details.get('agent_id')
        if not agent_id:
            raise HTTPException(status_code=404, detail="Agent not provided")

        if "recipient_phone_number" not in call_details:
            raise HTTPException(status_code=404, detail="Recipient phone number not provided")

        telephony_host, bolna_host = populate_ngrok_tunnels()
        
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
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.post("/exotel/status-callback")
async def status_callback(request: Request):
    form = await request.form()
    # Common fields include Status, CallSid, DateUpdated, RecordingUrl (if any)
    # Log & ack idempotently
    print("Exotel StatusCallback:", dict(form))
    return {"ok": True}


@app.websocket("/exotel/stream")
async def stream_ws(ws: WebSocket):
    """Bi-directional bridge: Exotel Voicebot <-> Bolna chat WS (via Redis routing)."""
    await ws.accept()

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

    # --- 2) Resolve routing from Redis (saved in /exotel/call) ---
    try:
        agent_id   = await rds.get(f"exotel:call:{call_sid}:agent_id")
        bolna_host = await rds.get(f"exotel:call:{call_sid}:bolna_host")
    except Exception as e:
        print("Redis error:", e)
        await ws.close(code=1011)
        return

    if not agent_id or not bolna_host:
        await ws.send_text(json.dumps({"error": "No route for this call (agent_id/bolna_host missing)"}))
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
