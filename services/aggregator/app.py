from fastapi import FastAPI, Request

app = FastAPI()


@app.post("/telemetry")
async def telemetry(request: Request):
    data = await request.json()
    # For now, we just log it. Later, this goes to ClickHouse.
    print(f"📡 Received from {data['source_ip']}: {data['payload']}")
    return {"status": "ok"}
