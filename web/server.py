# web/server.py
from pathlib import Path
import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from src.agent import build_agent

ROOT = Path(__file__).resolve().parent
app = FastAPI()

# mount /static only if present
static_dir = ROOT / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

AGENT = build_agent()
INDEX_HTML = (ROOT / "index.html").read_text(encoding="utf-8")

@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(INDEX_HTML)

@app.post("/chat")
async def chat(req: Request):
    data = await req.json()
    msg = str(data.get("message", "")).strip()
    if not msg:
        return JSONResponse({"reply": "Please type a message."})
    try:
        out = AGENT.invoke({"input": msg})
        reply = out.get("output", str(out))
        return JSONResponse({"reply": reply})
    except Exception as e:
        return JSONResponse({"reply": f"Error: {e}"}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web.server:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
