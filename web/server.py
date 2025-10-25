from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, render_template
from src.agent import build_agent

app = Flask(__name__, static_folder="static", template_folder="templates")
AGENT = build_agent()

def _safe_text(payload):
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        for k in ("text", "message", "query", "input", "prompt"):
            v = payload.get(k)
            if isinstance(v, str):
                return v
    return str(payload)

@app.get("/")
def index():
    return render_template("index.html")

@app.post("/chat")
def chat():
    data = request.get_json(silent=True) or {}
    text = _safe_text(data)
    res = AGENT.invoke(text if text else data)
    if isinstance(res, str):
        res = {"text": res, "suggestions": []}
    return jsonify(res)

@app.get("/health")
def health():
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
