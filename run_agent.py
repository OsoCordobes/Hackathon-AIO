# run_agent.py
from src.agent import build_agent

def main():
    agent = build_agent()
    print("Jarvis agent ready. Type 'exit' to quit.")
    while True:
        try:
            q = input("> ").strip()
        except EOFError:
            break
        if q.lower() in {"exit", "quit"}:
            break
        resp = agent.invoke({"input": q})
        print(resp.get("output", resp))

if __name__ == "__main__":
    main()
