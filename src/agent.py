import os
import re
from typing import Any, List, Optional, Dict

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool

from src.agent_tools import (
    delayed_orders_md,
    plan_missing_sku_md,
    impacted_by_plant_md,
    order_details_md,
    plan_all_delayed_orders_md,
)

SYS = (
    "You are Jarvis, a helpful supply-chain assistant.\n"
    "• If the user asks for facts about orders/inventory/plants, call TOOLS and answer from CSV outputs.\n"
    "• If the message is greeting or small talk, reply naturally yourself.\n"
    "• Never invent data. When facts are needed, use a TOOL.\n\n"
    "TOOLS:\n"
    " - delayed_orders() -> delayed orders from orders.csv\n"
    " - plan_missing_sku(sku:str) -> recovery plan for a SKU using CSVs\n"
    " - impacted_by_plant(plant_id:str) -> orders at a plant using CSVs\n"
    " - order_details(order_id:str) -> order info from CSVs\n"
    " - plan_all_delayed_orders() -> plan for all delayed orders\n"
    "Use IDs like product_####, order_####, plant_### when applicable."
)

@tool
def delayed_orders() -> str:
    """Return delayed orders from CSV."""
    return delayed_orders_md()

@tool
def plan_missing_sku(sku: str) -> str:
    """Plan recovery for a missing SKU. Example sku: product_556490."""
    return plan_missing_sku_md(sku)

@tool
def impacted_by_plant(plant_id: str) -> str:
    """Orders impacted at a plant. Example: plant_253 or numeric id."""
    return impacted_by_plant_md(plant_id)

@tool
def order_details(order_id: str) -> str:
    """Details for a specific order. Accepts order_311579 or numeric id."""
    return order_details_md(order_id)

@tool
def plan_all_delayed_orders() -> str:
    """Plan recovery across all delayed orders using CSVs."""
    return plan_all_delayed_orders_md()

TOOLS = [delayed_orders, plan_missing_sku, impacted_by_plant, order_details, plan_all_delayed_orders]

class LlmCsvAgent:
    def __init__(self, model: Optional[str] = None, temperature: float = 0.1, max_steps: int = 6):
        self.llm = ChatOpenAI(
            model=model or os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            temperature=temperature,
        ).bind_tools(TOOLS)
        self.max_steps = max_steps

    def _guess(self, pattern: str, text: str) -> Optional[str]:
        m = re.search(pattern, text, re.I)
        if not m:
            return None
        g = m.group(0)
        # normalize order id like plain digits -> order_#####
        if "order" not in g.lower():
            if re.fullmatch(r"\d{5,}", g):
                return f"order_{g}"
        return g

    def _ctx_text(self, messages: List[Any]) -> str:
        return " ".join(getattr(m, "content", "") or "" for m in messages)

    def _loop(self, messages: List[Any]) -> str:
        for _ in range(self.max_steps):
            ai = self.llm.invoke(messages)
            messages.append(ai)

            if not ai.tool_calls:
                return ai.content  # natural reply

            ctx = self._ctx_text(messages)

            for call in ai.tool_calls:
                name = call["name"]
                args = call.get("args") or {}

                try:
                    if name == "delayed_orders":
                        result = delayed_orders()

                    elif name == "plan_missing_sku":
                        sku = args.get("sku") or self._guess(r"(?:product|sku)_[A-Za-z0-9\-]+", ctx)
                        result = plan_missing_sku(sku) if sku else "Please provide a SKU like product_556490."

                    elif name == "impacted_by_plant":
                        pid = args.get("plant_id") or self._guess(r"(?:plant|site|wh)_[A-Za-z0-9\-]+|\b\d{2,}\b", ctx)
                        result = impacted_by_plant(pid) if pid else "Please provide a plant id like plant_253."

                    elif name == "order_details":
                        oid = args.get("order_id") or self._guess(r"(?:order|so|id)[ _-]?(\d+)|\b\d{5,}\b", ctx)
                        if oid and not oid.lower().startswith("order_"):
                            m = re.search(r"\d+", oid)
                            oid = f"order_{m.group(0)}" if m else None
                        result = order_details(oid) if oid else "Please provide an order id like order_311579."

                    elif name == "plan_all_delayed_orders":
                        result = plan_all_delayed_orders()

                    else:
                        result = "Tool not available."
                except Exception as e:
                    result = f"Tool error: {type(e).__name__}: {e}"

                messages.append(ToolMessage(content=result, tool_call_id=call["id"]))

        return "I could not complete the request. Please rephrase."

    def invoke(self, payload: Any) -> Dict[str, Any]:
        user_text = payload if isinstance(payload, str) else str(payload)
        msgs: List[Any] = [SystemMessage(content=SYS), HumanMessage(content=user_text)]
        answer = self._loop(msgs)
        return {
            "text": answer,
            "suggestions": [
                "Which orders are delayed?",
                "Plan recovery for all delayed orders",
                "product_556490 is missing",
                "Show order 311579 details",
                "plant_253 is not working",
            ],
        }

def build_agent():
    agent = LlmCsvAgent()
    print(f"[Jarvis] LLM ready: {os.getenv('OPENAI_MODEL','gpt-4o-mini')}")
    return agent

