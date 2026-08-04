"""Canned tools for the Agent loop (loop 2).

These simulate the kind of side-effect-free lookups a support agent might call.
They return deterministic fake data so the demo is stable offline.
"""

# --- Fake backing data ---------------------------------------------------------

_ORDERS = {
    "A1001": {"status": "shipped", "carrier": "DHL", "eta": "2 days", "total": "$79.00"},
    "A1002": {"status": "processing", "carrier": None, "eta": "not yet shipped", "total": "$21.50"},
    "A1003": {"status": "delivered", "carrier": "UPS", "eta": "delivered", "total": "$140.00"},
}

_KB = {
    "refund": "Refunds are available within 30 days of delivery for unused items. "
    "Refunds are processed to the original payment method within 5-7 business days.",
    "shipping": "Standard shipping takes 3-5 business days. Express shipping (1-2 days) "
    "is available at checkout for an extra fee.",
    "returns": "To return an item, start a return from your order page to get a prepaid label. "
    "Items must be returned within 30 days.",
    "warranty": "All products carry a 1-year limited warranty against manufacturing defects.",
}

_REFUND_POLICY = (
    "Refund policy: eligible within 30 days of delivery, item must be unused and in "
    "original packaging. Digital goods are non-refundable once downloaded."
)


# --- Tool implementations ------------------------------------------------------

def get_order_status(order_id: str) -> dict:
    order = _ORDERS.get(order_id.strip().upper())
    if not order:
        return {"error": f"No order found with id {order_id!r}.", "known_ids": list(_ORDERS)}
    return {"order_id": order_id.strip().upper(), **order}


def search_knowledge_base(query: str) -> dict:
    q = query.lower()
    hits = [{"topic": k, "content": v} for k, v in _KB.items() if k in q or any(w in v.lower() for w in q.split())]
    if not hits:
        # fall back to the closest single topic so the agent always gets something
        hits = [{"topic": "shipping", "content": _KB["shipping"]}]
    return {"query": query, "results": hits[:2]}


def get_refund_policy() -> dict:
    return {"policy": _REFUND_POLICY}


# --- OpenAI tool schemas -------------------------------------------------------

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "get_order_status",
            "description": "Look up the status, carrier and ETA of a customer order by its id (e.g. A1001).",
            "strict": True,
            "parameters": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "string", "description": "The order id, e.g. A1001"},
                },
                "required": ["order_id"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_knowledge_base",
            "description": "Search the help-center knowledge base for policies and how-tos.",
            "strict": True,
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "What to search for"},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_refund_policy",
            "description": "Return the full refund policy text.",
            "strict": True,
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
]

TOOL_IMPL = {
    "get_order_status": get_order_status,
    "search_knowledge_base": search_knowledge_base,
    "get_refund_policy": get_refund_policy,
}
