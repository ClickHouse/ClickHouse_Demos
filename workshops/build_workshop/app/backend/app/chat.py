from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.chat_service import SqlGuardrailError, run_chat
from app.db import get_client
from app.schemas import ChatChartSpec, ChatRequest, ChatResponse
from app.settings import settings

router = APIRouter()


@router.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    if not settings.openai_api_key:
        raise HTTPException(
            status_code=503,
            detail=(
                "AI chat is not configured. Set OPENAI_API_KEY (and optionally LLM_MODEL / "
                "LLM_BASE_URL) on the backend to enable POST /api/chat."
            ),
        )

    message = (req.message or "").strip()
    if not message:
        raise HTTPException(status_code=422, detail="message must not be empty.")

    client = get_client()

    # Schema-grounded NL-to-SQL, guardrails, and execution run as one traced turn.
    try:
        result = run_chat(client, message, req.conversation_id)
    except SqlGuardrailError as e:
        raise HTTPException(status_code=400, detail=f"Query rejected by guardrails: {e}") from e

    chart = ChatChartSpec(**result.chart) if result.chart else None
    return ChatResponse(answer=result.answer, sql=result.sql, rows=result.rows, chart=chart)
