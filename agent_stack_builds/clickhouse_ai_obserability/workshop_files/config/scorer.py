from litellm.integrations.custom_logger import CustomLogger
from langfuse import Langfuse
import os


class AutoScorer(CustomLogger):
    def __init__(self):
        self.lf = Langfuse(
            public_key=os.environ.get("LANGFUSE_PUBLIC_KEY"),
            secret_key=os.environ.get("LANGFUSE_SECRET_KEY"),
            host=os.environ.get("LANGFUSE_HOST", "https://us.cloud.langfuse.com"),
        )

    def log_success_event(self, kwargs, response_obj, start_time, end_time):
        try:
            slp = kwargs.get("standard_logging_object") or {}
            trace_id = slp.get("trace_id") or kwargs.get("litellm_call_id", "")
            if not trace_id or not response_obj:
                return

            # Extract user identity and session from the request
            # LibreChat sends `user` (MongoDB user ID) in the OpenAI API call
            # LiteLLM captures it in standard_logging_object
            user_id = (
                slp.get("user")
                or kwargs.get("user")
                or kwargs.get("litellm_params", {}).get("user")
                or "unknown"
            )
            # Use LiteLLM's call_id as a stable session handle per conversation turn
            session_id = slp.get("metadata", {}).get("session_id") or kwargs.get("litellm_call_id", "")

            # Update the trace with user_id and session so it's filterable in Langfuse
            self.lf.trace(
                id=trace_id,
                user_id=user_id,
                session_id=session_id,
                tags=["workshop"],
            )

            # Automatic quality score: depth of response (rough proxy for usefulness)
            choices = getattr(response_obj, "choices", [])
            output = choices[0].message.content or "" if choices else ""
            score = min(1.0, len(output) / 300) if len(output) > 20 else 0.2
            self.lf.score(trace_id=trace_id, name="quality", value=round(score, 2))
            self.lf.flush()
            print(f"[AutoScorer] user={user_id} trace={trace_id} quality={round(score, 2)}")
        except Exception as e:
            print(f"[AutoScorer] Error: {e}")

    async def async_log_success_event(self, kwargs, response_obj, start_time, end_time):
        self.log_success_event(kwargs, response_obj, start_time, end_time)


auto_scorer = AutoScorer()
