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
            choices = getattr(response_obj, "choices", [])
            output = choices[0].message.content or "" if choices else ""
            score = min(1.0, len(output) / 300) if len(output) > 20 else 0.2
            self.lf.score(trace_id=trace_id, name="quality", value=round(score, 2))
            self.lf.flush()
            print(f"[AutoScorer] scored trace={trace_id} quality={round(score, 2)}")
        except Exception as e:
            print(f"[AutoScorer] Error: {e}")

    async def async_log_success_event(self, kwargs, response_obj, start_time, end_time):
        self.log_success_event(kwargs, response_obj, start_time, end_time)


auto_scorer = AutoScorer()
