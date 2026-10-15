"""Compose entry point; service construction performs no database queries."""
from datetime import UTC,datetime
from .api import create_app
from .observability import configure
from .agents import AgentRuntime,OpenAIProvider
from .analytics import AnalyticsClient
from .clock import Clock
from .features import FeatureService
from .ledger import Ledger
from .model import DemandModel
from .publisher import MockPublisher
from .outbox import OutboxWorker
from .scenarios import ScenarioService
from .settings import Settings,LAB_ROOT
from .source import SourceStore


def build_services(settings):
    ledger=Ledger(settings.postgres_dsn.get_secret_value())
    source=SourceStore(settings.postgres_dsn.get_secret_value())
    analytics=AnalyticsClient(settings,request_timeout_seconds=60)
    clock=Clock(datetime(2026,1,30,12,tzinfo=UTC))
    features=FeatureService(analytics,ledger,clock)
    model=DemandModel.from_artifact(settings.model_path)
    publisher=MockPublisher(ledger,clock,[model.version])
    worker=OutboxWorker(ledger,analytics,clock)
    telemetry=configure('pricing-backend',settings.otel_endpoint)
    provider=OpenAIProvider(settings.agent_api_key.get_secret_value(),settings.agent_model) if settings.agent_provider=='openai' and settings.agent_model and settings.agent_api_key.get_secret_value() else None
    agent=AgentRuntime(ledger,features,model,clock,owner_id=settings.owner_id,scenario_id=settings.scenario_id,provider=provider)
    def shutdown(): analytics.close();worker.close();telemetry.shutdown()
    return {'agent':agent,'outbox':worker,'shutdown':shutdown,'ledger':ledger,'source':source,'features':features,'model':model,'publisher':publisher,
            'scenarios':ScenarioService(source,ledger,features,clock,model),'mode':'live'}


settings=Settings.from_env(include_inventory=False)
app=create_app(settings,build_services(settings))
