"""Private installation settings. Readiness is checked per module, not at import."""
from typing import Literal
from pathlib import Path
import os
import ipaddress
from uuid import UUID
from urllib.parse import parse_qsl,urlsplit,urlunsplit,urlencode
from pydantic import AliasChoices, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LAB_ROOT = Path(os.environ.get("PRICING_LAB_ROOT", Path(__file__).resolve().parents[3])).resolve()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PRICING_", env_file=LAB_ROOT / "app/.env.workshop",
        extra="ignore", validate_default=True, populate_by_name=True,
    )
    cloud_org_id: UUID | None = None
    learner_cidr: str = ''
    region: str = 'ap-southeast-1'
    postgres_size: str = 'c6gd.large'
    resource_inventory_path: Path = LAB_ROOT / 'infra/resources.json'
    private_env_path: Path = LAB_ROOT / 'app/.env.workshop'
    pg_ca_path: Path = LAB_ROOT / '.run/managed-postgres-ca.pem'
    otel_endpoint: str = ''
    agent_provider: str = Field(default='',validation_alias=AliasChoices('AGENT_PROVIDER','PRICING_AGENT_PROVIDER'))
    agent_model: str = Field(default='',validation_alias=AliasChoices('AGENT_MODEL','PRICING_AGENT_MODEL'))
    agent_api_key: SecretStr = Field(default=SecretStr(''),validation_alias=AliasChoices('AGENT_API_KEY','PRICING_AGENT_API_KEY'))
    diagnostics_agent_mode: Literal['live', 'replay'] = 'replay'
    clickstack_database: str = Field(default='otel',pattern=r'^[a-z][a-z0-9_]{0,62}$')
    clickstack_trace_table: str = Field(default='otel_traces',pattern=r'^[a-z][a-z0-9_]{0,62}$')
    ingestion_mode: Literal["clickpipes", "local_replay", "recorded"] = "clickpipes"
    model_path: Path = LAB_ROOT / "pricing/artifacts/development/model.json"
    owner_id: str = Field(default="learner-1", pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    scenario_id: str = "grocery"
    currency: str = "SGD"
    postgres_dsn: SecretStr = SecretStr("")
    postgres_admin_dsn: SecretStr = SecretStr("")

    def administrator_dsn(self):
        value = self.postgres_admin_dsn.get_secret_value()
        if not value:
            from .contracts import WorkshopError
            raise WorkshopError("admin_credentials_missing", "Set PRICING_POSTGRES_ADMIN_DSN for initialization and source grants.")
        return value
    clickhouse_host: str = ""
    clickhouse_port: int = Field(default=8443, ge=1, le=65535)
    clickhouse_secure: bool = True
    clickhouse_database: str = Field(default="pricing", pattern=r"^[a-z][a-z0-9_]{0,62}$")
    clickhouse_username: str = "default"
    clickhouse_password: SecretStr = SecretStr("")
    session_secret: SecretStr = SecretStr("")
    allowed_origin: str = "http://localhost:8080"
    dataset_manifest: str = ""
    lease_minutes: int = Field(default=60, ge=1, le=60)
    convergence_timeout_seconds: float = Field(default=30, gt=0, le=120, allow_inf_nan=False)

    @field_validator('cloud_org_id',mode='before')
    @classmethod
    def optional_org(cls,value):
        return None if value=='' else value

    @field_validator('learner_cidr')
    @classmethod
    def learner_address(cls,value):
        if value:
            network=ipaddress.ip_network(value,strict=True)
            if network.version!=4 or network.prefixlen!=32: raise ValueError('Use the learner public IPv4 address with /32.')
        return value

    @field_validator('postgres_dsn','postgres_admin_dsn')
    @classmethod
    def container_ca(cls,value):
        if os.environ.get('PRICING_LAB_ROOT')=='/opt/pricing' and value.get_secret_value():
            parts=urlsplit(value.get_secret_value());query=dict(parse_qsl(parts.query))
            if 'sslrootcert' in query:
                query['sslrootcert']='/opt/pricing/.run/managed-postgres-ca.pem'
                return SecretStr(urlunsplit(parts._replace(query=urlencode(query))))
        return value

    @field_validator("clickhouse_host")
    @classmethod
    def bare_host(cls, value: str) -> str:
        if value and any(c in value for c in "/:@?# "):
            raise ValueError("Use a bare hostname. Configure the port and TLS separately.")
        return value

    @classmethod
    def from_env(cls) -> "Settings":
        from dotenv import dotenv_values
        path=Path(os.environ.get('PRICING_PRIVATE_ENV_PATH',LAB_ROOT/'app/.env.workshop'))
        values=dotenv_values(path,interpolate=False) if path.exists() else {}
        arguments={}
        for name in cls.model_fields:
            keys=['PRICING_'+name.upper()]
            if name.startswith('agent_'): keys.insert(0,name.upper())
            if not any(k in os.environ for k in keys):
                key=next((k for k in keys if k in values),None)
                if key and values[key] is not None: arguments[name]=values[key]
        settings=cls(_env_file=None,**arguments)
        if not settings.cloud_org_id and settings.resource_inventory_path.exists():
            import json
            inventory=json.loads(settings.resource_inventory_path.read_text())
            settings=cls(_env_file=None,**{**settings.model_dump(),'cloud_org_id':inventory['organization_id']})
        return settings
