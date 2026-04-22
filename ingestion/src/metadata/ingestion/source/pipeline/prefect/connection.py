"""
Connection test for Prefect Cloud
"""
import httpx

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.pipeline.prefectConnection import (
    PrefectConnection,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata


def get_connection(connection: PrefectConnection) -> httpx.Client:
    """
    Create an HTTP client for Prefect Cloud or self-hosted Prefect Server.
    """
    # Handle both SecretStr and plain string for apiKey
    api_key = connection.apiKey
    if hasattr(api_key, "get_secret_value"):
        api_key_str = api_key.get_secret_value()
    else:
        api_key_str = str(api_key)

    # Use hostPort from config or default based on mode
    # Support both Prefect Cloud and self-hosted Prefect Server
    if connection.accountId and connection.workspaceId:
        # Prefect Cloud mode
        host = getattr(connection, "hostPort", None) or "https://api.prefect.cloud"
        base_url = (
            f"{host}/api/accounts"
            f"/{connection.accountId}"
            f"/workspaces/{connection.workspaceId}"
        )
    else:
        # Self-hosted Prefect Server mode
        host = getattr(connection, "hostPort", None) or "http://localhost:4200"
        base_url = f"{host}/api"

    headers = {
        "Authorization": f"Bearer {api_key_str}",
        "Content-Type": "application/json",
        "User-Agent": "OpenMetadata/Prefect-Connector",
    }
    return httpx.Client(base_url=base_url, headers=headers, timeout=30)


def test_connection(
    metadata: OpenMetadata,
    client: httpx.Client,
    service_connection: PrefectConnection,
    base_url: str = None,
    headers: dict = None,
    automation_workflow: AutomationWorkflow = None,
) -> None:
    """
    Test connection to Prefect Cloud by fetching flows.
    """

    def custom_test_connection(client: httpx.Client) -> None:
        # Test using POST /flows/filter as per Prefect 3.x API
        # Use provided base_url and headers if available (from metadata.py)
        # Otherwise construct them (when called from get_connection)
        if base_url and headers:
            url = f"{base_url}/flows/filter"
            response = client.post(url, headers=headers, json={"limit": 1, "offset": 0})
        else:
            # Fallback: client already has base_url and headers configured
            response = client.post("/flows/filter", json={"limit": 1, "offset": 0})
        response.raise_for_status()

    test_fn = {"GetFlows": custom_test_connection}
    test_connection_steps(
        metadata=metadata,
        service_type="Prefect",
        test_fn=test_fn,
        automation_workflow=automation_workflow,
    )
