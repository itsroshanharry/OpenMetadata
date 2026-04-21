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
    Create an HTTP client for Prefect Cloud API.
    """
    # Handle both SecretStr and plain string for apiKey
    api_key = connection.apiKey
    if hasattr(api_key, 'get_secret_value'):
        api_key_str = api_key.get_secret_value()
    else:
        api_key_str = str(api_key)
    
    base_url = (
        f"https://api.prefect.cloud/api/accounts"
        f"/{connection.accountId}"
        f"/workspaces/{connection.workspaceId}"
    )
    headers = {
        "Authorization": f"Bearer {api_key_str}",
        "Content-Type": "application/json"
    }
    return httpx.Client(base_url=base_url, headers=headers, timeout=30)


def test_connection(
    metadata: OpenMetadata,
    client: httpx.Client,
    service_connection: PrefectConnection,
    automation_workflow: AutomationWorkflow = None,
) -> None:
    """
    Test connection to Prefect Cloud by fetching flows.
    """

    def custom_test_connection(client: httpx.Client) -> None:
        # Test using POST /flows/filter as per Prefect 3.x API
        base_url = (
            f"https://api.prefect.cloud/api/accounts"
            f"/{service_connection.accountId}"
            f"/workspaces/{service_connection.workspaceId}"
        )
        response = client.post(
            f"{base_url}/flows/filter",
            json={"limit": 1, "offset": 0}
        )
        response.raise_for_status()

    test_fn = {"GetFlows": custom_test_connection}
    test_connection_steps(
        metadata=metadata,
        service_type="Prefect",
        test_fn=test_fn,
        automation_workflow=automation_workflow,
    )
