"""
Unit tests for Prefect pipeline connector.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
from metadata.generated.schema.entity.services.connections.pipeline.prefectConnection import (
    PrefectConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.source.pipeline.prefect.metadata import PrefectSource
from metadata.generated.schema.type.tagLabel import LabelType, State


# Mock Prefect API responses
MOCK_FLOWS = [
    {
        "id": "flow-1",
        "name": "test-flow",
        "tags": ["production", "source:db.schema.table1", "destination:db.schema.table2"],
    },
    {
        "id": "flow-2",
        "name": "etl-pipeline",
        "tags": ["etl"],
    },
]

MOCK_DEPLOYMENTS = [
    {
        "id": "dep-1",
        "flow_id": "flow-1",
        "name": "test-deployment",
        "tags": ["nightly"],
        "schedule": {"cron": "0 0 * * *"},
    }
]

MOCK_FLOW_RUNS = [
    {
        "id": "run-1",
        "name": "test-run",
        "state_type": "COMPLETED",
        "start_time": "2024-04-19T10:00:00Z",
        "end_time": "2024-04-19T10:05:00Z",
    },
    {
        "id": "run-2",
        "name": "test-run-2",
        "state_type": "FAILED",
        "start_time": "2024-04-19T11:00:00Z",
        "end_time": "2024-04-19T11:02:00Z",
    },
]


class TestPrefectSource(unittest.TestCase):
    """Test Prefect connector functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "source": {
                "type": "prefect",
                "serviceName": "test_prefect",
                "serviceConnection": {
                    "config": {
                        "type": "Prefect",
                        "apiKey": "test_key",
                        "accountId": "test_account",
                        "workspaceId": "test_workspace",
                        "numberOfStatus": 10,
                    }
                },
                "sourceConfig": {"config": {"type": "PipelineMetadata"}},
            }
        }
        
        self.mock_metadata = Mock()
        self.workflow_config = WorkflowSource.model_validate(self.config["source"])

    @patch("metadata.ingestion.source.pipeline.prefect.metadata.httpx.Client")
    def test_get_flows(self, mock_client):
        """Test fetching flows from Prefect API."""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = MOCK_FLOWS
        mock_client.return_value.post.return_value = mock_response
        
        source = PrefectSource(self.workflow_config, self.mock_metadata)
        flows = source.get_pipelines_list()
        
        self.assertEqual(len(flows), 2)
        self.assertEqual(flows[0]["name"], "test-flow")
        self.assertEqual(flows[1]["name"], "etl-pipeline")

    @patch("metadata.ingestion.source.pipeline.prefect.metadata.httpx.Client")
    def test_yield_pipeline(self, mock_client):
        """Test pipeline entity creation from Prefect flow."""
        # Mock deployments response
        mock_dep_response = Mock()
        mock_dep_response.status_code = 200
        mock_dep_response.json.return_value = MOCK_DEPLOYMENTS
        mock_client.return_value.post.return_value = mock_dep_response
        
        source = PrefectSource(self.workflow_config, self.mock_metadata)
        source.context.get = Mock(return_value=Mock(pipeline_service="test_prefect"))
        
        # Test with first flow
        results = list(source.yield_pipeline(MOCK_FLOWS[0]))
        
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].right is not None)
        
        pipeline_req = results[0].right
        self.assertEqual(pipeline_req.name.root, "test-flow")
        self.assertEqual(len(pipeline_req.tasks), 1)
        self.assertEqual(len(pipeline_req.tags), 4)  # 3 from flow + 1 from deployment
        
        # Verify tag structure
        tag = pipeline_req.tags[0]
        self.assertIsNotNone(tag.tagFQN)
        self.assertEqual(tag.labelType, LabelType.Automated)
        self.assertEqual(tag.state, State.Suggested)

    @patch("metadata.ingestion.source.pipeline.prefect.metadata.httpx.Client")
    def test_yield_pipeline_status(self, mock_client):
        """Test flow run status conversion."""
        # Mock flow runs response
        mock_runs_response = Mock()
        mock_runs_response.status_code = 200
        mock_runs_response.json.return_value = MOCK_FLOW_RUNS
        mock_client.return_value.post.return_value = mock_runs_response
        
        source = PrefectSource(self.workflow_config, self.mock_metadata)
        
        results = list(source.yield_pipeline_status(MOCK_FLOWS[0]))
        
        self.assertEqual(len(results), 2)
        
        # Check first status (COMPLETED)
        self.assertTrue(results[0].right is not None)
        status1 = results[0].right
        self.assertEqual(status1.executionStatus.value, "Successful")
        self.assertIsNotNone(status1.timestamp)
        self.assertEqual(len(status1.taskStatus), 1)
        
        # Check second status (FAILED)
        self.assertTrue(results[1].right is not None)
        status2 = results[1].right
        self.assertEqual(status2.executionStatus.value, "Failed")

    def test_parse_lineage_from_tags(self):
        """Test lineage detection from tags with both prefixed and legacy formats."""
        source = PrefectSource(self.workflow_config, self.mock_metadata)
        
        # Test with prefixed format (recommended)
        tags_prefixed = [
            "production",
            "om-source:warehouse.sales.orders",
            "om-source:warehouse.crm.customers",
            "om-destination:warehouse.analytics.summary",
            "etl",
        ]
        
        sources, destinations = source._parse_lineage_from_tags(tags_prefixed)
        
        self.assertEqual(len(sources), 2)
        self.assertIn("warehouse.sales.orders", sources)
        self.assertIn("warehouse.crm.customers", sources)
        
        self.assertEqual(len(destinations), 1)
        self.assertIn("warehouse.analytics.summary", destinations)
        
        # Test with legacy format (backward compatibility)
        tags_legacy = [
            "production",
            "source:warehouse.sales.orders",
            "destination:warehouse.analytics.summary",
        ]
        
        sources, destinations = source._parse_lineage_from_tags(tags_legacy)
        
        self.assertEqual(len(sources), 1)
        self.assertIn("warehouse.sales.orders", sources)
        
        self.assertEqual(len(destinations), 1)
        self.assertIn("warehouse.analytics.summary", destinations)
        
        # Test case insensitivity
        tags_mixed_case = [
            "OM-SOURCE:Warehouse.Sales.Orders",
            "om-destination:warehouse.analytics.SUMMARY",
        ]
        
        sources, destinations = source._parse_lineage_from_tags(tags_mixed_case)
        
        self.assertEqual(len(sources), 1)
        self.assertIn("warehouse.sales.orders", sources)
        
        self.assertEqual(len(destinations), 1)
        self.assertIn("warehouse.analytics.summary", destinations)
        
        # Test duplicate removal
        tags_duplicates = [
            "om-source:warehouse.sales.orders",
            "source:warehouse.sales.orders",  # Same table, different format
            "om-destination:warehouse.analytics.summary",
        ]
        
        sources, destinations = source._parse_lineage_from_tags(tags_duplicates)
        
        # Should only have one unique source
        self.assertEqual(len(sources), 1)

    def test_get_all_tags(self):
        """Test tag collection from flow and deployments."""
        source = PrefectSource(self.workflow_config, self.mock_metadata)
        
        flow = {"tags": ["flow-tag1", "flow-tag2"]}
        deployments = [
            {"tags": ["dep-tag1"]},
            {"tags": ["dep-tag2", "dep-tag3"]},
        ]
        
        all_tags = source._get_all_tags(flow, deployments)
        
        self.assertEqual(len(all_tags), 5)
        self.assertIn("flow-tag1", all_tags)
        self.assertIn("dep-tag1", all_tags)
        self.assertIn("dep-tag3", all_tags)

    def test_get_pipeline_name(self):
        """Test pipeline name extraction."""
        source = PrefectSource(self.workflow_config, self.mock_metadata)
        
        name = source.get_pipeline_name(MOCK_FLOWS[0])
        self.assertEqual(name, "test-flow")


if __name__ == "__main__":
    unittest.main()
