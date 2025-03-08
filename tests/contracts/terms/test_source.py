from random import sample
from unittest import mock

import pytest
from dbt.artifacts.resources.types import TimePeriod
from dbt.artifacts.resources.v1.components import FreshnessThreshold, Time
from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.terms.source import HasLoader, HasFreshness, HasDownstreamDependencies


def test_has_loader(source: SourceDefinition, context: ContractContext):
    source.loader = "i am a loader"
    assert HasLoader().run(source, context=context)

    source.loader = ""
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasLoader().run(source, context=context)
        mock_add_result.assert_called_once()


@pytest.fixture
def fresh_source(source: SourceDefinition) -> SourceDefinition:
    """Fixture for a source with freshness"""
    source.loaded_at_field = "i am a loaded at field"
    source.freshness = FreshnessThreshold(
        warn_after=Time(count=2, period=TimePeriod.hour),
        error_after=Time(count=5, period=TimePeriod.hour),
    )
    return source


def test_has_freshness(fresh_source: SourceDefinition, context: ContractContext):
    assert HasFreshness().run(fresh_source, context=context)


def test_has_no_freshness_invalid_loaded_at_field(fresh_source: SourceDefinition, context: ContractContext):
    fresh_source.loaded_at_field = None

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasFreshness().run(fresh_source, context=context)
        mock_add_result.assert_called_once()


def test_has_no_freshness_invalid_freshness(fresh_source: SourceDefinition, context: ContractContext):
    fresh_source.freshness.warn_after.count = None
    fresh_source.freshness.error_after.period = None

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasFreshness().run(fresh_source, context=context)
        mock_add_result.assert_called_once()


def test_has_downstream_dependencies(source: SourceDefinition, context: ContractContext):
    downstream_deps = sample(
        [node for node in context.manifest.nodes.values() if node.unique_id.startswith("model")], k=3
    )
    assert downstream_deps
    for dep in downstream_deps:
        dep.depends_on_nodes.append(source.unique_id)

    assert HasDownstreamDependencies().run(source, context=context)

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasDownstreamDependencies(min_count=5).run(source, context=context)
        mock_add_result.assert_called_once()
