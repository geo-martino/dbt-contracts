from dbt.contracts.graph.nodes import SourceDefinition
from faker import Faker

from dbt_contracts.contracts.conditions.source import IsEnabledCondition


def test_is_enabled_validation(source: SourceDefinition, faker: Faker):
    source.config.enabled = True
    assert IsEnabledCondition().run(source)

    source.config.enabled = False
    assert not IsEnabledCondition().run(source)
