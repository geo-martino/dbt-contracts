from hashlib import md5
from types import SimpleNamespace

from dbt.contracts.graph.manifest import Manifest

from dbt_contracts._compat import get_project_name


def test_get_project_name_from_metadata(manifest: Manifest, project_name: str):
    assert get_project_name(manifest) == project_name


def test_get_project_name_from_project_id():
    # dbt<1.7 only stores an md5 hash of the project name as the project_id
    project_name = "my_project"
    manifest = SimpleNamespace(
        metadata=SimpleNamespace(project_id=md5(project_name.encode()).hexdigest()),
        nodes={"model.dbt_utils.a": SimpleNamespace(package_name="dbt_utils")},
        sources={"source.my_project.b": SimpleNamespace(package_name=project_name)},
        macros={"macro.dbt.c": SimpleNamespace(package_name="dbt")},
    )
    assert get_project_name(manifest) == project_name


def test_get_project_name_from_unmatched_project_id():
    manifest = SimpleNamespace(
        metadata=SimpleNamespace(project_id=md5(b"unknown").hexdigest()),
        nodes={}, sources={}, macros={"macro.dbt.c": SimpleNamespace(package_name="dbt")},
    )
    assert get_project_name(manifest) is None
