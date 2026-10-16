import pkgutil
import subprocess
import sys

import pytest

import dbt_contracts

MODULES = sorted(info.name for info in pkgutil.walk_packages(dbt_contracts.__path__, f"{dbt_contracts.__name__}."))


@pytest.mark.parametrize("module", MODULES)
def test_module_imports_on_its_own(module: str):
    # import in a fresh interpreter so circular imports aren't hidden by modules other tests already imported
    result = subprocess.run([sys.executable, "-c", f"import {module}"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr.strip().splitlines()[-1]
