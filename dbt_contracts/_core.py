import os
from pathlib import Path

from dbt.flags import get_flags
from pydantic import BaseModel, ConfigDict


class BaseModelConfig(BaseModel):
    """
    Base class for all models in the dbt_contracts package.

    Sets common config and functionality as required.
    """
    model_config = ConfigDict(validate_assignment=True, validate_default=True)


def get_absolute_project_path(path: str | Path) -> Path | None:
    """
    Get the absolute path of the given relative ``path`` in the project directory.
    Only returns the path if it exists.

    :param path: The relative path.
    :return: The absolute project path.
    """
    flags = get_flags()
    project_dir = getattr(flags, "PROJECT_DIR", None) or ""

    if project_dir and (path_in_project := Path(project_dir, path)).exists():
        return path_in_project.resolve()
    elif (path_in_cwd := Path(os.getcwd(), path)).exists():
        return path_in_cwd.resolve()

    raise FileNotFoundError(f"Could not find absolute path for: {path}")
