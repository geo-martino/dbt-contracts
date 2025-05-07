from pydantic import BaseModel, ConfigDict


class BaseModelConfig(BaseModel):
    """
    Base class for all models in the dbt_contracts package.

    Sets common config and functionality as required.
    """
    model_config = ConfigDict(validate_assignment=True, validate_default=True)
