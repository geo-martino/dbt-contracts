from abc import ABCMeta

from dbt_contracts.contracts import ContractTerm
from dbt_contracts.types import NodeT


class NodeContractTerm[I: NodeT](ContractTerm[I, None], metaclass=ABCMeta):
    pass
