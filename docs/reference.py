"""
Handles automatic generation of contracts reference documentation from docstrings.
"""
import inspect
import types
from collections.abc import Collection, Iterable, Callable
from pathlib import Path
from typing import TypeVar, Any

import docstring_parser
from dbt_common.dataclass_schema import dbtClassMixin
from docstring_parser import DocstringParam

from dbt_contracts.contracts import Contract, ProcessorMethod, CONTRACTS, CompiledNodeT, NodeT, ParentContract
from dbt_contracts.contracts import PatchT, MetaT, TagT
from dbt_contracts.contracts.column import ColumnParentT
from dbt_contracts.types import T, ChildT, ParentT

HEADER_SECTION_CHARS = ["=", "-", "^", '"']

SECTIONS = {
    "Filters": lambda contract: contract.__filtermethods__,
    "Validations": lambda contract: contract.__validationmethods__,
}
SECTION_DESCRIPTIONS = {
    "Filters": [
        "Filters for reducing the scope of the contract.",
        "You may limit the number of {kind} processed by the rules of this contract "
        "by defining one or more of the following filters"
    ],
    "Validations": [
        "Validations to apply to the resources of this contract.",
        "These enforce certain standards that must be followed in order for the contract to be fulfilled."
    ]
}

IGNORE_ARG_TYPES = [T, ChildT, ParentT, NodeT, CompiledNodeT, PatchT, MetaT, TagT, ColumnParentT, dbtClassMixin]


class ReferencePageBuilder:

    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.lines: list[str] = []

    def add_lines(self, lines: str | Iterable[str]) -> None:
        self.lines.extend(lines) if not isinstance(lines, str) else self.lines.append(lines)

    def add_empty_lines(self, count: int = 1) -> None:
        self.add_lines([""] * count)

    @staticmethod
    def make_title(value: str) -> str:
        return value.replace("_", " ").replace("-", " ").capitalize()

    def add_header(self, value: str, section: int) -> None:
        if section >= len(HEADER_SECTION_CHARS):
            self.add_lines(f"**{value}**")
        else:
            header_char = HEADER_SECTION_CHARS[section]
            self.add_lines(value)
            self.add_lines(header_char * len(value))
        self.add_empty_lines()

    @staticmethod
    def _format_type_to_str(kind) -> str:
        if isinstance(kind, type):
            return f"``{kind.__name__}``"
        elif isinstance(kind, TypeVar):
            if kind.__bound__.__name__ is not None:
                return f"``{kind.__bound__.__name__}``"
            else:
                return " | ".join(map(lambda cls: f"``{cls.__name__}``", kind.__constraints__))
        elif isinstance(kind, types.UnionType):
            def _get_type_name(cls: Any) -> str:
                if isinstance(cls, types.GenericAlias):
                    params = ", ".join(map(_get_type_name, cls.__args__))
                    return f"``{cls.__origin__.__name__}``[{params}]"
                elif isinstance(cls, types.UnionType):
                    return " | ".join(map(_get_type_name, cls.__args__))
                return f"``{cls.__name__}``"

            return " | ".join(map(_get_type_name, kind.__args__))

        return f"``{kind}``"

    def _format_type_to_example(self, kind) -> str:
        if isinstance(kind, types.GenericAlias):
            types_list = list(map(self._format_type_to_example, kind.__args__))
            types_doc = " | ".join(types_list)
            return f"[{types_doc}, ...]"
        elif isinstance(kind, types.UnionType):
            return " | ".join(self._format_type_to_example(k) for k in kind.__args__)

        return f"<{self._format_type_to_str(kind)}>"

    def generate_ref_for_vararg(
            self, method_name: str, arg_name: str, kind: str | type | TypeVar = None
    ) -> None:
        indent = " " * 3
        doc = [
            f"You may define the {self.make_title(arg_name).lower().rstrip('s')}s as a list of values i.e.",
            "",
            ".. code-block:: yaml",
            f"{indent}{method_name}:"
        ]

        if kind is not None:
            kind_doc = self._format_type_to_example(kind)
            types_doc = [f"{indent}  - {kind_doc}" for _ in range(2)]
            types_doc.append(f"{indent}  - ...")
            doc.extend(types_doc)

        self.add_lines(doc)
        self.add_empty_lines()

    def generate_ref_for_varkwarg(
            self, method_name: str, arg_name: str, kind: str | type | TypeVar = None
    ) -> None:
        indent = " " * 3
        doc = [
            f"You may define the {self.make_title(arg_name).lower().rstrip('s')}s as a map of values i.e.",
            "",
            ".. code-block:: yaml",
            f"{indent}{method_name}: ",
        ]

        if kind is not None:
            kind_doc = self._format_type_to_example(kind)
            types_doc = [f"{indent}  - <key>: {kind_doc}" for _ in range(2)]
            types_doc.append(f"{indent}  - ...")
            doc.extend(types_doc)

        self.add_lines(doc)
        self.add_empty_lines()

    def generate_ref_for_kwargs(
            self, kwarg_names: Iterable[str], spec: inspect.FullArgSpec, params: Iterable[DocstringParam]
    ):
        kwarg_info = "You may define the following keyword arguments: "
        self.add_lines(kwarg_info)

        for kwarg_name in kwarg_names:
            kind = spec.annotations.get(kwarg_name)
            default = spec.kwonlydefaults.get(kwarg_name) if spec.kwonlydefaults else None
            description = next((param for param in params if param.arg_name == kwarg_name), None)

            self.generate_ref_for_kwarg(kwarg_name, kind=kind, default=default, description=description.description)

        self.add_empty_lines()

    def generate_ref_for_kwarg(
            self, name: str, kind: str | type | TypeVar = None, description: str = None, default: str = None
    ) -> None:
        line = f"`{name}`"

        if kind is not None:
            kind = self._format_type_to_str(kind)

        if kind and default:
            line += f" ({kind} = ``{default}``)"
        elif kind:
            line += f" ({kind})"

        if description:
            line += f" - {description.strip().rstrip(".")}"

        line = f"  - {line}"
        self.add_lines(line)

    def generate_ref_for_args(self, method: Callable, params: Iterable[DocstringParam]):
        arg_spec = inspect.getfullargspec(method)
        kwarg_names = [
            arg_name for arg_name in arg_spec.args
            if (kind := arg_spec.annotations.get(arg_name)) not in IGNORE_ARG_TYPES
            and arg_name != "self"
            and (
                       type(kind) is not type
                       or not any(issubclass(kind, cls) for cls in IGNORE_ARG_TYPES if type(cls) is type)
               )
        ]

        if not any((kwarg_names, arg_spec.varargs, arg_spec.varkw)):
            no_args_doc = (
                "This method does not need further configuration. "
                "Simply define the method name in your configuration."
            )
            self.add_lines(no_args_doc)
            return

        self.add_header("Arguments", section=4)

        if arg_spec.varargs:
            name = arg_spec.varargs
            kind = arg_spec.annotations.get(name)
            self.generate_ref_for_vararg(method_name=method.__name__, arg_name=name, kind=kind)

        if arg_spec.varkw:
            name = arg_spec.varkw
            kind = arg_spec.annotations.get(name)
            self.generate_ref_for_varkwarg(method_name=method.__name__, arg_name=name, kind=kind)

        if kwarg_names:
            self.generate_ref_for_kwargs(kwarg_names, arg_spec, params)

    def generate_ref_for_method(self, contract: type[Contract], method_name: str) -> None:
        method: ProcessorMethod = getattr(contract, method_name)
        self.add_header(f"``{method_name}``", section=3)

        description_split_on = ".. code-block::"

        doc = docstring_parser.parse(method.func.__doc__)
        description_split = iter(doc.description.split(description_split_on, 1))

        self.add_lines(next(description_split).strip())
        self.add_empty_lines()

        self.generate_ref_for_args(method.func, doc.params)

        example = next(description_split, None)
        if example:
            self.add_header("Example", section=4)
            self.add_lines((description_split_on + example).strip())

        self.add_empty_lines()

    def generate_ref_for_methods(
            self,
            contract: type[Contract],
            kind: str,
            method_names: Collection[str],
            description: str | Iterable[str] = None
    ) -> None:
        self.add_header(self.make_title(kind), section=2)
        if description:
            self.add_lines(description)
            self.add_empty_lines()

        for method_name in method_names:
            self.generate_ref_for_method(contract, method_name)

    def generate_ref_for_contract_body(self, contract: type[Contract]) -> None:
        kind = self.make_title(str(contract.config_key))

        for key, method_getter in SECTIONS.items():
            method_names = method_getter(contract)
            description = map(lambda line: line.format(kind=kind.lower()), SECTION_DESCRIPTIONS[key])
            self.generate_ref_for_methods(contract, key, method_names=method_names, description=description)

    def build(self, contract: type[Contract], description: str | Iterable[str] = None) -> None:
        self.lines.clear()

        contract.__new__(contract)
        kind = self.make_title(str(contract.config_key))
        self.add_header(kind, section=0)

        if description:
            self.add_lines(description)
            self.add_empty_lines()

        self.add_header("Configuration", section=1)

        self.generate_ref_for_contract_body(contract=contract)

        if issubclass(contract, ParentContract):
            # noinspection PyTypeChecker
            child_contract: type[Contract] = contract.child_type
            child_kind = self.make_title(str(child_contract.config_key))

            child_contract.__new__(child_contract)
            self.add_header(f"{child_kind} configuration", section=1)
            self.generate_ref_for_contract_body(contract=child_contract)

        filename = str(contract.config_key)
        output_path = self.output_dir.joinpath(filename).with_suffix(".rst")

        with output_path.open("w") as file:
            file.write("\n".join(self.lines))


if __name__ == "__main__":
    reference_pages_dir = Path(__file__).parent.joinpath("reference")
    builder = ReferencePageBuilder(reference_pages_dir)
    for c in CONTRACTS:
        builder.build(c)
