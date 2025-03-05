"""
Handles automatic generation of contracts reference documentation from docstrings.
"""
import shutil
from collections.abc import Collection, Iterable, Callable, Mapping
from pathlib import Path
from types import GenericAlias, UnionType
from typing import Any

import docstring_parser
import yaml
from pydantic import BaseModel
# noinspection PyProtectedMember
from pydantic.fields import FieldInfo

from dbt_contracts.contracts import Contract, ParentContract, ChildContract, CONTRACT_CLASSES
from dbt_contracts.contracts.conditions import ContractCondition
from dbt_contracts.contracts.terms import ContractTerm

HEADER_SECTION_CHARS = ("=", "-", "^", '"')

SECTIONS: dict[str, Callable[[type[Contract]], Collection[type[ContractTerm | ContractCondition]]]] = {
    "Filters": lambda contract: contract.__supported_conditions__,
    "Terms": lambda contract: contract.__supported_terms__,
}
SECTION_DESCRIPTIONS = {
    "Filters": [
        "Filters for reducing the scope of the contract.",
        "You may limit the number of {kind} processed by the rules of this contract "
        "by defining one or more of the following filters."
    ],
    "Terms": [
        "Terms to apply to the resources of this contract.",
        "These enforce certain standards that must be followed in order for the contract to be fulfilled."
    ]
}

URL_PATH = ("reference",)


class ReferencePageBuilder:

    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.lines: list[str] = []
        self.indent = " " * 3

    def add_lines(self, lines: str | Iterable[str], indent: int = 0) -> None:
        if isinstance(lines, str):
            lines = [lines]

        if indent:
            indent_str = self.indent * indent
            lines = (indent_str + line for line in lines)

        self.lines.extend(lines)

    def add_empty_lines(self, count: int = 1) -> None:
        self.add_lines([""] * count)

    def add_code_block_lines(self, lines: str | Iterable[str], indent: int = 0) -> None:
        if isinstance(lines, str):
            lines = [lines]

        indent_str = self.indent * indent
        lines = (self.indent + indent_str + line if i != 0 else indent_str + line for i, line in enumerate(lines))

        self.lines.extend(lines)

    @staticmethod
    def make_title(value: str) -> str:
        return value.replace("_", " ").replace("-", " ").capitalize()

    def add_header(self, value: str, section: int | None = None) -> None:
        if section is None:
            header_char = HEADER_SECTION_CHARS[0]
            self.add_lines(header_char * len(value))
            self.add_lines(value)
            self.add_lines(header_char * len(value))
        elif section >= len(HEADER_SECTION_CHARS):
            self.add_lines(f"**{value}**")
        else:
            header_char = HEADER_SECTION_CHARS[section]
            self.add_lines(value)
            self.add_lines(header_char * len(value))
        self.add_empty_lines()

    @staticmethod
    def _get_description(key: str, format_map: Mapping[str, Any]) -> Iterable[str]:
        return (line.format(**format_map) for line in SECTION_DESCRIPTIONS[key])

    @staticmethod
    def _get_dropdown_block(title: str, colour: str = "primary", icon: str = None) -> list[str]:
        block = [
            f".. dropdown:: {title}",
            ":animate: fade-in",
            f":color: {colour}",
        ]
        if icon:
            block.append(f":icon: {icon}")

        block.append("")
        return block

    def generate_args(self, model: type[BaseModel], name: str):
        if not model.model_fields:
            no_args_doc = [
                ".. note::",
                "This method does not need further configuration. "
                "Simply define the method name in your configuration."
            ]
            self.add_code_block_lines(no_args_doc)
            self.add_empty_lines()
            return

        self.generate_schema_ref(model, name=name)
        self.generate_example_ref(model, name=name)

    def generate_schema_ref(self, model: type[BaseModel], name: str) -> None:
        schema = self.generate_schema_dict(model)
        if not schema:
            return

        self.add_code_block_lines(self._get_dropdown_block("Schema", icon="gear"))

        schema_block = [".. code-block:: yaml", "", *yaml.dump({name: schema}).splitlines()]
        self.add_code_block_lines(schema_block, indent=1)
        self.add_empty_lines()

    def generate_schema_dict(self, model: type[BaseModel]) -> dict[str, Any]:
        schema = model.model_json_schema()["properties"]
        self._trim_schema(schema)
        return schema

    @staticmethod
    def _trim_schema(schema: dict[str, Any]) -> None:
        for value in schema.values():
            value.pop("examples", "")
            value.pop("title", "")

    def generate_example_ref(self, model: type[BaseModel], name: str) -> None:
        example = self.generate_example_dict(model)
        if not example:
            return

        self.add_code_block_lines(self._get_dropdown_block("Example", colour="info", icon="code"))

        example_block = [".. code-block:: yaml", "", *yaml.dump({name: example}).splitlines()]
        self.add_code_block_lines(example_block, indent=1)
        self.add_empty_lines()

        # noinspection PyTypeChecker
        first_field = next(iter(model.model_fields))
        if first_field not in example:
            return

        first_field_example_desc = (
            f"You may also define the parameters for ``{first_field}`` directly on the term definition like below."
        )
        self.add_code_block_lines(first_field_example_desc, indent=1)
        self.add_empty_lines()

        example_block = [".. code-block:: yaml", "", *yaml.dump({name: example[first_field]}).splitlines()]
        self.add_code_block_lines(example_block, indent=1)
        self.add_empty_lines()

    def generate_example_dict(self, model: type[BaseModel]) -> dict[str, Any]:
        examples = {}
        # noinspection PyUnresolvedReferences
        for name, field in model.model_fields.items():
            field: FieldInfo
            if field.examples:
                examples[name] = field.examples[0]
            elif isinstance(field.annotation, (GenericAlias, UnionType)):
                continue
            elif issubclass(field.annotation, BaseModel):
                examples[name] = self.generate_example_dict(field.annotation)

        return examples

    def generate_contract_parts(
            self,
            kind: str,
            parts: Collection[type[ContractTerm | ContractCondition]],
            description: str | Iterable[str] = None
    ) -> None:
        self.add_header(self.make_title(kind), section=0)
        if description:
            self.add_lines(description)
            self.add_empty_lines()

        list(map(self.generate_contract_part, parts))

    def generate_contract_part(self, part: type[ContractTerm | ContractCondition]) -> None:
        # noinspection PyProtectedMember
        name = part._name()
        self.add_header(name, section=1)

        doc = docstring_parser.parse(part.__doc__)
        if doc.description:
            self.add_lines(doc.description.strip())
            self.add_empty_lines()

        self.generate_args(part, name=name)

    def generate_contract_body(self, contract: type[Contract]) -> None:
        title = self.make_title(contract.__config_key__)

        for key, getter in SECTIONS.items():
            description = self._get_description(key, format_map={"kind": title.lower()})
            self.generate_contract_parts(key, parts=getter(contract), description=description)

    def generate_ref_to_child_page(self, contract: type[ChildContract], parent_title: str) -> None:
        key = contract.__config_key__
        title = self.make_title(key)
        self.add_header(title, section=0)

        link_ref = f":ref:`{title.lower()} <{key}>`"
        description = (
            f"You may also define {title.lower().rstrip('s')}s contracts as a child set of contracts "
            f"on {parent_title.lower().rstrip('s')}s. ",
            f"Refer to the {link_ref} reference for more info."
        )

        self.add_lines(description)
        self.add_empty_lines()

    def build(self, contract: type[Contract], description: str | Iterable[str] = None) -> None:
        self.lines.clear()

        key = contract.__config_key__
        title = self.make_title(key)
        self.add_lines(f".. _{key}:")
        self.add_header(title)

        if description:
            self.add_lines(description)
            self.add_empty_lines()

        self.generate_contract_body(contract=contract)

        if issubclass(contract, ParentContract):
            self.generate_ref_to_child_page(contract.__child_contract__, parent_title=title)

        self._save(contract.__config_key__)

    def _save(self, filename: str) -> None:
        output_path = self.output_dir.joinpath(filename).with_suffix(".rst")

        self.output_dir.mkdir(parents=True, exist_ok=True)
        with output_path.open("w") as file:
            file.write("\n".join(self.lines))


if __name__ == "__main__":
    reference_pages_dir = Path(__file__).parent.joinpath(*URL_PATH)
    if reference_pages_dir.is_dir():
        shutil.rmtree(reference_pages_dir)

    builder = ReferencePageBuilder(reference_pages_dir)
    for contract_cls in CONTRACT_CLASSES:
        builder.build(contract_cls)
        if issubclass(contract_cls, ParentContract):
            builder.build(contract_cls.__child_contract__)
