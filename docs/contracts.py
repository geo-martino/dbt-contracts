"""
Handles automatic generation of contracts reference documentation from docstrings.
"""
import shutil
from collections.abc import Collection, Iterable, Callable, Mapping
from pathlib import Path
from random import choice, sample, randrange
from types import GenericAlias, UnionType
from typing import Any

import docstring_parser
import yaml
from pydantic import BaseModel
# noinspection PyProtectedMember
from pydantic.fields import FieldInfo

from dbt_contracts.contracts import Contract, ParentContract, ChildContract, ContractPart, CONTRACT_CLASSES
from dbt_contracts.contracts.generators import PropertiesGenerator

HEADER_SECTION_CHARS = ("=", "-", "^", '"')

SECTIONS: dict[str, Callable[[type[Contract]], Collection[type[ContractPart]]]] = {
    "Filters": lambda contract: contract.__supported_conditions__,
    "Validations": lambda contract: contract.__supported_terms__,
    "Generator": lambda contract: contract.__supported_generator__ if contract.__supported_generator__ else (),
}
SECTION_DESCRIPTIONS = {
    "Filters": [
        "Filters (or Conditions) for reducing the scope of the contract.",
        "You may limit the number of {kind} processed by the rules of this contract "
        "by defining one or more of the following filters."
    ],
    "Validations": [
        "Validations (or Terms) to apply to the resources of this contract.",
        "These enforce certain standards that must be followed in order for the contract to be fulfilled."
    ],
    "Generator": [
        "You may also configure a Generator for your contract.",
        "A Generator creates/updates properties files for {kind} from the attributes found on the database resource.",
    ]
}

URL_PATH = ("reference", "contracts")


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
            cls = next(cls for cls in model.mro() if cls.__name__.startswith("Contract"))
            kind = cls.__name__.replace("Contract", "").split("[")[0].lower()
            no_args_doc = [
                ".. note::",
                f"This {kind} does not need further configuration. "
                f"Simply define the {kind}'s name as an item in your configuration."
            ]
            self.add_code_block_lines(no_args_doc)
            self.add_empty_lines()
            return

        self.generate_schema_ref(model, name=name)
        self.generate_example_ref(model, name=name)

    def generate_schema_ref(self, model: type[BaseModel], name: str) -> None:
        schema = self.generate_schema_dict(model)
        if not (properties := schema.get("properties")):
            return

        self.add_code_block_lines(self._get_dropdown_block("Schema", icon="gear"))

        properties_block = [".. code-block:: yaml", "", *yaml.dump({name: properties}, sort_keys=False).splitlines()]
        self.add_code_block_lines(properties_block, indent=1)
        self.add_empty_lines()

        if not (defs := self.generate_schema_dict_for_defs(schema.get("$defs"))):
            return

        self.add_code_block_lines(["**defs**", ""], indent=1)
        def_block = [".. code-block:: yaml", "", *yaml.dump(defs, sort_keys=False).splitlines()]
        self.add_code_block_lines(def_block, indent=1)
        self.add_empty_lines()

    def generate_schema_dict(self, model: type[BaseModel]) -> dict[str, Any]:
        schema = model.model_json_schema()
        self._trim_schema(schema.get("properties"))
        return schema

    def generate_schema_dict_for_defs(self, defs: dict[str, Any]) -> dict[str, Any] | None:
        if not defs:
            return

        result = {}
        for name, schema_def in defs.items():
            if not (properties := schema_def.get("properties")):
                continue
            self._trim_schema(properties)
            result[name] = properties

        return result

    @staticmethod
    def _trim_schema(schema: dict[str, Any]) -> None:
        if not schema:
            return

        for value in schema.values():
            value.pop("examples", "")
            value.pop("title", "")

    def generate_example_ref_for_contract(self, contract: type[Contract]) -> None:
        example = self.generate_example_dict_for_contract(contract)

        if issubclass(contract, ParentContract):
            example = {"contracts": {contract.__config_key__: [example]}}
        else:
            parent_contract = next(cls for cls in CONTRACT_CLASSES if cls.__child_contract__ is contract)
            example = {"contracts": {parent_contract.__config_key__: [{contract.__config_key__: [example]}]}}

        self._generate_example_dropdown(example, key="Full Example")

    @classmethod
    def generate_example_dict_for_contract(cls, contract: type[Contract]) -> dict[str, Any]:
        conditions_count = randrange(
            min(len(contract.__supported_conditions__), 3) - 1, len(contract.__supported_conditions__)
        )
        conditions = sample(contract.__supported_conditions__, k=max(1, conditions_count))

        terms_count = randrange(min(len(contract.__supported_terms__), 3) - 1, len(contract.__supported_terms__))
        terms = sample(contract.__supported_terms__, k=max(1, terms_count))

        # noinspection PyProtectedMember
        example = {
            "filter": [
                {cond._name(): cls.generate_example_dict(cond)} if cls.generate_example_dict(cond) else cond._name()
                for cond in conditions
            ],
            "validations": [
                {term._name(): cls.generate_example_dict(term)} if cls.generate_example_dict(term) else term._name()
                for term in terms
            ]
        }
        if contract.__supported_generator__ is not None:
            example["generator"] = cls.generate_example_dict(contract.__supported_generator__)

        return example

    def generate_example_ref(self, model: type[BaseModel], name: str) -> None:
        example = self.generate_example_dict(model)
        if not example:
            return

        self._generate_example_dropdown({name: example}, key="Example")

        doc = docstring_parser.parse(model.__doc__)
        if doc.description and "__EXAMPLE__" in doc.description:
            self.add_code_block_lines(doc.description.strip().split("__EXAMPLE__")[1].splitlines())
            self.add_empty_lines()

        # noinspection PyUnresolvedReferences
        field_1_name, field_1 = next(iter(model.model_fields.items()))
        if field_1_name not in example:
            return

        example = self.generate_example_dict(model)[field_1_name]
        if isinstance(example, Mapping):
            return

        first_field_example_desc = (
            f"You may also define the parameters for ``{field_1_name}`` directly on the definition like below."
        )
        self.add_code_block_lines(first_field_example_desc, indent=1)
        self.add_empty_lines()

        example_block = [".. code-block:: yaml", "", *yaml.dump({name: example}, sort_keys=False).splitlines()]
        self.add_code_block_lines(example_block, indent=1)
        self.add_empty_lines()

    def _generate_example_dropdown(self, example: dict[str, Any], key: str) -> None:
        self.add_code_block_lines(self._get_dropdown_block(key, colour="info", icon="code"))

        example_block = [".. code-block:: yaml", "", *yaml.dump(example, sort_keys=False).splitlines()]
        self.add_code_block_lines(example_block, indent=1)
        self.add_empty_lines(2)

    @classmethod
    def generate_example_dict(cls, model: type[BaseModel]) -> dict[str, Any]:
        # noinspection PyUnresolvedReferences
        examples = {name: cls._generate_example_for_field(field) for name, field in model.model_fields.items()}
        return {key: val for key, val in examples.items() if val is not None}

    @classmethod
    def _generate_example_for_field(cls, field: FieldInfo) -> Any:
        if field.examples:
            return choice(field.examples)
        elif isinstance(field.annotation, (GenericAlias, UnionType)):
            field = choice([arg for arg in field.annotation.__args__ if arg is not type(None)])
            if issubclass(field, BaseModel):
                return cls.generate_example_dict(field)
        elif issubclass(field.annotation, BaseModel):
            return cls.generate_example_dict(field.annotation)

    def generate_contract_parts(
            self, parts: type[ContractPart] | Collection[type[ContractPart]], kind: str, title: str
    ) -> None:
        description = self._get_description(kind, format_map={"kind": title.lower()})

        kind = self.make_title(kind)
        self.add_header(kind, section=0)
        if description:
            self.add_lines(description)
            self.add_empty_lines()

        if not isinstance(parts, Collection):
            title = None
            parts = [parts]

        for part in parts:
            self.generate_contract_part(part, title=title)

    def generate_contract_part(self, part: type[ContractPart], title: str | None) -> None:
        # noinspection PyProtectedMember
        name = part._name() if not issubclass(part, PropertiesGenerator) else "generator"
        if title:
            self.add_header(name, section=1)

        doc = docstring_parser.parse(part.__doc__)
        if doc.description:
            self.add_lines(doc.description.strip().split("__EXAMPLE__")[0].format(kind=title.lower()))
            self.add_empty_lines()

        self.generate_args(part, name=name)

    def generate_contract_body(self, contract: type[Contract]) -> None:
        title = self.make_title(contract.__config_key__)

        for key, getter in SECTIONS.items():
            parts = getter(contract)
            if not parts:
                continue

            self.generate_contract_parts(parts=parts, kind=key, title=title)

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

        self.generate_example_ref_for_contract(contract)

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

    print("Generated references for all contracts")
