"""
Fills in the variable fields of the README template and generates README.md file.
"""
import re

import docstring_parser

from dbt_contracts import PROGRAM_OWNER_USER, PROGRAM_NAME, DOCUMENTATION_URL
from dbt_contracts.contracts import CONTRACT_CLASSES, Contract, ParentContract
import docs.reference as docs

SRC_FILENAME = "README.template.md"
TRG_FILENAME = SRC_FILENAME.replace(".template", "")


def format_contract_title(contract: type[Contract], parent_key: str = "") -> str:
    """Format the title for a contract"""
    key = f"{parent_key.rstrip('s')}_{contract.__config_key__}"
    return key.replace("_", " ").title().strip()


def format_contract_reference(contract: type[Contract], parent_key: str = "") -> list[str]:
    """Format the readme template for the contracts reference"""
    lines = []

    key = contract.__config_key__
    title = format_contract_title(contract, parent_key)
    lines.extend((f"### {title}", ""))

    # noinspection PyTypeChecker,PyProtectedMember
    contract_parts_map = {
        "Filters": contract.__supported_conditions__,
        "Terms": contract.__supported_terms__,
    }

    for header, parts in contract_parts_map.items():
        lines.extend((f"#### {header}", ""))

        for part in parts:
            # noinspection PyProtectedMember
            name = part._name()
            url = f"{DOCUMENTATION_URL}/{'/'.join(docs.URL_PATH)}/{key}.html#{name.replace('_', '-')}"
            doc = docstring_parser.parse(part.__doc__).short_description.strip().format(kind=title.lower())
            doc = re.sub(r"\s*\n\s+", " ", doc)

            line = f"- [`{name}`]({url}): {doc}"
            lines.append(line)

        lines.append("")

    lines.append("")

    if issubclass(contract, ParentContract):
        lines.extend(format_contract_reference(contract.__child_contract__, key))

    return lines


def format_contracts_reference() -> str:
    """Format the readme template for the contracts reference"""
    lines = []
    for contract in CONTRACT_CLASSES:
        lines.extend(format_contract_reference(contract))

    return "\n".join(lines)


def format_contracts_reference_toc_entry(contract: type[Contract], parent_key: str = "") -> str:
    """Format the readme template for a contracts reference table of contents entry"""
    title = format_contract_title(contract, parent_key)
    return f"  * [{title}](#{title.replace(" ", "-").lower()})"


def format_contracts_reference_toc() -> str:
    """Format the readme template for the contracts reference table of contents"""
    lines = []

    for contract in CONTRACT_CLASSES:
        lines.append(format_contracts_reference_toc_entry(contract))
        if issubclass(contract, ParentContract):
            lines.append(format_contracts_reference_toc_entry(contract.__child_contract__, contract.__config_key__))

    return "\n".join(lines)


def format_readme():
    """Format the readme template and save the formatted readme"""
    format_map_standard = {
        "program_name": PROGRAM_NAME,
        "program_name_lower": PROGRAM_NAME.lower(),
        "program_owner_user": PROGRAM_OWNER_USER,
        "documentation_url": DOCUMENTATION_URL,
        "contracts_reference": format_contracts_reference().strip(),
        "contracts_reference_toc": format_contracts_reference_toc().rstrip(),
    }
    format_map_code = {
    }
    format_map_code = {k: "`" + "` `".join(v) + "`" for k, v in format_map_code.items()}
    format_map = format_map_standard | format_map_code

    with open(SRC_FILENAME, 'r') as file:
        template = file.read()

    formatted = template.format_map(format_map)
    with open(TRG_FILENAME, 'w') as file:
        file.write(formatted)


if __name__ == "__main__":
    format_readme()
    print(f"Formatted {TRG_FILENAME} file using template: {SRC_FILENAME}")
