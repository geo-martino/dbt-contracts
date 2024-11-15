"""
Fills in the variable fields of the README template and generates README.md file.
"""
import re

import docstring_parser

from dbt_contracts import PROGRAM_OWNER_USER, PROGRAM_NAME, DOCUMENTATION_URL
from dbt_contracts.contracts import CONTRACTS, Contract, ParentContract
import docs.reference as docs

SRC_FILENAME = "README.template.md"
TRG_FILENAME = SRC_FILENAME.replace(".template", "")


def format_contract_title(contract: type[Contract], parent_key: str = "") -> str:
    key = f"{parent_key.rstrip('s')}_{str(contract.config_key)}"
    return key.replace("_", " ").title().strip()


def format_contract_reference(contract: type[Contract], parent_key: str = "") -> list[str]:
    """Format the readme template for the contracts reference"""
    contract.__new__(contract)  # needed to populate contract methods lists

    lines = []

    key = str(contract.config_key)
    title = format_contract_title(contract, parent_key)
    lines.extend((f"### {title}", ""))

    method_map = {
        "Filters": sorted(contract.__filtermethods__),
        "Enforcements": sorted(contract.__enforcementmethods__),
    }

    for header, methods in method_map.items():
        lines.extend((f"#### {header}", ""))

        for method_name in methods:
            url = f"{DOCUMENTATION_URL}/{'/'.join(docs.URL_PATH)}/{key}.html#{method_name.replace('_', '-')}"
            method = getattr(contract, method_name).func
            doc = docstring_parser.parse(method.__doc__).short_description.strip()
            doc = re.sub(r"\s*\n\s+", " ", doc)

            method_line = f"- [`{method_name}`]({url}): {doc}"
            lines.append(method_line)

        lines.append("")

    lines.append("")

    if issubclass(contract, ParentContract):
        lines.extend(format_contract_reference(contract.child_type, key))

    return lines


def format_contracts_reference() -> str:
    """Format the readme template for the contracts reference"""
    lines = []
    for contract in CONTRACTS:
        lines.extend(format_contract_reference(contract))

    return "\n".join(lines)


def format_contracts_reference_toc_entry(contract: type[Contract], parent_key: str = "") -> str:
    """Format the readme template for a contracts reference table of contents entry"""
    title = format_contract_title(contract, parent_key)
    return f"  * [{title}](#{title.replace(" ", "-").lower()})"


def format_contracts_reference_toc() -> str:
    """Format the readme template for the contracts reference table of contents"""
    lines = []

    for contract in CONTRACTS:
        lines.append(format_contracts_reference_toc_entry(contract))
        if issubclass(contract, ParentContract):
            key = str(contract.config_key)
            lines.append(format_contracts_reference_toc_entry(contract.child_type, key))

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
