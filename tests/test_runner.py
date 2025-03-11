import json
from argparse import Namespace
from pathlib import Path
from random import choice, sample
from unittest import mock

import pytest
import yaml
from faker import Faker

from dbt_contracts.contracts import ModelContract, SourceContract, ColumnContract
from dbt_contracts.contracts.conditions import properties as c_properties
from dbt_contracts.contracts.result import Result, ModelResult
from dbt_contracts.contracts.terms import properties as t_properties, source as t_source, column as t_column
from dbt_contracts.properties import PropertiesIO
# noinspection PyProtectedMember
from dbt_contracts.runner import _get_default_table_header, ContractsRunner


class TestContractsRunner:
    @pytest.fixture
    def results(self, tmp_path: Path) -> list[Result]:
        """A fixture for the results."""
        return [
            ModelResult(
                name="this is the 1st result",
                path=tmp_path,
                result_type="failure",
                result_level="error",
                result_name="result",
                message="This is an error message.",
            ),
            ModelResult(
                name="this is the 5th result",
                path=tmp_path,
                result_type="warning",
                result_level="warning",
                result_name="result",
                message="This is a warning message.",
            ),
            ModelResult(
                name="this is the 3rd result",
                path=tmp_path,
                result_type="another very great success",
                result_level="info",
                result_name="result",
                message="This is a success message.",
            ),
            ModelResult(
                name="this is the 4th result",
                path=tmp_path,
                result_type="failure",
                result_level="error",
                result_name="result",
                message="This is an error message.",
            ),
            ModelResult(
                name="this is the 2nd result",
                path=tmp_path,
                result_type="a very great success",
                result_level="info",
                result_name="result",
                message="This is a success message.",
            ),
        ]

    @pytest.fixture
    def runner(self, tmp_path: Path) -> ContractsRunner:
        """Fixture for the contracts runner to test."""
        contracts = []
        conditions = [
            c_properties.NameCondition(include=["model1", "model2"]),
            c_properties.TagCondition(tags=["include"])
        ]
        terms = [
            t_properties.HasRequiredTags(tags="required_tag"),
        ]
        contracts.append(ModelContract(conditions=conditions, terms=terms))

        conditions = [
            c_properties.NameCondition(include=["source1", "source2"]),
            c_properties.TagCondition(tags=["include"])
        ]
        terms = [
            t_properties.HasRequiredTags(tags="required_tag"),
            t_source.HasLoader(),
        ]
        contracts.append(SourceContract(conditions=conditions, terms=terms))

        conditions = [
            c_properties.TagCondition(tags=["valid"])
        ]
        terms = [
            t_column.HasDataType(),
        ]
        contracts.append(ColumnContract(parent=contracts[0], conditions=conditions, terms=terms))

        for contract in contracts:
            assert not contract.needs_manifest
            assert not contract.needs_catalog

        config = Namespace(
            project_root=str(tmp_path),
            project_name="name",
        )

        # noinspection PyTypeChecker
        return ContractsRunner(contracts=contracts, config=config)

    def test_get_default_table_header(self, results: list[Result]):
        """Test the `get_default_table_header` function."""
        result = results[0]
        assert not result.properties_path

        output = _get_default_table_header(result)
        assert result.result_type in output
        assert str(result.path) in output
        assert str(result.properties_path) not in output

        result.properties_path = Path("properties_path.yml")
        output = _get_default_table_header(result)
        assert result.result_type in output
        assert str(result.path) in output
        assert str(result.properties_path) in output

    def test_cached_properties(self, runner: ContractsRunner):
        config = runner.config
        assert runner.config is config

        dbt = runner.dbt
        assert runner.dbt is dbt

        with mock.patch("dbt_contracts.runner.dbt_cli.get_manifest", return_value="manifest"):
            manifest = runner.manifest
            assert runner.manifest is manifest
            assert runner.dbt.manifest is manifest

        with mock.patch("dbt_contracts.runner.dbt_cli.get_catalog", return_value="catalog"):
            catalog = runner.catalog
            assert runner.catalog is catalog

    def test_set_paths(self, runner: ContractsRunner):
        def _get_absolute_path(p: str | Path) -> Path:
            path = Path(p)
            if path == paths[-1]:
                return path
            return Path(runner.config.project_root, path)

        paths = [
            str(Path("path", "to", "model.sql")),
            Path("path", "to", "another", "model.sql"),
            Path("path", "to", "dir"),
            Path("path", "not", "in", "project"),
        ]

        with mock.patch("dbt_contracts.runner.get_absolute_project_path", new=_get_absolute_path):
            runner.paths = paths

        assert runner.paths.include == tuple(map(str, paths[:3]))

        with mock.patch("dbt_contracts.runner.get_absolute_project_path", new=_get_absolute_path):
            runner.paths = paths[0]
        assert runner.paths.include == tuple(map(str, paths[:1]))

        runner.paths = []
        assert runner.paths is None

    def test_resolve_config_path(self, tmp_path: Path):
        path = tmp_path.joinpath("path", "to", "dir")
        path.mkdir(parents=True)

        expected = path.joinpath(ContractsRunner.default_config_file_name)
        expected.touch()

        assert ContractsRunner._resolve_config_path(path) == expected

    # noinspection PyTestUnpassedFixture
    def test_from_objects(self):
        args = Namespace(config="path/to/config.yml")
        config = Namespace(args=args)

        with (
            mock.patch.object(ContractsRunner, "from_file") as mock_file,
            mock.patch("dbt_contracts.runner.dbt_cli.get_config", return_value="config") as mock_config
        ):
            # noinspection PyTypeChecker
            ContractsRunner.from_config(config)
            mock_file.assert_called_with(config.args.config, config=config)

            ContractsRunner.from_args(args)
            mock_file.assert_called_with(args.config, config=mock_config.return_value)

    def test_from_file(self, tmp_path: Path):
        path = tmp_path.joinpath(ContractsRunner.default_config_file_name)
        path.with_suffix("")

        with (
            mock.patch.object(ContractsRunner, "from_yaml") as mock_yaml,
            mock.patch.object(ContractsRunner, "from_json") as mock_json,
        ):
            # uses yml extension by default when no extension given
            # noinspection PyTypeChecker
            ContractsRunner.from_file(path, config=None)
            mock_yaml.assert_called_once()
            mock_json.assert_not_called()

            # noinspection PyTypeChecker
            ContractsRunner.from_file(path.with_suffix(choice((".yml", ".yaml"))), config=None)
            assert len(mock_yaml.mock_calls) == 2
            mock_json.assert_not_called()

            # noinspection PyTypeChecker
            ContractsRunner.from_file(path.with_suffix(".json"), config=None)
            assert len(mock_yaml.mock_calls) == 2
            mock_json.assert_called_once()

        with pytest.raises(Exception):
            # noinspection PyTypeChecker
            ContractsRunner.from_file(path.with_suffix(".mp3"), config=None)

    def test_from_yaml(self, tmp_path: Path):
        path = tmp_path.joinpath("config.yml")
        config = {"models": [{"filters": [], "terms": []}]}
        with path.open("w") as f:
            yaml.dump(config, f)

        with mock.patch.object(ContractsRunner, "from_dict") as mock_dict:
            # noinspection PyTypeChecker
            ContractsRunner.from_yaml(path, config=None)
            mock_dict.assert_called_once_with(config, config=None)

    def test_from_json(self, tmp_path: Path):
        path = tmp_path.joinpath("config.json")
        config = {"models": [{"filters": [], "terms": []}]}
        with path.open("w") as f:
            json.dump(config, f)

        with mock.patch.object(ContractsRunner, "from_dict") as mock_dict:
            # noinspection PyTypeChecker
            ContractsRunner.from_json(path, config=None)
            mock_dict.assert_called_once_with(config, config=None)

    def test_from_dict(self):
        config = {
            "contracts": {
                "models": [
                    {
                        "filters": sample([cls._name() for cls in ModelContract.__supported_conditions__], k=3),
                        "terms": sample([cls._name() for cls in ModelContract.__supported_terms__], k=3),
                    },
                    {
                        "filters": sample([cls._name() for cls in ModelContract.__supported_conditions__], k=3),
                        "terms": sample([cls._name() for cls in ModelContract.__supported_terms__], k=3),
                    }
                ],
                "sources": [
                    {
                        "filters": sample([cls._name() for cls in SourceContract.__supported_conditions__], k=3),
                        "terms": sample([cls._name() for cls in SourceContract.__supported_terms__], k=3),
                    }
                ]
            }
        }

        with mock.patch.object(ContractsRunner, "_create_contracts_from_config") as mock_create:
            # noinspection PyTypeChecker
            ContractsRunner.from_dict(config, config=None)

            assert len(mock_create.mock_calls) == 3 * 2  # *2 for iter calls
            mock_create.assert_any_call("models", config=config["contracts"]["models"][0])
            mock_create.assert_any_call("models", config=config["contracts"]["models"][1])
            mock_create.assert_any_call("sources", config=config["contracts"]["sources"][0])

    def test_create_contracts_from_config(self):
        models_config = {
            "filters": sample([cls._name() for cls in ModelContract.__supported_conditions__], k=3),
            "terms": sample([cls._name() for cls in ModelContract.__supported_terms__], k=3),
        }
        sources_config = {
            "filters": sample([cls._name() for cls in ModelContract.__supported_conditions__], k=3),
            "terms": sample([cls._name() for cls in ModelContract.__supported_terms__], k=3),
        }
        columns_config = {
            "filters": sample([cls._name() for cls in ColumnContract.__supported_conditions__], k=3),
            "terms": sample([cls._name() for cls in ColumnContract.__supported_terms__], k=3),
        }

        with (
            mock.patch.object(ModelContract, "from_dict", return_value=ModelContract()) as mock_model,
            mock.patch.object(SourceContract, "from_dict", return_value=SourceContract()) as mock_source,
            mock.patch.object(ColumnContract, "from_dict", return_value=ColumnContract()) as mock_column,
        ):
            ContractsRunner._create_contracts_from_config("model", models_config)
            mock_model.assert_called_once()
            mock_source.assert_not_called()
            mock_column.assert_not_called()

            ContractsRunner._create_contracts_from_config("source", sources_config)
            mock_model.assert_called_once()
            mock_source.assert_called_once()
            mock_column.assert_not_called()

            sources_config[ColumnContract.__config_key__] = columns_config
            ContractsRunner._create_contracts_from_config("source", sources_config)
            mock_column.assert_called_once()

    def test_set_artifacts_on_contracts_skips_artifacts(self, runner: ContractsRunner):
        # manifest is set by default as it will always be available
        runner.__dict__["manifest"] = "manifest"

        runner._set_artifacts_on_contracts(runner._contracts)

        # cached properties were not set
        assert "catalog" not in runner.__dict__

        for contract in runner._contracts:
            assert not contract.needs_catalog
            assert contract.catalog is None

    def test_set_artifacts_on_contracts_sets_artifacts(self, runner: ContractsRunner):
        # set cached properties manually
        runner.__dict__["manifest"] = "manifest"
        runner.__dict__["catalog"] = "catalog"

        contract = choice(list(runner._contracts))
        contract.terms.append(
            choice([term() for term in contract.__supported_terms__ if term.needs_manifest])
        )
        contract.terms.append(
            choice([term() for term in contract.__supported_terms__ if term.needs_catalog])
        )
        assert contract.needs_manifest
        assert contract.needs_catalog

        runner._set_artifacts_on_contracts(runner._contracts)
        for c in runner._contracts:
            assert c.manifest == "manifest"
            assert c.catalog == "catalog"

    def test_set_artifacts_on_contracts_sets_paths(self, runner: ContractsRunner):
        runner.__dict__["manifest"] = "manifest"
        runner._paths = c_properties.PathCondition(include="path/to/model.sql")

        runner._set_artifacts_on_contracts(runner._contracts)
        for contract in runner._contracts:
            if c_properties.PathCondition in contract.__supported_conditions__:
                assert runner.paths in contract.conditions
            else:
                assert runner.paths not in contract.conditions

        # only adds the condition once
        runner._set_artifacts_on_contracts(runner._contracts)
        for contract in runner._contracts:
            if c_properties.PathCondition in contract.__supported_conditions__:
                assert contract.conditions.count(runner.paths) == 1
            else:
                assert contract.conditions.count(runner.paths) == 0

    def test_get_contract_by_key(self, runner: ContractsRunner):
        contract = choice(list(runner._contracts))
        assert runner._get_contract_by_key(contract.config_key) == contract

        with pytest.raises(Exception):
            runner._get_contract_by_key("unknown key")

    def test_validate_runs_all_contracts(self, runner: ContractsRunner):
        with (
            mock.patch.object(ModelContract, "validate") as mock_model,
            mock.patch.object(SourceContract, "validate") as mock_source,
            mock.patch.object(ColumnContract, "validate") as mock_column,
            mock.patch.object(ContractsRunner, "_set_artifacts_on_contracts") as mock_set_artifacts,
        ):
            runner.validate()

            mock_set_artifacts.assert_called_once()
            mock_model.assert_called_once()
            mock_source.assert_called_once()
            mock_column.assert_called_once()

    def test_validate_runs_selected_contract(self, runner: ContractsRunner):
        runner.__dict__["manifest"] = "manifest"

        with (
            mock.patch.object(ModelContract, "validate") as mock_model,
            mock.patch.object(SourceContract, "validate") as mock_source,
            mock.patch.object(ColumnContract, "validate") as mock_column,
        ):
            key = ModelContract.child_config_key
            terms = ["term_1", "term_2"]
            runner.validate(key, terms=terms)

            mock_model.assert_not_called()
            mock_source.assert_not_called()
            mock_column.assert_called_once_with(terms=terms)

    def test_validate_logs_results(self, runner: ContractsRunner, results: list[Result]):
        runner.__dict__["manifest"] = "manifest"
        assert all(not contract.context.results for contract in runner._contracts)

        with (
            mock.patch.object(ModelContract, "validate"),
            mock.patch.object(SourceContract, "validate"),
            mock.patch.object(ColumnContract, "validate"),
            mock.patch.object(ContractsRunner, "log_results") as mock_log_results,
        ):
            assert not runner.validate()
            mock_log_results.assert_not_called()
            assert all(not contract.context.results for contract in runner._contracts)

            contract = choice(list(runner._contracts))
            contract.context.results.extend(results.copy())

            assert runner.validate() == results
            mock_log_results.assert_called_once()
            mock_log_results.assert_any_call(results)
            assert all(not contract.context.results for contract in runner._contracts)

    def test_generate_runs_all_contracts(self, runner: ContractsRunner, tmp_path: Path):
        with (
            mock.patch.object(ModelContract, "generate", return_value={tmp_path: 1}) as mock_model,
            mock.patch.object(SourceContract, "generate", return_value={tmp_path: 2}) as mock_source,
            mock.patch.object(ColumnContract, "generate", return_value={tmp_path: 3}) as mock_column,
            mock.patch.object(ContractsRunner, "_set_artifacts_on_contracts") as mock_set_artifacts,
            mock.patch.object(ContractsRunner, "_log_generated_paths") as mock_log_paths,
            mock.patch.object(PropertiesIO, "save") as mock_save,
            mock.patch("dbt_contracts.runner.dbt_cli.get_manifest", return_value="manifest") as mock_manifest,
        ):
            results = runner.generate()
            assert results == {tmp_path: 6}

            mock_set_artifacts.assert_any_call(runner._contracts, force=True)
            mock_model.assert_called_once()
            mock_source.assert_called_once()
            mock_column.assert_called_once()

            mock_log_paths.assert_called_once_with({})  # nothing was saved so results log is empty
            mock_save.assert_called_with(results)
            mock_manifest.assert_any_call(runner=runner.dbt, config=runner.config, logger=runner.logger, refresh=True)
            mock_set_artifacts.assert_called_with(runner._contracts)

    def test_generate_runs_selected_contract(self, runner: ContractsRunner, tmp_path: Path):
        with (
            mock.patch.object(ModelContract, "generate", return_value={tmp_path: 1}) as mock_model,
            mock.patch.object(SourceContract, "generate", return_value={tmp_path: 3}) as mock_source,
            mock.patch.object(ColumnContract, "generate", return_value={tmp_path: 8}) as mock_column,
            mock.patch.object(ContractsRunner, "_set_artifacts_on_contracts"),
            mock.patch.object(ContractsRunner, "_log_generated_paths") as mock_log_paths,
            mock.patch.object(PropertiesIO, "save", return_value=[tmp_path]) as mock_save,
            mock.patch("dbt_contracts.runner.dbt_cli.get_manifest", return_value="manifest") as mock_manifest,
        ):
            key = ModelContract.child_config_key
            results = runner.generate(key)
            assert results == {tmp_path: 8}

            mock_model.assert_not_called()
            mock_source.assert_not_called()
            mock_column.assert_called_once()

            mock_log_paths.assert_called_once_with(results)
            mock_save.assert_called_with(results)
            mock_manifest.assert_any_call(runner=runner.dbt, config=runner.config, logger=runner.logger, refresh=True)

    def test_build_results(self, runner: ContractsRunner, results: list[Result]):
        assert runner._build_results([]) == ""
        assert runner._build_results(results) != ""

    def test_write_results(self, runner: ContractsRunner, results: list[Result], tmp_path: Path):
        output_type = choice(list(runner.output_writers_map))
        assert runner.write_results([], path=tmp_path, output_type=output_type) is None

        output_path = runner.write_results(results, path=tmp_path, output_type="json")
        assert output_path == tmp_path.joinpath(runner.default_output_file_name).with_suffix(".json")
        assert output_path.is_file()

        # uses given filename when not dir
        output_path = tmp_path.joinpath("file.json")
        assert runner.write_results(results, path=output_path, output_type="json") == output_path

    def test_write_results_as_text(self, runner: ContractsRunner, results: list[Result], tmp_path: Path):
        output_path = runner.write_results(results, path=tmp_path, output_type="text")
        assert output_path == tmp_path.joinpath(runner.default_output_file_name).with_suffix(".txt")

        with output_path.open("r") as f:
            assert f.read() == runner._build_results(results)

    def test_write_results_as_json(self, runner: ContractsRunner, results: list[Result], tmp_path: Path):
        output_path = runner.write_results(results, path=tmp_path, output_type="json")
        assert output_path == tmp_path.joinpath(runner.default_output_file_name).with_suffix(".json")

        expected = json.dumps([result.model_dump_json() for result in results], indent=2)
        with output_path.open("r") as f:
            assert f.read() == expected

    def test_write_results_as_jsonl(self, runner: ContractsRunner, results: list[Result], tmp_path: Path):
        output_path = runner.write_results(results, path=tmp_path, output_type="jsonl")
        assert output_path == tmp_path.joinpath(runner.default_output_file_name).with_suffix(".json")

        expected = "\n".join(json.dumps(result.model_dump_json()) for result in results) + "\n"
        with output_path.open("r") as f:
            assert f.read() == expected

    def test_write_results_as_github_annotation(
            self, runner: ContractsRunner, results: list[Result], faker: Faker, tmp_path: Path
    ):
        output_path = runner.write_results(results, path=tmp_path, output_type="github-annotations")
        assert output_path == tmp_path.joinpath(runner.default_output_file_name).with_suffix(".json")

        assert not any(result.can_format_to_github_annotation for result in results)
        with output_path.open("r") as f:
            assert f.read() == "[]"

        for result in results:
            result.properties_start_line = faker.random_int()
            result.properties_end_line = faker.random_int(min=result.properties_start_line)

        assert all(result.can_format_to_github_annotation for result in results)
        output_path = runner.write_results(results, path=tmp_path, output_type="github-annotations")

        expected = json.dumps([result.as_github_annotation() for result in results], indent=2)
        with output_path.open("r") as f:
            assert f.read() == expected
