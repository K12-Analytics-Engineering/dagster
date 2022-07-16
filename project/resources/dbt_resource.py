from dagster import ResourceDefinition, Permissive, resource
from dagster_dbt.cli.resources import DbtCliResource
from dagster_dbt.cli.constants import (
    CLI_COMMON_FLAGS_CONFIG_SCHEMA,
    CLI_COMMON_OPTIONS_CONFIG_SCHEMA,
)


class DbtResource(DbtCliResource):
    def run(self, **kwargs):
        self.cli("deps")  #  install dbt packages
        self.cli(
            "run-operation stage_external_sources"
        )  # create bigquery external tables
        super().run(**kwargs)


@resource(
    config_schema=Permissive(
        {
            k.replace("-", "_"): v
            for k, v in dict(
                **CLI_COMMON_FLAGS_CONFIG_SCHEMA, **CLI_COMMON_OPTIONS_CONFIG_SCHEMA
            ).items()
        }
    ),
    description="A resource that can run dbt CLI commands.",
)
def dbt_cli_resource(context) -> DbtCliResource:
    # set of options in the config schema that are not flags
    non_flag_options = {k.replace("-", "_") for k in CLI_COMMON_OPTIONS_CONFIG_SCHEMA}
    # all config options that are intended to be used as flags for dbt commands
    default_flags = {
        k: v for k, v in context.resource_config.items() if k not in non_flag_options
    }
    return DbtResource(
        executable=context.resource_config["dbt_executable"],
        default_flags=default_flags,
        warn_error=context.resource_config["warn_error"],
        ignore_handled_error=context.resource_config["ignore_handled_error"],
        target_path=context.resource_config["target_path"],
        logger=context.log,
    )
