from kedro_databricks.cli.bundle.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)
from kedro_databricks.cli.bundle.resource_generator.node_resource_generator import (
    NodeResourceGenerator,
)
from kedro_databricks.cli.bundle.resource_generator.pipeline_resource_generator import (
    PipelineResourceGenerator,
)

RESOURCE_GENERATORS: dict[str, type[AbstractResourceGenerator]] = {
    "node": NodeResourceGenerator,
    "pipeline": PipelineResourceGenerator,
}

__all__ = [
    "RESOURCE_GENERATORS",
    "AbstractResourceGenerator",
    "NodeResourceGenerator",
    "PipelineResourceGenerator",
]
