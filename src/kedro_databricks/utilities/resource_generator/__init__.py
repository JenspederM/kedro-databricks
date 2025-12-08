"""Resource generators for building Databricks bundle resources.

This package exposes concrete generators that transform Kedro pipelines into
Databricks Asset Bundle resources (jobs). Select the appropriate
generator via `RESOURCE_GENERATORS` to produce resources at node or pipeline
granularity.
"""

from kedro_databricks.utilities.resolver_generics import (
    CompositeResourceResolver,
    ModuleResourceResolver,
    RegistryResourceResolver,
)
from kedro_databricks.utilities.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)
from kedro_databricks.utilities.resource_generator.node_resource_generator import (
    NodeResourceGenerator,
)
from kedro_databricks.utilities.resource_generator.pipeline_resource_generator import (
    PipelineResourceGenerator,
)

RESOURCE_GENERATOR_RESOLVER = CompositeResourceResolver[
    type[AbstractResourceGenerator]
](
    [
        RegistryResourceResolver(
            {
                "node": NodeResourceGenerator,
                "pipeline": PipelineResourceGenerator,
            }
        ),
        ModuleResourceResolver(
            validate_fn=lambda cls: (
                isinstance(cls, type)
                and issubclass(cls, AbstractResourceGenerator)
                and cls is not AbstractResourceGenerator
            )
        ),
    ]
)

__all__ = [
    "AbstractResourceGenerator",
    "NodeResourceGenerator",
    "PipelineResourceGenerator",
    "RESOURCE_GENERATOR_RESOLVER",
]
