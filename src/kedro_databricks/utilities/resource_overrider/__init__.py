"""Resource overriders for templating Databricks bundle resources.

This package exposes concrete overriders that Databricks Asset Resources through overrides.
Select the appropriate overrider via `RESOURCE_OVERRIDERS` to produce the desired behavior.
"""

from kedro_databricks.utilities.resolver_generics import (
    CompositeResourceResolver,
    ModuleResourceResolver,
    RegistryResourceResolver,
)
from kedro_databricks.utilities.resource_overrider.abstract_resource_overrider import (
    AbstractResourceOverrider,
)
from kedro_databricks.utilities.resource_overrider.default_resource_overrider import (
    DefaultResourceOverrider,
)
from kedro_databricks.utilities.resource_overrider.jobs_resource_overrider import (
    JobsResourceOverrider,
)

RESOURCE_OVERRIDER_RESOLVER = CompositeResourceResolver[
    type[AbstractResourceOverrider]
](
    [
        RegistryResourceResolver(
            {
                "jobs": JobsResourceOverrider,
            },
            default=DefaultResourceOverrider,
        ),
        ModuleResourceResolver(
            validate_fn=lambda cls: (
                isinstance(cls, type)
                and issubclass(cls, AbstractResourceOverrider)
                and cls is not AbstractResourceOverrider
            )
        ),
    ]
)

__all__ = [
    "AbstractResourceOverrider",
    "JobsResourceOverrider",
    "DefaultResourceOverrider",
    "RESOURCE_OVERRIDER_RESOLVER",
]
