"""Resolver for selecting a resource generator implementation.

This module exposes a ``RESOURCE_GENERATOR_RESOLVER`` which maps a user-facing
name to a concrete resource generator class. It supports:

- Registry lookups for built-in generators (``"node"``, ``"pipeline"``)
- Import-by-dotted-path (e.g. ``path.to.MyCustomGenerator``) with validation to
  ensure the resolved attribute is a subclass of
  ``AbstractResourceGenerator``.

Typical usage:

>>> ResourceGenerator = RESOURCE_GENERATOR_RESOLVER.resolve("node")
>>> ResourceGenerator  # doctest: +ELLIPSIS
<class '...NodeResourceGenerator'>

>>> # Or resolve a custom class
>>> # ResourceGenerator = RESOURCE_GENERATOR_RESOLVER.resolve(
... #     "my_project.generators.CustomGenerator"
... # )
"""

from kedro_databricks.cli.bundle.resource_generator import (
    AbstractResourceGenerator,
    NodeResourceGenerator,
    PipelineResourceGenerator,
)
from kedro_databricks.resource_resolver import (
    CompositeResourceResolver,
    ModuleResourceResolver,
    RegistryResourceResolver,
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
