"""Generic, composable resource resolvers.

This module defines a small set of primitives to resolve arbitrary "resources"
from string identifiers. It includes:

- ``RegistryResourceResolver``: resolves from a given mapping/registry.
- ``ModuleResourceResolver``: resolves a dotted path (``module.attr``) and
  optionally validates the resulting attribute.
- ``CompositeResourceResolver``: chains multiple resolvers and aggregates
  their errors for better diagnostics.

These utilities allow flexible lookups (e.g., built-in names via a registry,
or user-provided dotted paths) while surfacing clear error messages.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

ResourceType = TypeVar("ResourceType")


class ResourceResolverError(Exception):
    """Base error for all resource resolver failures.

    This is the common ancestor for all resolver exceptions and is also used
    by composite resolvers to aggregate multiple failures into a single
    exception with a readable, concatenated message.
    """

    @classmethod
    def for_errors(
        cls, errors: Sequence["ResourceResolverError"]
    ) -> "ResourceResolverError":
        """Construct an aggregated resolver error.

        Args:
            errors: The list of individual ``ResourceResolverError`` instances
                raised by underlying resolvers.

        Returns:
            ResourceResolverError: A single error whose message concatenates
            the messages of all provided errors, separated by semicolons.
        """
        messages = "; ".join(str(error) for error in errors)
        return cls(f"Multiple resource resolution errors: {messages}")


class ResourceNotFoundError(ResourceResolverError):
    """Raised when a resource cannot be resolved.

    Provides helpers to produce consistent, user-friendly error messages
    for single- and multi-resolver failures.
    """

    @classmethod
    def for_value(
        cls, value: str, message: str | None = None
    ) -> "ResourceNotFoundError":
        """Construct an error for a single value.

        Args:
            value: The unresolved identifier.
            message: Optional details about why resolution failed.

        Returns:
            ResourceNotFoundError: A descriptive error instance.
        """
        return cls(
            f"Resource '{value}' not found: {message}."
            if message
            else f"Resource '{value}' not found."
        )


class ResourceImportError(ResourceResolverError):
    """Raised when a resource cannot be imported.

    Typical causes include:
    - The value is not a dotted path of the form ``module.attribute``.
    - The specified module cannot be imported.

    This error is commonly emitted by ``ModuleResourceResolver`` when the
    path format is invalid or the module import fails.
    """

    @classmethod
    def for_value(cls, value: str, message: str | None = None) -> "ResourceImportError":
        """Construct an error for a single value.

        Args:
            value: The unresolved identifier.
            message: Optional details about why resolution failed.

        Returns:
            ResourceImportError: A descriptive error instance.
        """
        return cls(
            f"Resource '{value}' could not be imported: {message}."
            if message
            else f"Resource '{value}' could not be imported."
        )


class ResourceInvalidError(ResourceResolverError):
    """Raised when a resource is invalid.

    Emitted when a resolved value fails a post-resolution validation (e.g.,
    does not satisfy a provided ``validate_fn``). This is distinct from
    ``ResourceNotFoundError``, which indicates the resource could not be
    located at all.
    """

    @classmethod
    def for_value(
        cls, value: str, message: str | None = None
    ) -> "ResourceInvalidError":
        """Construct an error for a single value.

        Args:
            value: The unresolved identifier.
            message: Optional details about why resolution failed.

        Returns:
            ResourceInvalidError: A descriptive error instance.
        """
        return cls(
            f"Resource '{value}' is invalid: {message}."
            if message
            else f"Resource '{value}' is invalid."
        )


class ResourceResolver(ABC, Generic[ResourceType]):
    """Abstract protocol for resolving a value into a resource of type ``T``."""

    @abstractmethod
    def resolve(self, value: str) -> ResourceType:
        """Resolve a string identifier into a resource.

        Args:
            value: The identifier to resolve.

        Returns:
            The resolved resource.

        Raises:
            ResourceNotFoundError: If the resource cannot be found.
        """


@dataclass
class RegistryResourceResolver(Generic[ResourceType], ResourceResolver[ResourceType]):
    registry: dict[str, ResourceType]
    default: ResourceType | None = None

    def resolve(self, value: str) -> ResourceType:
        """Resolve using a static registry mapping.

        Raises ``ResourceNotFoundError`` when the key is missing.
        """
        if value not in self.registry and self.default is None:
            raise ResourceNotFoundError.for_value(value)
        elif value not in self.registry and self.default is not None:
            return self.default
        return self.registry[value]


@dataclass
class ModuleResourceResolver(Generic[ResourceType], ResourceResolver[ResourceType]):
    validate_fn: Callable[[Any], bool] | None = None

    def resolve(self, value: str) -> ResourceType:
        """Resolve a dotted path (``module.attr``) to a Python attribute.

        If ``validate_fn`` is provided, the resolved attribute must satisfy it;
        otherwise a ``ResourceInvalidError`` is raised.

        Raises:
            ResourceImportError: If the value is not of the form
                ``module.attribute`` or the module import fails.
            ResourceNotFoundError: If the module is found but the attribute
                is missing.
            ResourceInvalidError: If ``validate_fn`` is provided and returns
                ``False`` for the resolved attribute.
        """
        try:
            module_path, attribute_name = value.rsplit(".", 1)
        except ValueError as exc:
            raise ResourceImportError.for_value(
                value, "Invalid resource path format, expected 'module.attribute'"
            ) from exc

        try:
            module = __import__(module_path, fromlist=[attribute_name])
        except ImportError as exc:
            raise ResourceImportError.for_value(
                value, f"Module '{module_path}' could not be imported"
            ) from exc

        try:
            attribute = getattr(module, attribute_name)
        except AttributeError as exc:
            raise ResourceNotFoundError.for_value(
                value,
                f"Attribute '{attribute_name}' not found in module '{module_path}'",
            ) from exc

        if self.validate_fn and not self.validate_fn(attribute):
            raise ResourceInvalidError.for_value(value)

        return attribute


@dataclass
class CompositeResourceResolver(Generic[ResourceType], ResourceResolver[ResourceType]):
    resolvers: Sequence[ResourceResolver[ResourceType]]

    def resolve(self, value: str) -> ResourceType:
        """Try multiple resolvers in order, returning the first success.

        If all resolvers fail, a combined ``ResourceNotFoundError`` is raised
        containing the individual failure messages for easier debugging.
        """
        errors: list[ResourceResolverError] = []
        for resolver in self.resolvers:
            try:
                return resolver.resolve(value)
            except ResourceResolverError as exc:
                errors.append(exc)
        raise ResourceNotFoundError.for_errors(errors)
