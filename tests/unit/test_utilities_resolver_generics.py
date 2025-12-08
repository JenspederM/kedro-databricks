import pytest

from kedro_databricks.utilities.resolver_generics import (
    CompositeResourceResolver,
    ModuleResourceResolver,
    RegistryResourceResolver,
    ResourceImportError,
    ResourceInvalidError,
    ResourceNotFoundError,
    ResourceResolverError,
)

TEST_STR = "test_string"


@pytest.fixture
def registry():
    return {"resource1": "value1", "resource2": "value2"}


@pytest.fixture
def resource_resolver(registry):
    return CompositeResourceResolver[str](
        [RegistryResourceResolver(registry), ModuleResourceResolver()]
    )


def test_registry_resolver_found(resource_resolver, registry):
    for key, value in registry.items():
        assert resource_resolver.resolve(key) == value


def test_module_resolver_found(resource_resolver):
    assert resource_resolver.resolve(f"{__name__}.TEST_STR") == TEST_STR


def test_resolver_not_found(resource_resolver):
    with pytest.raises(ResourceResolverError):
        resource_resolver.resolve("non_existent_resource")


def test_registry_resolver_missing_key_message(registry):
    resolver = RegistryResourceResolver(registry)
    with pytest.raises(ResourceNotFoundError) as excinfo:
        resolver.resolve("missing")
    msg = str(excinfo.value)
    assert "Resource 'missing' not found" in msg


def test_module_resolver_invalid_format_message():
    resolver = ModuleResourceResolver()
    with pytest.raises(ResourceImportError) as excinfo:
        resolver.resolve("nodotpath")
    msg = str(excinfo.value)
    assert "Invalid resource path format" in msg
    assert "module.attribute" in msg


def test_module_resolver_module_not_found_message():
    resolver = ModuleResourceResolver()
    with pytest.raises(ResourceImportError) as excinfo:
        resolver.resolve("nonexistent.module.attr")
    msg = str(excinfo.value)
    assert "Module 'nonexistent.module' could not be imported" in msg


def test_module_resolver_attribute_not_found_message():
    resolver = ModuleResourceResolver()
    with pytest.raises(ResourceNotFoundError) as excinfo:
        resolver.resolve(f"{__name__}.NOT_THERE")
    msg = str(excinfo.value)
    assert "Attribute 'NOT_THERE' not found" in msg
    assert __name__ in msg


def test_module_resolver_returns_builtin_type():
    resolver = ModuleResourceResolver()
    assert resolver.resolve("builtins.str") is str


def test_composite_prefers_first_resolver_over_module(registry):
    # Shadow a valid module path with a registry entry to ensure order is respected
    shadow_key = f"{__name__}.TEST_STR"
    shadow_registry = {**registry, shadow_key: "shadowed"}
    resolver = CompositeResourceResolver[str](
        [RegistryResourceResolver(shadow_registry), ModuleResourceResolver()]
    )
    assert resolver.resolve(shadow_key) == "shadowed"


def test_composite_aggregates_errors_from_all_resolvers():
    resolver = CompositeResourceResolver(
        [RegistryResourceResolver({}), ModuleResourceResolver()]
    )
    with pytest.raises(ResourceResolverError) as excinfo:
        # No dot triggers invalid format for ModuleResourceResolver; registry is empty
        resolver.resolve("missingkey")
    msg = str(excinfo.value)
    assert msg.startswith("Multiple resource resolution errors:")
    assert "Resource 'missingkey' not found" in msg  # from registry resolver
    assert "Invalid resource path format" in msg  # from module resolver


def test_module_resolver_validate_fn_accepts():
    # Accept only callables; builtins.str is callable
    resolver = ModuleResourceResolver(validate_fn=lambda obj: callable(obj))
    assert resolver.resolve("builtins.str") is str


def test_module_resolver_validate_fn_rejects():
    # Accept only callables; TEST_STR is not callable -> rejected
    resolver = ModuleResourceResolver(validate_fn=lambda obj: callable(obj))
    with pytest.raises(ResourceInvalidError, match="is invalid") as excinfo:
        resolver.resolve(f"{__name__}.TEST_STR")
    msg = str(excinfo.value)
    assert "is invalid" in msg
