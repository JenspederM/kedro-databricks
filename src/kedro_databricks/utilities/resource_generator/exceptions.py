from kedro_databricks.utilities.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)


class MemoryDatasetError(Exception):
    def __init__(
        self, klass: AbstractResourceGenerator, undeclared_datasets: dict[str, set[str]]
    ) -> None:
        resource_lines = []
        for name, items in undeclared_datasets.items():
            items_str = ",".join(items)
            resource_lines.append(f"  - {name}: {items_str}")
        resource_line = "\n".join(resource_lines)
        msg = f"""Resource Generator of type {klass.__class__.__name__} does not support MemoryDatasets.

The following inputs/outputs are not specified in your catalog:
{resource_line}

If This is intentional, you can use --resource-generator='pipeline' to generate a single job"""
        super().__init__(msg)
