from __future__ import annotations

import copy
import re
from typing import Any

from kedro.pipeline.node import Node
from packaging.version import Version

from kedro_databricks.constants import KEDRO_VERSION, MAX_TASK_KEY_LENGTH, OVERRIDE_KEY_MAP, IGNORED_OVERRIDE_KEYS
from kedro_databricks.utilities.logger import get_logger

log = get_logger("utilities.common")


def require_databricks_run_script(_version=KEDRO_VERSION) -> bool:
    """Check if the current Kedro version is less than 0.19.8.

    Kedro 0.19.8 introduced a new `run_script` method that is required for
    running tasks on Databricks. This method is not available in earlier
    versions of Kedro. This function checks if the current Kedro version is
    less than 0.19.8.

    Returns:
        bool: whether the current Kedro version is less than 0.19.8
    """
    return _version < Version("0.19.8")


def version_to_str(version: list[int]) -> str:
    if len(version) != 3:  # noqa: PLR2004 - Semantic versioning requires 3 parts
        raise ValueError(f"Invalid version: {version}")
    return ".".join(str(x) for x in version)


def get_arg_value(args: list[str], arg_name: str) -> str | None:
    for i, arg in enumerate(args):
        if "=" in arg:
            _arg, value = arg.split("=", 1)
            if _arg == arg_name:
                return value
        elif arg == arg_name:
            return args[i + 1]


def remove_nulls(value: dict[str, Any] | list[Any]) -> dict[str, Any] | list[Any]:
    """Remove None values from a dictionary or list.

    Args:
        value (Dict[Any, Any] | List[Dict[Any, Any]]): dictionary or list to remove None values from

    Returns:
        Dict[Any, Any] | List[Dict[Any, Any]]: dictionary or list with None values removed
    """
    non_null = copy.deepcopy(value)
    if isinstance(non_null, dict):
        _remove_nulls_from_dict(non_null)
    elif isinstance(non_null, list):
        _remove_nulls_from_list(non_null)
    return non_null


def _remove_nulls_from_list(lst: list) -> list:
    """Remove None values from a list.

    Args:
        l (List[Dict[Any, Any]]): list to remove None values from
    """
    for i, item in enumerate(lst):
        value = remove_nulls(item)
        if not value:
            del lst[i]
        else:
            lst[i] = value
    return lst


def _remove_nulls_from_dict(d: dict) -> dict:
    """Remove None values from a dictionary.

    Args:
        d (Dict[str, Any]): dictionary to remove None values from

    Returns:
        Dict[str, float | int | str | bool]: dictionary with None values removed
    """
    for k, v in list(d.items()):
        value = remove_nulls(v)
        if not value:
            del d[k]
        else:
            d[k] = value
    return d


def get_entry_point(project_name: str) -> str:
    """Get the entry point for a project.

    Args:
        project_name (str): name of the project

    Returns:
        str: entry point for the project
    """
    entrypoint = project_name.strip().lower()
    entrypoint = re.sub(r" +", " ", entrypoint)
    entrypoint = re.sub(r"[^a-zA-Z_]", "-", entrypoint)
    entrypoint = re.sub(r"(-+)$", "", entrypoint)
    entrypoint = re.sub(r"^(-+)", "", entrypoint)
    return entrypoint


def sort_dict(d: dict[Any, Any], key_order: list[str] | None = None) -> dict[Any, Any]:
    """Recursively sort the keys of a dictionary.

    Args:
        d (Dict[Any, Any]): dictionary to sort
        key_order (List[str]): list of keys to sort by

    Returns:
        Dict[Any, Any]: dictionary with ordered values
    """
    if key_order is None:
        key_order = []
    other_keys = [k for k in d.keys() if k not in key_order]
    order = key_order + other_keys

    return dict(sorted(d.items(), key=lambda x: order.index(x[0])))


def sanitize_name(node: Node | str) -> str:
    """Sanitize the node name to be used as a task key in Databricks.

    Args:
        node (Node | str): Kedro node object or node name

    Returns:
        str: sanitized task key
    """
    if isinstance(node, str):
        _name = node
    else:
        _name = node.name

    if not re.match(r"^[\w\-\_]+$", _name):  # Ensure the name is valid
        log.warning(
            f"Node name '{_name}' contains invalid characters and will be sanitized. "
            "To avoid this use an explicit node name as `node(..., name='valid_name')`."
        )

        _name = re.sub(r"[^\w\_]", "_", _name)
        _name = re.sub(r"_{2,}", "_", _name).lstrip("_").rstrip("_")

    if len(_name) > MAX_TASK_KEY_LENGTH:  # Ensure the name is not too long
        log.warning(
            f"Node name '{_name}' is too long. "
            f"Truncating to {MAX_TASK_KEY_LENGTH} characters."
        )
        _name = _name[:MAX_TASK_KEY_LENGTH]

    return _name


def get_lookup_key(key: str, lookup_map: dict):
    """Get the lookup key for a given key.

    Args:
        key (str): key to get the lookup key for
        lookup_map (Dict): map of keys to lookup keys

    Returns:
        str: lookup key for the given key
    """
    lookup = lookup_map.get(key)
    if lookup is None:
        raise ValueError(f"Key {key} not found in OVERRIDE_KEY_MAP")
    return lookup


def get_defaults(lst: list, lookup_key: str, default_key):
    """Get the default dictionary from a list of dictionaries.

    Args:
        lst (List[Dict]): list of dictionaries
        lookup_key (str): key to use for looking up dictionaries

    Returns:
        Dict: the default dictionary
    """
    for d in lst:
        if isinstance(d, dict) and d.get(lookup_key, "") == default_key:
            return d
    return {}


def update_list_by_key(
    old: list[dict[str, Any]],
    new: list[dict[str, Any]],
    lookup_key: str,
    default: dict[str, Any] = {},
    default_key: str = "default",
):
    """Update a list of dictionaries with another list of dictionaries.

    Args:
        old (List[Dict[str, Any]]): list of dictionaries to update
        new (List[Dict[str, Any]]): list of dictionaries to update with
        lookup_key (str): key to use for looking up dictionaries
        default (Dict[str, Any], optional): default dictionary to use for updating

    Returns:
        List[Dict[str, Any]]: updated list of dictionaries
    """
    validate_list_by_key(old=old, new=new, lookup_key=lookup_key)
    old_obj = {curr.pop(lookup_key): curr for curr in copy.deepcopy(old)}

    literal_updates, regex_updates = _split_updates_by_type(new, lookup_key)
    keys = _collect_target_keys(old_obj, literal_updates)
    aggregated_updates = _initialize_aggregated_updates(
        keys, default, lookup_key, default_key
    )
    _augment_with_regex_updates(
        aggregated_updates,
        regex_updates,
        keys,
        default_key,
        lookup_key,
        skip_default_literal=bool(default),
    )
    _augment_with_literal_updates(
        aggregated_updates,
        literal_updates,
        default_key,
        skip_default_literal=bool(default),
    )
    _apply_aggregated_updates(old_obj, aggregated_updates, default_key)

    return sorted(
        [{lookup_key: k, **v} for k, v in old_obj.items()],
        key=lambda x: x[lookup_key],
    )


def validate_list_by_key(
    old: list[dict[str, Any]], new: list[dict[str, Any]], lookup_key: str
):
    """Validate that a list of dictionaries contains the lookup key.

    Args:
        old (List[Dict[str, Any]]): list of dictionaries to validate
        new (List[Dict[str, Any]]): list of dictionaries to validate
        lookup_key (str): key to use for looking up dictionaries

    Raises:
        ValueError: if the lookup key is not found in any dictionary
    """
    assert isinstance(old, list), (
        f"old must be a list not {type(old)} for key: {lookup_key} - {old}"
    )
    assert isinstance(new, list), (
        f"new must be a list not {type(new)} for key: {lookup_key} - {new}"
    )
    assert all(lookup_key in o for o in old), (
        f"lookup_key {lookup_key} not found in current: {old}"
    )
    assert all(lookup_key in n for n in new), (
        f"lookup_key {lookup_key} not found in updates: {new}"
    )


def get_old_value(result: Any, key: Any, value: Any):
    """Get the old value from a dictionary with a default based on the type of value.

    Args:
        result (Any): dictionary to get the old value from
        key (Any): key to get the old value for
        value (Any): value to determine the default type

    Returns:
        Any: old value from the dictionary or default value
    """
    default = None
    if isinstance(value, dict):
        default = {}
    elif isinstance(value, list):
        default = []
    return result.get(key, default)


def get_value_from_dotpath(validated_conf, dotpath):
    if not isinstance(validated_conf, dict):
        return None
    keys = dotpath.split(".")
    key = keys.pop(0)
    current_level = validated_conf.get(key)
    if current_level is None:
        return None
    elif len(keys) > 0:
        return get_value_from_dotpath(current_level, ".".join(keys))
    else:
        return current_level

def _split_updates_by_type(
    updates: list[dict[str, Any]], lookup_key: str
) -> tuple[
    list[tuple[str, dict[str, Any]]], list[tuple[re.Pattern[str], dict[str, Any], str]]
]:
    """Split update items into literal and regex-based entries while preserving order.

    The function identifies regex-based entries by the ``re:`` prefix on the
    ``lookup_key`` value and compiles them for later matching.

    Args:
        updates (list[dict[str, Any]]): Incoming update items.
        lookup_key (str): Identifier key used to match items (e.g., ``task_key``).

    Returns:
        tuple[list[tuple[str, dict]], list[tuple[Pattern, dict, str]]]:
            - literal updates as ``(key, payload)``
            - regex updates as ``(compiled_pattern, payload, pattern_str)``
    """
    literal_updates: list[tuple[str, dict[str, Any]]] = []
    regex_updates: list[tuple[re.Pattern[str], dict[str, Any], str]] = []
    for upd in copy.deepcopy(updates):
        key_value = upd.pop(lookup_key)
        if isinstance(key_value, str) and key_value.startswith("re:"):
            pattern = key_value[3:]
            try:
                compiled = re.compile(pattern)
            except re.error:
                log.debug(f"Invalid regex pattern for {lookup_key}: {pattern}")
                continue
            regex_updates.append((compiled, upd, pattern))
        else:
            literal_updates.append((key_value, upd))
    return literal_updates, regex_updates


def _collect_target_keys(
    old_obj: dict[str, dict[str, Any]],
    literal_updates: list[tuple[str, dict[str, Any]]],
) -> set[str]:
    """Collect all keys that will be updated by combining existing and new keys.

    Args:
        old_obj (dict[str, dict[str, Any]]): Existing items indexed by key.
        literal_updates (list[tuple[str, dict[str, Any]]]): New literal updates.

    Returns:
        set[str]: Union of existing keys and literal update keys.
    """
    return set(old_obj.keys()).union({k for k, _ in literal_updates})


def _initialize_aggregated_updates(
    keys: set[str], default: dict[str, Any], lookup_key: str, default_key: str
) -> dict[str, dict[str, Any]]:
    """Create a base updates map per key, seeded with the default payload.

    The ``default`` object is deep-copied per target key and the ``lookup_key``
    is removed so only payload fields remain.

    Args:
        keys (set[str]): Keys to initialize.
        default (dict[str, Any]): Default payload to seed per key.
        lookup_key (str): Identifier to strip from the payload.
        default_key (str): Reserved key that is skipped if defaults are provided.

    Returns:
        dict[str, dict[str, Any]]: Aggregated updates initialized per key.
    """
    aggregated_updates: dict[str, dict[str, Any]] = {}
    for key in keys:
        if default and key == default_key:
            continue
        base = copy.deepcopy(default)
        base.pop(lookup_key, None)
        aggregated_updates[key] = base
    return aggregated_updates


def _augment_with_regex_updates(
    aggregated_updates: dict[str, dict[str, Any]],
    regex_updates: list[tuple[re.Pattern[str], dict[str, Any], str]],
    keys: set[str],
    default_key: str,
    lookup_key: str,
    skip_default_literal: bool = False,
) -> None:
    """Apply regex-based updates to all matching keys, in order.

    Args:
        aggregated_updates (dict[str, dict[str, Any]]): In-place target updates per key.
        regex_updates (list[tuple[Pattern, dict, str]]): Ordered list of regex rules.
        keys (set[str]): Target keys to evaluate matches against.
        default_key (str): Reserved default key; optionally skipped.
        lookup_key (str): Identifier used for logging context.
        skip_default_literal (bool): When True, do not apply updates to ``default``.
    """
    for compiled, upd, pattern in regex_updates:
        for key in keys:
            if skip_default_literal and key == default_key:
                continue
            if compiled.fullmatch(str(key)):
                log.debug(
                    f"Applying regex update for {lookup_key} '{key}' matched '{pattern}'"
                )
                aggregated_updates[key].update(copy.deepcopy(upd))


def _augment_with_literal_updates(
    aggregated_updates: dict[str, dict[str, Any]],
    literal_updates: list[tuple[str, dict[str, Any]]],
    default_key: str,
    skip_default_literal: bool = False,
) -> None:
    """Apply literal key updates with highest precedence.

    Args:
        aggregated_updates (dict[str, dict[str, Any]]): In-place target updates per key.
        literal_updates (list[tuple[str, dict[str, Any]]]): Ordered list of literal rules.
        default_key (str): Reserved default key; optionally skipped.
        skip_default_literal (bool): When True, do not apply updates to ``default``.
    """
    for key, upd in literal_updates:
        if skip_default_literal and key == default_key:
            continue
        aggregated_updates.setdefault(key, {}).update(copy.deepcopy(upd))


def _apply_aggregated_updates(
    old_obj: dict[str, dict[str, Any]],
    aggregated_updates: dict[str, dict[str, Any]],
    default_key: str,
) -> None:
    """Merge aggregated updates into the existing items using recursive override.

    Args:
        old_obj (dict[str, dict[str, Any]]): Existing items indexed by key.
        aggregated_updates (dict[str, dict[str, Any]]): Final updates per key.
        default_key (str): Reserved default key for list semantics.
    """
    for key, update in aggregated_updates.items():
        new_item = _override_workflow(old_obj.get(key, {}), update, {}, default_key)  # type: ignore
        old_obj[key] = new_item  # type: ignore


def _deep_merge_dicts(base: dict, extra: dict) -> dict:
    """Deep merge two override dictionaries.

    - Dict values are merged recursively
    - For list-of-dict sections recognized in OVERRIDE_KEY_MAP (e.g., job_clusters, tasks),
      lists are merged by identifier (last wins) to preserve entries from both sides
    - Other list values are replaced by the newer list (last wins)
    - Other values are overwritten by the newer value
    """
    result = copy.deepcopy(base)
    for k, v in extra.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge_dicts(result[k], v)
        elif (
            k in result
            and isinstance(result[k], list)
            and isinstance(v, list)
            and k in OVERRIDE_KEY_MAP
        ):
            result[k] = _merge_override_lists(result[k], v, k)
        else:
            result[k] = copy.deepcopy(v)
    return result


def _merge_override_lists(
    base_list: list[dict[str, Any]], extra_list: list[dict[str, Any]], key_name: str
) -> list[dict[str, Any]]:
    """Merge two override lists of dicts by identifier (last wins).

    This is used when combining default and regex/literal overrides before applying
    to a specific workflow, so that entries like job_clusters are not lost.

    Args:
        base_list (list[dict]): Existing list of items.
        extra_list (list[dict]): Additional items to merge in.
        key_name (str): Section name used to look up the identifier key.

    Returns:
        list[dict]: Merged items keyed by the section's identifier.
    """
    identifier = get_lookup_key(key_name, OVERRIDE_KEY_MAP)
    # Preserve order: start with base keys in order, then append new keys
    base_items = [copy.deepcopy(i) for i in base_list]
    extra_items = [copy.deepcopy(i) for i in extra_list]

    def to_map(
        items: list[dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        order: list[str] = []
        mapping: dict[str, dict[str, Any]] = {}
        for it in items:
            key = it.get(identifier)
            if key is None:
                continue
            order.append(str(key))
            mapping[str(key)] = it
        return mapping, order

    base_map, base_order = to_map(base_items)
    extra_map, extra_order = to_map(extra_items)

    merged_order = base_order[:]
    for k in extra_order:
        if k not in merged_order:
            merged_order.append(k)

    merged_map: dict[str, dict[str, Any]] = {}
    for k in merged_order:
        merged_map[k] = copy.deepcopy(base_map.get(k, {}))
        # overlay with extra (last wins)
        merged_map[k].update(copy.deepcopy(extra_map.get(k, {})))

    return [merged_map[k] for k in merged_order]


def _get_workflow_overrides(overrides, workflow, default_key):
    """Resolve workflow overrides including optional regex-based keys.

    Precedence: default < regex matches (last wins) < exact workflow name
    """
    name = workflow.get("name")

    # Defaults
    default_overrides = overrides.copy().pop(default_key, {})

    # Regex-based matches
    regex_aggregate: dict = {}
    for key, value in overrides.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        if key.startswith("re:"):
            pattern = key[3:]
            try:
                compiled = re.compile(pattern)
            except re.error:
                log.debug(f"Invalid regex pattern skipped: {pattern}")
                continue
            if name is not None and compiled.fullmatch(name):
                log.debug(f"Workflow '{name}' matched regex '{pattern}'")
                regex_aggregate = _deep_merge_dicts(regex_aggregate, value)

    # Exact name override
    workflow_overrides = overrides.copy().pop(name, {})

    merged = {}
    merged = _deep_merge_dicts(merged, default_overrides)
    merged = _deep_merge_dicts(merged, regex_aggregate)
    merged = _deep_merge_dicts(merged, workflow_overrides)

    default_task = get_defaults(merged.get("tasks", []), "task_key", default_key)
    return merged, default_task

def _override_workflow(
    workflow: dict,
    workflow_overrides: dict,
    task_overrides: dict = {},
    default_key: str = "default",
):
    """Override a Databricks workflow with the given overrides.

    Args:
        workflow (Dict): the Databricks workflow
        overrides (Dict): the overrides to apply

    Returns:
        Dict: the Databricks workflow with the overrides applied
    """
    result = {**workflow}

    for key, value in workflow_overrides.items():
        old_value = get_old_value(result, key, value)
        if isinstance(value, dict) and isinstance(old_value, dict):
            result[key] = _override_workflow(
                workflow=old_value,
                workflow_overrides=value,
                task_overrides={},
                default_key=default_key,
            )
        elif isinstance(value, list) and isinstance(old_value, list):
            if isinstance(value[0], dict) and key not in IGNORED_OVERRIDE_KEYS:
                lookup_key = get_lookup_key(key, OVERRIDE_KEY_MAP)
                if lookup_key:
                    result[key] = update_list_by_key(
                        old=old_value,
                        new=value,
                        lookup_key=lookup_key,
                        default=task_overrides if key == "tasks" else {},
                        default_key=default_key,
                    )
            elif key == "parameters":
                # Special case for parameters, which can be a list of strings
                log.debug(f"Overriding parameters for key {key} with value {value}")
                result[key] = old_value + value
            else:
                result[key] = value
        else:
            result[key] = workflow_overrides.get(key, old_value)
    return result
