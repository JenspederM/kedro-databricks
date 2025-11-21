import copy
import re
from typing import Any

from kedro_databricks.constants import IGNORED_OVERRIDE_KEYS, OVERRIDE_KEY_MAP
from kedro_databricks.logger import get_logger

log = get_logger("bundle").getChild(__name__)


def override_resources(bundle: dict, overrides: dict, default_key) -> dict[str, Any]:
    """Override the resources in a Databricks bundle.

    This function applies the given overrides to the resources in a Databricks bundle.

    Args:
        bundle (Dict): the Databricks bundle
        overrides (Dict): the overrides to apply
        default_key (str): the default key to use for overrides

    Raises:
        ValueError: if the workflow or overrides are not dictionaries
        ValueError: if the key in overrides is not found in OVERRIDE_KEY_MAP

    Returns:
        Dict[str, Any]: the Databricks bundle with the overrides applied
    """
    result = {"resources": {"jobs": {}}}
    for name, workflow in bundle.get("resources", {}).get("jobs", {}).items():
        if not isinstance(workflow, dict):
            raise ValueError(f"workflow must be a dictionary not {type(workflow)}")
        if not isinstance(overrides, dict):
            raise ValueError(f"overrides must be a dictionary not {type(overrides)}")
        workflow_overrides, task_overrides = _get_workflow_overrides(
            overrides, workflow, default_key
        )
        result["resources"]["jobs"][name] = _override_workflow(
            workflow=workflow,
            workflow_overrides=workflow_overrides,
            task_overrides=task_overrides,
            default_key=default_key,
        )
    return result


def _get_lookup_key(key: str):
    """Return the identifier key used to merge lists for a given override section.

    This looks up the appropriate unique key (e.g., ``task_key`` for ``tasks``)
    from ``OVERRIDE_KEY_MAP``. If the section is unknown, a ``ValueError`` is raised.

    Args:
        key (str): Top-level section name (e.g., ``tasks``, ``job_clusters``).

    Returns:
        str: The unique identifier key for items within the section.

    Raises:
        ValueError: If the section is not recognized in ``OVERRIDE_KEY_MAP``.
    """
    lookup = OVERRIDE_KEY_MAP.get(key)
    if lookup is None:
        raise ValueError(f"Key {key} not found in OVERRIDE_KEY_MAP")
    return lookup


def _get_defaults(lst: list, lookup_key: str, default_key):
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


def _update_list_by_key(
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
    _validate_list_by_key(old=old, new=new, lookup_key=lookup_key)
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
        [{lookup_key: k, **v} for k, v in old_obj.items()], key=lambda x: x[lookup_key]
    )


def _validate_list_by_key(
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
    assert isinstance(
        old, list
    ), f"old must be a list not {type(old)} for key: {lookup_key} - {old}"
    assert isinstance(
        new, list
    ), f"new must be a list not {type(new)} for key: {lookup_key} - {new}"
    assert all(
        lookup_key in o for o in old
    ), f"lookup_key {lookup_key} not found in current: {old}"
    assert all(
        lookup_key in n for n in new
    ), f"lookup_key {lookup_key} not found in updates: {new}"


def _get_old_value(result: Any, key: Any, value: Any):
    """Return the current value for ``key`` in ``result`` with a sensible default.

    The default is derived from the shape of ``value`` to keep merge semantics:
    - dict -> {}
    - list -> []
    - other -> None

    Args:
        result (Any): Current object being merged into.
        key (Any): Key to fetch from ``result``.
        value (Any): Incoming value used to infer a default.

    Returns:
        Any: The existing value for the key or a type-appropriate default.
    """
    default = None
    if isinstance(value, dict):
        default = {}
    elif isinstance(value, list):
        default = []
    return result.get(key, default)


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
    identifier = _get_lookup_key(key_name)
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

    default_task = _get_defaults(merged.get("tasks", []), "task_key", default_key)
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
        old_value = _get_old_value(result, key, value)
        if isinstance(value, dict) and isinstance(old_value, dict):
            result[key] = _override_workflow(
                workflow=old_value,
                workflow_overrides=value,
                task_overrides={},
                default_key=default_key,
            )
        elif isinstance(value, list) and isinstance(old_value, list):
            if isinstance(value[0], dict) and key not in IGNORED_OVERRIDE_KEYS:
                lookup_key = _get_lookup_key(key)
                if lookup_key:
                    result[key] = _update_list_by_key(
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
