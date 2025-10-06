from __future__ import annotations

from kedro_databricks.cli.bundle.override_resources import override_resources


def _mk_resources(names: list[str]) -> dict:
    """Make a resources dictionary with the given names."""
    return {
        "resources": {
            "jobs": {
                n: {
                    "name": n,
                    "tags": {},
                    "tasks": [
                        {"task_key": "node0"},
                        {"task_key": "ns_0_node_0_1"},
                    ],
                }
                for n in names
            }
        }
    }


def test_workflow_regex_applied_and_precedence():
    """Test that regex overrides are applied and precedence is correct."""
    resources = _mk_resources(
        [
            "country_1",
            "country_1.subpipe1",
            "country_2.subpipe2",
        ]
    )

    overrides = {
        # Defaults apply first
        "default": {"tags": {"base": "yes", "country": "unknown"}},
        # Regex for all country_1.*
        "re:^country_1(\\..+)?$": {"tags": {"country": "country_1", "x": "1"}},
        # Literal exact override wins over regex
        "country_1.subpipe1": {"tags": {"x": "literal"}},
    }

    result = override_resources(resources, overrides, default_key="default")

    jobs = result["resources"]["jobs"]

    # country_1 inherits regex override
    assert jobs["country_1"]["tags"]["country"] == "country_1"
    assert jobs["country_1"]["tags"]["x"] == "1"
    assert jobs["country_1"]["tags"]["base"] == "yes"

    # literal beats regex
    assert jobs["country_1.subpipe1"]["tags"]["country"] == "country_1"
    assert jobs["country_1.subpipe1"]["tags"]["x"] == "literal"
    assert jobs["country_1.subpipe1"]["tags"]["base"] == "yes"

    # non-matching keeps default
    assert jobs["country_2.subpipe2"]["tags"]["country"] == "unknown"
    assert jobs["country_2.subpipe2"]["tags"]["base"] == "yes"


def test_task_regex_applied_with_default():
    """Test that task regex overrides are applied and default is used."""
    resources = _mk_resources(["wf"])
    overrides = {
        "default": {
            "tasks": [
                {"task_key": "default", "job_cluster_key": "default"},
                {"task_key": "re:^ns_.*", "job_cluster_key": "hp"},
            ]
        }
    }

    result = override_resources(resources, overrides, default_key="default")

    tasks = result["resources"]["jobs"]["wf"]["tasks"]
    by_key = {t["task_key"]: t for t in tasks}
    assert by_key["ns_0_node_0_1"]["job_cluster_key"] == "hp"
    assert by_key["node0"]["job_cluster_key"] == "default"


def test_task_multiple_regex_last_wins():
    """Test that task regex overrides are applied and last wins."""
    resources = _mk_resources(["wf"])
    overrides = {
        "default": {
            "tasks": [
                {"task_key": "re:^ns_.*", "job_cluster_key": "hp1"},
                {"task_key": "re:^ns_0_.*", "job_cluster_key": "hp2"},
            ]
        }
    }

    result = override_resources(resources, overrides, default_key="default")
    tasks = result["resources"]["jobs"]["wf"]["tasks"]
    by_key = {t["task_key"]: t for t in tasks}
    # both regex match, last wins
    assert by_key["ns_0_node_0_1"]["job_cluster_key"] == "hp2"
