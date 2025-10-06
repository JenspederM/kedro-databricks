This example demonstrates regex-based workflow and task overrides, including how rules are merged and how precedence works.

**Rule precedence and ordering**

- Exact-name overrides > regex overrides > defaults.
- If multiple regex rules match, the last one wins (ordered application).
- For list-of-dicts (e.g., `job_clusters`, `tasks`), items merge by identifier:
    - `job_clusters` by `job_cluster_key`
    - `tasks` by `task_key`
- Non-identifier lists are replaced by the newer value; dicts are deep-merged.

**What this example shows**

- Workflow regex: `re:^country_1(\..+)?$` applies tags and a job cluster to `country_1`, `country_1.subpipe1`, and `country_1.subpipe2`.
- Task regex: default `tasks` include `task_key: "re:^ns_.*"` so all `ns_*` tasks (e.g., `ns_1_node_1`, `ns_1_node_2`, `ns_2_node_1`, `ns_2_node_2`) run on the `high-performance` cluster unless an exact task override exists.
- Exact-name precedence:
    - `country_1.subpipe1` sets `tags.x: literal`, overriding the regex tag (`x: 1`).
    - `country_1.subpipe1` sets task `ns_1_node_1` to run on `ultra`, overriding the task regex.
    This demonstrates exact > regex > default at both workflow and task level.
