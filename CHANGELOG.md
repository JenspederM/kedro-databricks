## 0.3.0 (2024-09-14)

### Feat

- Make changes necessary to utilize run in kedro>=0.19.8

### Fix

- entry_point is separated by hyphens
- project must be build to be listed
- log target path when uploading config
- use __main__ when kedro > 0.19.8

## 0.2.1 (2024-09-02)

### Fix

- project must be build to be listed

## 0.2.0 (2024-09-02)

### Feat

- **env**: add --env parameter to the python_wheel_task parameters
- **deploy**: add target option to the deploy command

### Refactor

- **deploy**: rename env argument into target to better with its usage
