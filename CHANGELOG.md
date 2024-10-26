## 0.6.1 (2024-10-26)

### Refactor

- add utility to check if user has databricks CLI
- move init templates to separate folder
- make init a class to reduce duplicated code

## 0.6.0 (2024-10-20)

### Feat

- **bundle**: allow to bundle a single pipeline

## 0.5.0 (2024-10-17)

### Feat

- **bundle**: sort tasks for easy change review

## 0.4.0 (2024-10-05)

### Feat

- **deploy**: add conf option to the bundle and the deploy commands

### Fix

- adds support for another conf path#

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
