## 0.6.7 (2024-12-28)

### Fix

- allow usage of environment variables to configure workspace client
- command should return CompletedProcess
- tarfile fitler was implemented in python 3.12
- completion logging was unreachable

### Refactor

- command run

## 0.6.6 (2024-11-28)

### Fix

- it's var not vars
- add support for vars

### Refactor

- improve run command

## 0.6.5 (2024-11-24)

### Fix

- log deployed jobs to user

### Refactor

- make _gather_jobs easier to test
- add types to deploy methods
- simplify log_deployed_resources
- return jobs after logging
- handle errors in init
- fix args in make_workflow_name
- move make_workflow_name to utils

## 0.6.4 (2024-11-06)

### Refactor

- simplify generate_resources
- make deploy a controller

## 0.6.3 (2024-11-06)

### Refactor

- **bundle**: sort task dependencies

## 0.6.2 (2024-11-02)

### Fix

- make overwrite arg a flag
- pass init args to bundle controller in deploy

### Refactor

- make bundle a controller

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
