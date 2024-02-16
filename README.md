# astronomer-airflow-dbcleanup-plugin
[![Build Status](https://circleci.com/gh/astronomer/airflow-dbcleanup-plugin.svg?style=shield)](https://circleci.com/gh/astronomer/airflow-dbcleanup-plugin)
[![GitHub release](https://img.shields.io/github/release/astronomer/airflow-dbcleanup-plugin)](https://github.com/astronomer/airflow-dbcleanup-plugin/releases/latest)
[![Contributors](https://img.shields.io/github/contributors/astronomer/airflow-dbcleanup-plugin)](https://github.com/astronomer/airflow-dbcleanup-plugin)
[![Commit activity](https://img.shields.io/github/commit-activity/m/astronomer/airflow-dbcleanup-plugin)](https://github.com/astronomer/airflow-dbcleanup-plugin)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/astronomer/airflow-dbcleanup-plugin/blob/main/.pre-commit-config.yaml)

An Apache Airflow plugin that exposes an rest endpoint to perform cleanup on airflow tables.

## How do I test this repo

You must have the astro CLI installed.

1. Clone the repo
2. Run `astro dev init`
3. Create a directory and copy some files into the `plugins/` directory of your Astro project:
   ```bash
   mkdir plugins/astronomer_dbcleanup_plugin
   cp *.py plugins/astronomer_dbcleanup_plugin/
   ```
4. Run `astro dev start`

## Publishing new packages to GitHub

1. Update the `__version__` variable in `dbcleanup_plugin.py`
2. Run `python -m build`
3. Upload the generated Wheel (`.whl`) and `.tar.gz` files generated in `dist/` to GitHub as a release
