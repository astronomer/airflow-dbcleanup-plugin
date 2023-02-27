# Airflow Plugin [Document In Progress]#

This is an [Airflow plugin](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html) that queries the Airflow metadata database and performs task cleanup at an HTTP endpoint.

## How do I test this repo

You must have the astro CLI installed.

1. Clone repo
2. Run `astro dev init`
3. Create a directory and copy some files into the `plugins/` directory of your Astro project:
   ```bash
   mkdir plugins/astronomer_dbcleanup_plugin
   cp analytics_plugin.py plugins/astronomer_dbcleanup_plugin/
   cp -a templates plugins/astronomer_dbcleanup_plugin/
   ```
4. Run `astro dev start`
   ```
