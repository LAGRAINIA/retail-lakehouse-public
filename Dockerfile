FROM astrocrpublic.azurecr.io/runtime:3.3-9

# dbt lives in its own virtualenv: dbt and Airflow pin conflicting libraries.
# Cosmos and the BashOperators call this executable (DBT_EXECUTABLE).
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir "dbt-databricks==1.12.7"

ENV DBT_EXECUTABLE=/usr/local/airflow/dbt_venv/bin/dbt \
    DBT_TARGET=prod
