import os

# Cosmos caches the rendered dbt graph in Airflow Variables, which needs a metadata database.
# The test container has none: render without cache.
os.environ.setdefault("AIRFLOW__COSMOS__ENABLE_CACHE", "False")
