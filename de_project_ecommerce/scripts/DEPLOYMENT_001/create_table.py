import os
import time
from pyspark.sql import Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


# --------------------------------------------
# MAGIC %md ### Environment Configs
# --------------------------------------------
def load_env():
    env     = os.environ.get("ENVIRONMENT")
    catalog = os.environ.get("CATALOG")
    schema  = os.environ.get("SCHEMA")

    print("===== ENVIRONMENT SETUP =====")
    print("ENV       :", env)
    print("CATALOG   :", catalog)
    print("SCHEMA    :", schema)

    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")

    return env, catalog, schema


# --------------------------------------------
# MAGIC %md ### Submit Notebook
# --------------------------------------------
def submit_run(wspc, cluster_id, notebook_path, file_name, base_parameters):
    run = wspc.jobs.submit(
        run_name = f"execute-init-{file_name}-{time.time_ns()}",
        tasks = [
            jobs.SubmitTask(
                existing_cluster_id = cluster_id,
                notebook_task = jobs.NotebookTask(
                    notebook_path   = notebook_path + file_name,
                    base_parameters = base_parameters
                ),
                task_key = f"execute-init-{file_name}-{time.time_ns()}"
            )
        ]
    )

    run_id = run.run_id
    run_info = wspc.jobs.get_run(run_id).as_dict()

    return run_info


# --------------------------------------------
# MAGIC %md ### Run SQL Files
# --------------------------------------------
def run_sql_files(wspc, cluster_id, sql_path, table_list, catalog, schema):
    run_id_dict     = {}
    runner_notebook = "/Workspace/Shared/sql_runner_notebook"

    for table_name in table_list:

        sql_file = os.path.join(sql_path, f"{table_name}.sql")

        if not os.path.exists(sql_file):
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        with open(sql_file, "r") as f:
            sql_text = f.read()

        print(f"\n=== SUBMIT RUN FOR TABLE: {table_name} ===")

        run_info = submit_run(
            wspc,
            cluster_id,
            notebook_path="",
            file_name=runner_notebook,
            base_parameters={
                "sql_text": sql_text,
                "table_name": table_name,
                "catalog": catalog,
                "schema": schema
            }
        )

        run_id_dict[table_name] = run_info["run_id"]

    return run_id_dict


# --------------------------------------------
# MAGIC %md ### Collect Result (STATUS + run_url)
# --------------------------------------------
def collect_run_results(wspc, run_id_dict):
    run_results = []

    print("\n===== COLLECTING RUN RESULTS =====")
    for table_name, run_id in run_id_dict.items():

        run_info = wspc.jobs.get_run(run_id).as_dict()

        status = run_info["state"]["result_state"]
        run_url = run_info["run_page_url"]

        print(f"{table_name}: {status} ({run_url})")

        run_results.append(
            Row(
                table_init_name=table_name,
                run_id=run_id,
                status=status,
                run_url=run_url
            )
        )

    return run_results


# --------------------------------------------
# MAGIC %md ### MAIN FUNCTION
# --------------------------------------------
def main():
    print("===== START TABLE CREATION JOB =====")

    env, catalog, schema = load_env()

    # Retrieve tokens / host
    dbutils_local = locals().get("dbutils")
    api_token = dbutils_local.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    workspace_url = dbutils_local.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

    wspc = WorkspaceClient(host=workspace_url, token=api_token)

    # Current cluster_id
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

    # Tables to create
    table_list = [
        "bronze_ga4_events"
    ]

    table_list = [tbl.lower() for tbl in table_list]

    sql_path = "/Workspace/de_project_ecommerce/scripts/ddl/table_init/"

    run_id_dict = run_sql_files(
        wspc=wspc,
        cluster_id=cluster_id,
        sql_path=sql_path,
        table_list=table_list,
        catalog=catalog,
        schema=schema
    )

    # Collect results
    results = collect_run_results(wspc, run_id_dict)
    result_df = spark.createDataFrame(results)
    result_df.display()

    dbutils.notebook.exit("SUCCESS")


# Databricks entrypoint
if __name__ == "__main__":
    main()
