import json

from google.cloud import bigquery
from google.cloud.bigquery import Dataset

client = bigquery.Client(project="afb-lakehouse-poc")

pd_config_table = client.query("""SELECT * FROM control_dev.dl_conf_batch_exec_dev""").to_dataframe()

for index, row in pd_config_table.iterrows():
    d = json.loads()
    v = {"DAG_FREQUENCY": row["DAG_FREQUENCY"],
         "FILES_PER_DAY": 1 if row["RAW_FILE_NAME"] != "Valores" else 3}
    d.update(v)
    pd_config_table.at[index, "JSON_EXTRA"] = d

pd_config_table["DAG_FREQUENCY"] = "None"

table_ref = Dataset("afb-lakehouse-poc.control_dev").table("dl_conf_batch_exec_dev")

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

job = client.load_table_from_dataframe(
    pd_config_table, table_ref, job_config=job_config
)
job.result()
