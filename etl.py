import json
from datetime import datetime, timedelta
from pprint import pprint

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook


@dag(schedule_interval="@daily", start_date=datetime(2021, 11, 1), catchup=False)
def nombre_velo_nantes():
    @task()
    def fetch_data_from_nantes_api(data_interval_start: datetime = None, **kwargs) -> str:
        pprint(kwargs)
        if data_interval_start is None:
            raise Exception("Start date not provided.")

        import requests as r

        yesterday = data_interval_start - timedelta(days=1)
        queryParams = {
            "where": "jour = date'{}'".format(yesterday.strftime("%Y/%m/%d")),
            "order_by": "jour desc",
            "limit": 100,
            "timezone": "UTC"
        }
        j = r.get(
            url="https://data.nantesmetropole.fr/api/v2/catalog/datasets/244400404_comptages-velo-nantes-metropole/records",
            params=queryParams
        ).json()

        if j["total_count"] == 0:
            raise Exception("No results found")

        if j["total_count"] > 100:
            pprint("Must loop over results")

        with open("/opt/airflow/dags/raw.json", mode="w") as raw:
            raw.write(json.dumps(j["records"]))
            raw.close()

        return "/opt/airflow/dags/raw.json"

    @task()
    def reshape(path: str):
        import pandas as pd
        df = pd.read_json(path)
        transformed_df = None

        for index, value in df["record"].items():
            if transformed_df is None:
                transformed_df = pd.DataFrame(data=[value["fields"]])
                continue
            transformed_df = pd.concat([transformed_df, pd.DataFrame(data=[value["fields"]])], ignore_index=True)

        with open("/opt/airflow/dags/reshape.json", mode="w") as reshaped_file:
            transformed_df.to_json(reshaped_file)

        return "/opt/airflow/dags/reshape.json"

    @task()
    def load_to_mongo(path: str):
        m = MongoHook()
        df = pd.read_json(path)
        for k, row in df.iterrows():
            doc = {
                "boucle_libelle": row["boucle_libelle"]
            }
            m.insert_one("velo", doc, "velo")
            pprint(row)

        return None

    raw_file_path = fetch_data_from_nantes_api()
    reshape_file_path = reshape(raw_file_path)
    load_to_mongo(reshape_file_path)


pipeline_velo_nantes = nombre_velo_nantes()
