#region Importing libraries

import pandas as pd
import requests as re
from datetime import datetime, timedelta
import json

from prefect import task, Flow, Parameter
from prefect.schedules import CronSchedule

#endregion

#region Extracting data
@task( max_retries=5, retry_delay=timedelta(seconds=20))
def extract(url: str) -> dict:
    response = re.get(url)
    if not response:
        raise Exception('Nenhum dado encontrado')
    return json.loads(response.content)

#endregion

#region Transforming data
@task
def transform(data: list) -> pd.DataFrame:
    data_transformed = []
    for record in data:
        data_transformed.append({
            'concessionaria':record['concessionaria'],
            'ano': record['ano'],
            'valor': record['valor']
        })

        df = pd.DataFrame(data_transformed)
        df['concessionaria'] = df['concessionaria'].astype(str)
        df['ano'] = df['ano'].astype(int)

    return df

#endregion

#region Loading data
@task
def load(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path_or_buf=path, index=False)

#endregion

#region Schedule

#scheduler = IntervalSchedule(interval=timedelta(seconds=10)) #Execução agendada a cada 10s
scheduler = CronSchedule(cron='* * * * *') #Execução agendada a cada 1min

#endregion

#region Flow

def prefect_flow():
    with Flow(name='simple_etl_pipeline', schedule=scheduler) as flow:
        url_principal = Parameter('url', required=True)

        data = extract(url_principal)
        df_transformed = transform(data['investimentos'])
        load(df=df_transformed, path=f'loads/fornecedores_{int(datetime.now().timestamp())}.csv')
    return flow

#endregion

if __name__ == '__main__':
    flow = prefect_flow()
    flow.run(parameters={'url': 'https://dados.antt.gov.br/dataset/a133da64-1e03-4832-909d-e1eb835eec2e/resource/d46bbb49-95f3-44b0-bb9a-0ce095746bbe/download/investimentos.json'})
