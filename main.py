#region Importing libraries

import pandas as pd
import requests as re
from datetime import datetime, timedelta
import json

#Importando as bibliotecas prefect
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

#endregion

#region Extracting data

@task( name='extract', max_retries=5, retry_delay=timedelta(seconds=30))
def extract(url: str) -> dict:
    response = re.get(url)
    if not response:
        raise Exception('Nenhum dado encontrado')
    content = json.loads(response.content)
    return content['investimentos']

#endregion

#region Transforming data
@task(name='transform',  max_retries=5, retry_delay=timedelta(seconds=30))
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

@task(name='load',  max_retries=5, retry_delay=timedelta(seconds=30))
def load(df: pd.DataFrame, path: str):
    df.to_csv(path_or_buf=path, index=False)

#endregion

#region Schedule

scheduler = IntervalSchedule(interval=timedelta(seconds=300)) #Agendamento configurado para 60seg

#endregion

#region Flow

def flow():
    with Flow(name='etl_pipeline', schedule=scheduler) as flow:
        url_principal = Parameter('url', required=True)

        data = extract(url_principal)
        df_transformed = transform(data)
        load(df=df_transformed, path=f'loads/fornecedores_{int(datetime.now().timestamp())}.csv')
    return flow

#endregion

if __name__ == '__main__':
    url = 'https://dados.antt.gov.br/dataset/a133da64-1e03-4832-909d-e1eb835eec2e/resource/d46bbb49-95f3-44b0-bb9a-0ce095746bbe/download/investimentos.json'
    flow = flow()
    flow.run(parameters={'url': url})

