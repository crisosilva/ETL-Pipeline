<h1 align='center'> ETL-Pipeline </h1>
Projeto de um pipeline de ETL usando a biblioteca prefect que realizará
o gerenciamento do fluxo de trabalho podendo ser usado tanto localmente
quanto na nuvem, podendo assim substituir o Airflow.

<h2>:clipboard:Pré requisitos</h2>
Para utilizar o projeto será necessário instalar o Python 3 caso não tenha
instalado além das bibliotecas utilizadas no projeto

<h4>Instalação do Python </h4>
Se o seu SO for o linux o Python já estará instalado, caso esteja utilizando
o SO Windows o Python pode ser baixado e instalado em:

<href>https://www.python.org/downloads/windows/</href>

Acesse, baixe e siga as orientações de instalação.

<h4>Instalação das bibliotecas</h4>
Para instalar as bibliotecas utilize os comandos abaixo:

'''
pip install prefect
pip install pandas
'''

<h2>🛠️ Construído com</h2>

O pipeline de ETL foi construido utilizando a linguagem de programação Python
a IDE PyCharm e as bilbiotecas <b>pandas, requests, datetime.datetime, timedelta, 
json, prefect.task, prefect.Flow, prefect.Parameter, prefect.schedules.IntervalSchedule
prefect.schedules.CronSchedule</b>
O dataset trata-se de um arquivo Json baixado do site <href>'https://dados.antt.gov.br/dataset/a133da64-1e03-4832-909d-e1eb835eec2e/resource/d46bbb49-95f3-44b0-bb9a-0ce095746bbe/download/investimentos.json'</href>

<h2>✒ Autores </h2>

Projeto criado por Cristiano Oliveira

<h2>📌 Versão </h2>

Version: 1.0.0

<h2>Referências</h2>

<href>https://www.prefect.io/</href>
