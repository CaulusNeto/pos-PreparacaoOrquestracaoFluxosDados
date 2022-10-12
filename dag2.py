import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from datetime import datetime


default_args = {
    'owner': "Carlos de Souza Nogueira Neto",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 11)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def dag_titanic_tabela_unica():

    inicio = DummyOperator(task_id="Inicio")

    @task
    def processa_medias():
        arquivo = '/tmp/tabela_unica.csv'
        t_resultados = "/tmp/resultados.csv"
        df = pd.read_csv(arquivo, sep=';')
        media = df.agg({'Passengers': 'mean', 'Fare': 'mean', 'SibSp_Parch': 'mean'}).reset_index(
            ).rename(columns={'index': 'Indicadores', 0: 'Valor'})
        print(media)
        media.to_csv(t_resultados, index=False, sep=';')
        return t_resultados    

    executa_medias = processa_medias()   

    fim = DummyOperator(task_id="fim")  

    inicio >> executa_medias >> fim  

executa = dag_titanic_tabela_unica() 