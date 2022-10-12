import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Carlos de Souza Nogueira Neto",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 11)
}


@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def dag_titatic():

    inicio = DummyOperator(task_id="Inicio")

    @task
    def salva_arquivo_titanic():
        arquivo = "/tmp/titanic.csv"
        sf= pd.read_csv(URL, sep=';')
        sf.to_csv(arquivo, index=False, sep=";")
        return arquivo

    @task
    def indicador_passageiros_sexo_classe(arquivo):
        #Quantidade de passageiros por sexo e classe (produzir e escrever)
        t_passageiros_sexo_classe = "/tmp/passageiros_sexo_classe.csv"
        df = pd.read_csv(arquivo, sep=";")
        qpxc = df.groupby(['Sex', 'Pclass']).agg(
            {"PassengerId": "count"}).reset_index().rename(columns={'PassengerId': 'Passengers'})
        qpxc.to_csv(t_passageiros_sexo_classe, index=False, sep=";")
        print(qpxc)
        return t_passageiros_sexo_classe

    @task
    def indicador_preço_tarifa_sexo_classe(arquivo):
        #Preço médio da tarifa pago por sexo e classe (produzir e escrever)
        t_preço_tarifa_sexo_classe = "/tmp/preço_tarifa_sexo_classe.csv"
        df = pd.read_csv(arquivo, sep = ';')
        ptxc = df.groupby(['Sex', 'Pclass']).agg(
            {'Fare': 'mean'}).reset_index()
        print(ptxc) 
        ptxc.to_csv(t_preço_tarifa_sexo_classe, sep= ';', index= False)
        return t_preço_tarifa_sexo_classe

    @task
    def indicador_quantidade_total_sexo_classe(arquivo):
        #Quantidade total de SibSp + Parch (tudo junto) por sexo e classe (produzir e escrever)
        t_qtde_total_sexo_classe = "/tmp/qtde_total_sexo_classe.csv"
        df = pd.read_csv(arquivo, sep= ';')
        df['SibSp_Parch'] = df['SibSp'] + df['Parch']
        qtxc = df.groupby(['Sex', 'Pclass']).agg(
            {'SibSp_Parch': 'sum'}).reset_index()
        print(qtxc)   
        qtxc.to_csv(t_qtde_total_sexo_classe, index= False, sep=';') 
        return t_qtde_total_sexo_classe    

    @task
    def junta_indicadores(passageiros_sexo_classe, preço_tarifa_sexo_classe, qtde_total_sexo_classe):
        # Juntar todos os indicadores criados em um único dataset (produzir o dataset e escrever) /tmp/tabela_unica.csv
        t_tabela_unica = "/tmp/tabela_unica.csv"
        df_passageiros = pd.read_csv(passageiros_sexo_classe, sep=';')
        df_preço = pd.read_csv(preço_tarifa_sexo_classe, sep=';')
        df_qtde = pd.read_csv(qtde_total_sexo_classe, sep=';')
        ji = df_passageiros.merge(df_preço, how='inner', on=['Sex', 'Pclass'])
        ji = ji.merge(df_qtde, how='inner', on=['Sex', 'Pclass'])
        print(ji)
        ji.to_csv(t_tabela_unica, index=False, sep=';')
        return t_tabela_unica 

    arquivo = salva_arquivo_titanic()
    processa_passageiros_sexo_classe = indicador_passageiros_sexo_classe(arquivo)
    processa_preço_tarifa_sexo_classe = indicador_preço_tarifa_sexo_classe(arquivo)
    processa_indicador_quantidade_total_sexo_classe = indicador_quantidade_total_sexo_classe(arquivo)
    processa_juntar_indicadores = junta_indicadores(processa_passageiros_sexo_classe, processa_preço_tarifa_sexo_classe, processa_indicador_quantidade_total_sexo_classe)

    chamar_dag2 = TriggerDagRunOperator(
        task_id="dag_titanic_tabela_unica",
        trigger_dag_id="dag_titanic_tabela_unica"
    )

    fim = DummyOperator(task_id="fim")

    inicio >> arquivo >> [processa_passageiros_sexo_classe, processa_preço_tarifa_sexo_classe, processa_indicador_quantidade_total_sexo_classe] >> processa_juntar_indicadores >> chamar_dag2 >> fim

executa = dag_titatic()