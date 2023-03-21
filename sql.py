# criando uma classe para conexao, query e procedures
# import pymssql
import json
import psycopg2


with open('dados_conexao.json') as conexao_json:
    dados_conexao = json.load(conexao_json)

def retornar_conexao_sql(server,database,user,password):
    #conexao = pymssql.connect(host=server,user=user,password=password,database=database)
    conn = psycopg2.connect(host=server,database=database,user=user,password=password)
    return conn


#@st.experimental_memo(ttl=600)
def executar_procedure(query,params,conn):
    cursor = conn.cursor()
    cursor.callproc(query,params)
    conn.commit()