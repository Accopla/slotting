# criando uma classe para conexao, query e procedures
import pymssql
import json

with open("dados_conexao.json") as conexao_json:
    dados_conexao = json.load(conexao_json)

class sql:
    def __init__(self):
        pass

def retornar_conexao_sql():
    
    conexao = pymssql.connect(host=dados_conexao['server'],port=dados_conexao['port'],user=dados_conexao['user'],password=dados_conexao['password'],database=dados_conexao['database'])
    return conexao

conn = retornar_conexao_sql()

#@st.experimental_memo(ttl=600)
def executar_procedure(query,params):
    cursor = conn.cursor()
    cursor.callproc(query,params)
    conn.commit()