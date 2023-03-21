#import pymssql
import psycopg2
import json

with open("dados_con.json") as conexao_json:
    dados_conexao = json.load(conexao_json)

#print('1')

conexao = psycopg2.connect(host=dados_conexao['server'],user=dados_conexao['user'],password=dados_conexao['password'],database=dados_conexao['database'])

#print('2')

cursor = conexao.cursor()
#print('3')

cursor.execute('SELECT COUNT (DISTINCT AREA_PICKING) AS TOT_AREA_PICKING FROM ENDERECOS')
#print('4')

row = cursor.fetchone()
while row:
    print(row[0])
    row = cursor.fetchone()

conexao.close()