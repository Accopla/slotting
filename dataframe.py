import sql
import psycopg2
import pandas as pd
import Ferramenta_de_Balanceamento as fb

# Executar a conexao com o servidor SQLSERVER
conn = psycopg2.connect(
    host = "200.98.168.230",
    user = "glad",
    password = "Gl@d2023",
    dbname = "glad")

sql_BI1 = "select * from vw_bi_unidades_data_area_picking" ##"select * from bi_unidades_data_area_picking"
df_BI1 = pd.read_sql(sql_BI1,conn)

sql_BI2 = "select * from vw_bi_caixas_iniciam_estacao__atual where area_picking_mae = 'principal'"
df_BI2 = pd.read_sql(sql_BI2,conn)

sql_BI3 = "select * from vw_bi_caixas_iniciam_estacao_ideal where area_picking_mae = 'principal'"
df_BI3 = pd.read_sql(sql_BI3,conn)

sql_BI4 = "select * from base_balanceamento_atual"
df_BI4 = pd.read_sql(sql_BI4,conn)

sql_BI5 = "select * from base_balanceamento_ideal"
df_BI5 = pd.read_sql_query(sql_BI5,conn)
