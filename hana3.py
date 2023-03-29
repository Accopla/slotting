from sqlalchemy import create_engine
from hana_ml import dataframe
import pandas as pdf
import sql
import psycopg2

# Conexão com SAP
conn = dataframe.ConnectionContext(address='10.41.15.47',
                                   port=30515,
                                   user='SQL_POWER_BI',
                                   password='wed213EWQewd@#53da')

# Conexão com Postgres
engine = create_engine('postgresql://glad@10.41.12.34:5432/glad')

# Atualização de Filiais
hn_centro =  dataframe.DataFrame(conn, 'SELECT distinct "Centro" FROM _SYS_BIC."balanceamento/CVD_ZTBLMM1032_Material"')
df_centro = hn_centro.collect()
df_centro.to_sql('erp_centro', con=engine, if_exists='replace')

# Atualização de Materiais
#hn_material = conn.table('balanceamento/CVD_ZTBLMM1032_Material', schema='_SYS_BIC').filter("Centro"='PI11')
Centro = 'MG11'
hn_material =  dataframe.DataFrame(conn, 'SELECT * FROM _SYS_BIC."balanceamento/CVD_ZTBLMM1032_Material" where "Centro"= \'' + Centro +'\'')
df_material = hn_material.head().collect()
df_material.to_sql('erp_material', con=engine, if_exists='replace')

# Atualização de Endereços
#hn_endereco = conn.table('balanceamento/CVD_ZTBLMM1032_Endereco', schema='_SYS_BIC')
hn_endereco =  dataframe.DataFrame(conn, 'SELECT * FROM _SYS_BIC."balanceamento/CVD_ZTBLMM1032_Endereco" where WERKS= \'' + Centro +'\'')
df_endereco = hn_endereco.head().collect()
df_endereco.to_sql('erp_endereco', con=engine, if_exists='replace')

# Atualização de Saídas
#hn_saidas = conn.table('balanceamento/CVD_ZTBLMM1032_Balanceamento', schema='_SYS_BIC')
hn_saidas =  dataframe.DataFrame(conn, 'SELECT * FROM _SYS_BIC."balanceamento/CVD_ZTBLMM1032_Balanceamento" where WERKS= \'' + Centro +'\' and ERDAT BETWEEN \'20220301\' AND \'20220315\' ')
df_saidas = hn_saidas.head().collect()
df_saidas.to_sql('erp_saidas', con=engine, if_exists='replace')

conn.close()

