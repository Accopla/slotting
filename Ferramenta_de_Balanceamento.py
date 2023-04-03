import pickle
from pathlib import Path
import streamlit_authenticator as stauth
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sql
from PIL import Image
from datetime import date
import json
import generate_keys as gk
import yaml
import jwt
from datetime import timedelta
from hana_ml import dataframe
from sqlalchemy import create_engine
import psycopg2

st.set_page_config(page_title="Ferramenta_de_Balanceamento")
#CARREGAR HASHED PASSWORDS
# passwords = ["123", "123"]  123456
# hashed_passwords = stauth.Hasher(passwords).generate()

with open('config.yaml') as file:
        config = yaml.load(file, Loader=yaml.SafeLoader)
        
authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )
    
name, authenticator_status, username = authenticator.login("Login", "main")

if authenticator_status == False:
        st.error("Usuário/senha incorretos")
if authenticator_status == None:
        st.warning("Por favor entre com seu usuário e senha")
if authenticator_status:

    st.title("PAGINA INICIAL")
    
    #ABRINDO O ARQUIVO JSON
    with open("dados_conexao.json") as conexao_json:
        dados_conexao = json.load(conexao_json)

    #FORMULÁRIO PARA APONTAR PARA O IP DO SERVIDOR
    with st.sidebar.expander("CONFIGURAR CONEXÕES"):
        form_ip = st.form("IP do Servidor:")   
    with form_ip:
        text_ip = st.text_input("IP do Servidor:",dados_conexao['server'])
        #text_port = st.text_input("Porta:",dados_conexao['port'])
        text_database = st.text_input("Banco de Dados:",dados_conexao['database'])
        text_user = st.text_input("Usuário:",dados_conexao['user'])
        text_password = st.text_input("Senha:",dados_conexao['password'], type="password")
        ip_button = st.form_submit_button("Atualizar")
    if ip_button:
        st.write(text_ip)

    # Executar a conexao com o servidor SQLSERVER
    conn = sql.retornar_conexao_sql(text_ip,text_database,text_user,text_password)

    # class Dataframe:
    #     def __init__(self, df):
    #         self.df_BI1 = df

    # Verificar se tem Aframe
    sql0 = "select count (distinct area_picking) as tot_area_picking from enderecos where area_picking = 'AFRAME'"
    df0 = pd.read_sql(sql0,conn)

    if df0["tot_area_picking"].values[0] == 1:
            # Formulário para atualizar os parâmetros do AFRAME
        with open("parametros_aframe.json") as conexao_parametros_aframe:
            parametros_aframe = json.load(conexao_parametros_aframe)
        with st.sidebar.expander("PARÂMETROS DO AFRAME:"):
            form = st.form("Parametros_Aframe")
            col1, col2 = form.columns(2)
        with col1:
            txt_custo_funcionario = st.text_input("Custo Funcionário:",parametros_aframe['custo_funcionario'])
            txt_produtividade_reposicao_canal = st.text_input("Prod. Rep. Canal:",parametros_aframe['produtividade_acessar_canal'])
            txt_total_canais_maior = st.text_input("Total Canais Maior:",parametros_aframe['quantidade_canais'])
            txt_altura_canal_maior = st.text_input("Alt Canal Maior:",parametros_aframe['tamanho_canal'])
            txt_produtividade_backuploop_canal_maior = st.text_input("Prod. Canal Maior:",parametros_aframe['produtividade_backuploop'])
        with col2:
            txt_produtividade_manual = st.text_input("Prod. Manual:",parametros_aframe['produtividade_manual'])
            txt_tempo_enchimento_canal = st.text_input("Ench. Canal:",parametros_aframe['tempo_encher_canal'])
            txt_total_canais_menor = st.text_input("Canais Menor:",parametros_aframe['quantidade_canais_2'])
            txt_altura_canal_menor = st.text_input("Alt Canal Menor:",parametros_aframe['tamanho_canal_2'])
            txt_produtividade_backuploop_canal_menor = st.text_input("Prod. Canal Menor:",parametros_aframe['produtividade_backuploop_2'])
            submit_button = form.form_submit_button("Submit")

        if submit_button:
            # Executar as procedure para atualizar as bases
            sp_processar_sugestao_balanceamento_ideal = 'processar_sugestao_balanceamento_ideal'
            param_procedure = (txt_custo_funcionario, txt_produtividade_manual, txt_produtividade_reposicao_canal,txt_tempo_enchimento_canal,txt_total_canais_maior,
                                txt_altura_canal_maior,txt_produtividade_backuploop_canal_maior,txt_total_canais_menor,txt_altura_canal_menor,txt_produtividade_backuploop_canal_menor,)
            sql.executar_procedure(sp_processar_sugestao_balanceamento_ideal,param_procedure,conn)

    # Formulário para atualizar a base de Dados ( Filial e Período)
    with st.sidebar.expander("PARÂMETROS ATUALIZAÇÃO DA BASE:"):
        form2 = st.form("Parametros_Base")
    with form2:
        # Verificar se tem Aframe
        #sql1 = "SELECT \"Centro\" FROM public.erp_centro order by \"Centro\""
        #df1 = pd.read_sql(sql1,conn)
        filial = 'MG11' #st.selectbox("Filial:",df1["Centro"].values)
        d = timedelta(days=7)
        data_ini = st.date_input("Data Inicial:",date.today() - d)
        data_fin = st.date_input("Data Final:",date.today())
        submit_button2 = st.form_submit_button("Submit")
        if submit_button2:
            #st.write("Os dados de filial e o periodo serão atualizados..")
            #sql_BI1 = "SELECT * FROM VW_BI_UNIDADES_DATA_AREA_PICKING"
            #Dataframe.df_BI1 = pd.read_sql(sql_BI1,conn)
            #Dataframe(pd.read_sql(sql_BI1,conn))

            st.write("Conectando ao ERP")
            # Conexão com SAP
            conh = dataframe.ConnectionContext(address='10.41.15.47',
                                               port=30515,
                                               user='SQL_POWER_BI',
                                               password='wed213EWQewd@#53da')
            st.write("Conectando ao PG")
            # Conexão com Postgres
            engine = create_engine('postgresql://glad@10.41.12.34:5432/glad')

            st.write("Importando filiais")
            # Atualização de Filiais
            hn_centro =  dataframe.DataFrame(conh, 'SELECT distinct "Centro" FROM _SYS_BIC."balanceamento/CVD_ZTBLMM1032_Material"')
            df_centro = hn_centro.collect()
            df_centro.to_sql('erp_centro', con=engine, if_exists='replace')

            st.write("Importando Materiais")
            # Atualização de Materiais
            #hn_material =  dataframe.DataFrame(conh, 'SELECT * FROM _SYS_BIC."balanceamento/CVD_ZTBLMM1032_Material" where "Centro"= \'' + filial +'\'')
            #df_material = hn_material.collect()
            #df_material.to_sql('erp_material', con=engine, if_exists='replace')

            st.write("Importando Endereços")
            ## Atualização de Endereços
            #hn_endereco =  dataframe.DataFrame(conh, 'SELECT * FROM _SYS_BIC."balanceamento/CVD_ZTBLMM1032_Endereco" where WERKS= \'' + filial +'\'')
            #df_endereco = hn_endereco.collect()
            #df_endereco.to_sql('erp_endereco', con=engine, if_exists='replace')

            st.write("Importando Saidas")
            ## Atualização de Saídas 
            #hn_saidas =  dataframe.DataFrame(conh, 'SELECT * FROM _SYS_BIC."balanceamento/CVD_ZTBLMM1032_Balanceamento" where WERKS= \'' + filial +'\' and ERDAT BETWEEN \'20220701\' AND \'20220701\' ')
            #df_saidas = hn_saidas.collect()
            #df_saidas.to_sql('erp_saidas', con=engine, if_exists='replace')
            conh.close()

            sp_erp_import = 'prc_erp_import'
            st.write("Finalizando Importação")
            param_procedure = (filial, data_ini, data_fin)
            sql.executar_procedure(sp_erp_import,param_procedure,conn)
             
            st.write("Atualizando Cadastros e Saidas")
            sp_erp_import = 'prc_erp_update'
            param_procedure = ()
            sql.executar_procedure(sp_erp_import,param_procedure,conn)
             
            st.write("Atualizacao concluida!")

    authenticator.logout("Logout", "sidebar")
