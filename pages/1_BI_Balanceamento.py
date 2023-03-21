import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sql
from datetime import date
import json
import Ferramenta_de_Balanceamento as fb
import pickle
from pathlib import Path
import streamlit_authenticator as stauth
import generate_keys as gk
import dataframe as DF


# Executar a conexao com o servidor SQLSERVER 123456hsfhgdfgh789
conn = sql.retornar_conexao_sql(fb.text_ip,fb.text_database,fb.text_user,fb.text_password)
st.sidebar.write(fb.text_ip)

#with st.sidebar:
selected = option_menu(
    menu_title= None,
    options=["Balanceamento","Indicadores", "Caixas por área", "Abastecimento"],
    orientation="horizontal",
)

if selected == "Balanceamento":
    st.title("PAGINA INICIAL")

    st.sidebar.header("PARÂMETROS:")
    Medida_Selecionada = st.sidebar.radio("Escolha uma medida:",["Unidades","Acessos"], index = 1, horizontal=True)


    # Gráfico do Balanceamento entre áreas
    st.header("Balanceamento Entre Áreas")
    sql1 = "select * from vw_base_acessos_area_picking_mae order by area_picking_mae, area_picking"
    df1 = pd.read_sql(sql1,conn)
    # Fazendo o gráfico do balanceamento entre areas
    if Medida_Selecionada == "Acessos":
        fig = go.Figure(data=[
        go.Bar(name='Atual', x=df1["area_picking"], y=df1["acessos_atual"]),
        go.Bar(name='Ideal', x=df1["area_picking"], y=df1["acessos_ideal"])])
        # Change the bar mode
        fig.update_layout(barmode='group',title = "ACESSOS")
        st.plotly_chart(fig)
    else:
        fig = go.Figure(data=[
        go.Bar(name='Atual', x=df1["area_picking"], y=df1["unidades_atual"]),
        go.Bar(name='Ideal', x=df1["area_picking"], y=df1["unidades_ideal"])])
        # Change the bar mode
        fig.update_layout(barmode='group', title = "UNIDADES")
        st.plotly_chart(fig)

    # Gráfico do Balanceamento entre estações
    st.header("Balanceamento Estações")
    sql2 = "select * from vw_base_acessos_area_picking order by area_picking, estacao"
    df2 = pd.read_sql(sql2,conn)
    Area_Selecionada = st.selectbox("Area_Picking:",df2["area_picking"].unique())
    df2 = df2[(df2["area_picking"] == Area_Selecionada)] 

    #executar a select para pegar o desvio médio entre estaçoes
    sql4 = "select cast(avg(abs(desvio_ideal_acessos_estacao_com_desviador)) as decimal(5,2)) as desvio_medio from vw_base_acessos_area_picking where area_picking = '"+Area_Selecionada+"'"
    df4 = pd.read_sql(sql4,conn)
    st.write("Desvio médio entre estações: " + str(df4["desvio_medio"].values[0]))

    # Fazendo o gráfico do balanceamento entre estaçoes 654987321
    st.subheader("Balanceamento entre Estações")
    if Medida_Selecionada == "Acessos":
        fig2 = go.Figure(data=[
        go.Bar(name='Atual', x=df2["estacao"], y=df2["acessos"]),
        go.Bar(name='Ideal', x=df2["estacao"], y=df2["acessos_ideal"]),
        go.Line(name='Média', x=df2["estacao"], y=df2["ideal_acessos_estacao_com_desviador"])])
        # Change the bar mode
        fig2.update_layout(barmode='group',title = "ACESSOS")
        st.plotly_chart(fig2)
    else:
        fig2 = go.Figure(data=[
        go.Bar(name='Atual', x=df2["estacao"], y=df2["unidades"]),
        go.Bar(name='Ideal', x=df2["estacao"], y=df2["unidades_ideal"]),
        go.Line(name='Média', x=df2["estacao"], y=df2["ideal_unidades_estacao_com_desviador"])])
        # Change the bar mode
        fig2.update_layout(barmode='group',title = "UNIDADES")
        st.plotly_chart(fig2)

    # Gráfico do Balanceamento dentro da estação
    st.subheader("Balanceamento Dentro das Estações")
    sql3 = "select * from vw_base_acessos_area_picking_posicao_estante order by posicao_estante"
    df3 = pd.read_sql(sql3,conn)
    df3 = df3[(df3["area_picking"] == Area_Selecionada)]
    # Fazendo o gráfico do balanceamento dentro da estação
    if Medida_Selecionada == "Acessos":
        fig3 = go.Figure(data=[
        go.Bar(name='Atual', x=df3["posicao_estante"], y=df3["acessos"]),
        go.Bar(name='Ideal', x=df3["posicao_estante"], y=df3["acessos_ideal"])])
        # Change the bar mode
        fig2.update_layout(barmode='group',title = "ACESSOS")
        st.plotly_chart(fig3)
    else:
        fig3 = go.Figure(data=[
        go.Bar(name='Atual', x=df3["posicao_estante"], y=df3["unidades"]),
        go.Bar(name='Ideal', x=df3["posicao_estante"], y=df3["unidades_ideal"])])
        # Change the bar mode
        fig2.update_layout(barmode='group',title = "UNIDADES")
        st.plotly_chart(fig3)
        
if selected == "Indicadores":
    # Carregar os dados do SQL
    df0 = DF.df_BI1
    df0["data"] = df0["data"].astype(str)
    df0["unidades"] = df0["unidades"].astype(int)
    # # Montar a tabela Pivot
    df_pivot_unidades = pd.pivot_table(df0,values="unidades",index=["data"], columns=["area_picking"],aggfunc='sum' ,margins=True, margins_name="TOTAL")
    df_pivot_acessos = pd.pivot_table(df0,values="acessos",index=["data"], columns=["area_picking"],aggfunc='sum' ,margins=True, margins_name="TOTAL")
    st.header("UNIDADES")
    st.dataframe(df_pivot_unidades)
    st.header("ACESSOS")
    st.dataframe(df_pivot_acessos)
    
if selected == "Caixas por área":
     st.header("Gráfico Caixas por área")
     df0 = DF.df_BI2
     fig = go.Figure(data=[
     go.Bar(name='caixas', x=df0["estacao"], y=df0["cx"]/df0["tt"]*100, text=(df0["cx"]/df0["tt"]*100).round(2))])
     # Change the bar mode
     fig.update_layout(barmode='group', title = "% Caixas que iniciam por estação atual")
     st.plotly_chart(fig)

if selected == "Caixas por área":
     df0 = DF.df_BI3
     fig = go.Figure(data=[
     go.Bar(name='caixas', x=df0["estacao"], y=df0["cx"]/df0["tt"]*100, text=(df0["cx"]/df0["tt"]*100).round(2))])
     # Change the bar mode
     fig.update_layout(barmode='group', title = "% Caixas que iniciam por estação ideal")
     st.plotly_chart(fig)

if selected == "Abastecimento":
    st.title("Relatório de abastecimento Atual")
    df0 = DF.df_BI4
    st.write(df0)

    st.title("Relatório abastecimento ideal")
    df5 = DF.df_BI5
    st.write(df5)
    
    
