import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sql
# import openpyxl
import Ferramenta_de_Balanceamento as fb

# Executar a conexao com o servidor SQLSERVER
conn = sql.retornar_conexao_sql(fb.text_ip,fb.text_database,fb.text_user,fb.text_password)

#with st.sidebar:
selected = option_menu(
    menu_title= None,
    options=["Entre Áreas","Entre Estações","Dentro da Estação"],
    orientation="horizontal",
)

if selected == "Entre Áreas":
    st.title("BALANCEAMENTO ENTRE AREA PICKING")
    st.sidebar.header("PARÂMETROS:")

    # Criando um formulário para entrar com os parâmetros
    sql1 = "select area_picking_mae,count(distinct area_picking) as total_area_picking	from enderecos group by	area_picking_mae order by area_picking_mae"
    df1 = pd.read_sql(sql1,conn)
    df1 = df1[(df1["total_area_picking"] > 1)]
    if  not df1.empty:
        # Criando um formulário para entrar com os parâmetros
        form = st.sidebar.form("Parâmetros:")
        txt_area_picking = form.selectbox("Area Picking Mãe:",df1)
        txt_limite_interacoes = form.text_input("Limite de Interações:")
        submit_button = form.form_submit_button("Submit")
        if submit_button:
            # Executar a procedure de balanceamento entre estaçoes
            spbalanceamento_entre_area_picking = 'processar_balanceamento_entre_area_picking_final'
            param_procedure = (txt_area_picking, txt_limite_interacoes,)
            sql.executar_procedure(spbalanceamento_entre_area_picking,param_procedure,conn)

        #executar a select para pegar os dados do balancemaento entre areas
        sql2 = "select * from vw_base_acessos_area_picking_mae_realizado order by area_picking_mae, area_picking"
        df2 = pd.read_sql(sql2,conn)
            
        # Fazendo o gráfico do balanceamento entre areas
        fig1 = go.Figure(data=[
        go.Bar(name='Atual', x=df2["area_picking"], y=df2["acessos_atual"],text=(df2["acessos_atual"]/sum(df2["acessos_atual"])*100).round(2)),
        go.Bar(name='Ideal', x=df2["area_picking"], y=df2["acessos_ideal"],text=(df2["acessos_ideal"]/sum(df2["acessos_ideal"])*100).round(2))])
        # Change the bar mode
        fig1.update_layout(barmode='group',title = "ACESSOS")
        st.plotly_chart(fig1)

        #executar a select para pegar a relação de DE_PARA
        st.header("Relação de DE-PARA:")
        sql3 = "select * from vw_produtos_de_para order by estacao_de,endereco_de"
        df3 = pd.read_sql(sql3,conn)
        if df3.empty:
            st.write("Não há dados pra serem visualizados...")
        else:
            st.write(df3)
            button_exportar = st.button("Exportar")
            if button_exportar:
                # exportar o arquivo de-para para o formato xls
                writer = pd.ExcelWriter(r"C:\Users\rocha\OneDrive\Documentos\Python\de-para.xlsx", engine= "openpyxl")
                df3.to_excel(writer,sheet_name = 'DE_PARA')
                writer.save()
                writer.close()
                # exportar o arquivo de-para para o formato csv
                df3.to_csv(r"C:\Users\rocha\OneDrive\Documentos\Python\de-para.csv",index=False)
                st.write("Arquivo exportado com sucesso...")

        #EXECUTAR VW_GERAR_PRODUTOS_SEM_ENDERECOS
        st.header("Produtos sem endereços")
        sql5 = "select * from vw_gerar_produtos_sem_enderecos"
        df5 = pd.read_sql(sql5,conn)
        if df5.empty:
            st.write("Não há dados pra serem visualizados...")
        else:
            st.write(df5)
            button_exportar = st.button("Exportar1")
            if button_exportar:
                #EXPORTAR O ARQUIVO PARA FPRMATO XLS
                writer = pd.ExcelWriter(r"C:\Users\rocha\OneDrive\Documentos\Python\sem_end.xlsx", engine= "openpyxl")
                df5.to_excel(writer,sheet_name = "PROD_SEM_ENDERECO")
                writer.save()
                writer.close()
                #EXPORTAR O ARQUIVO PARA O FORMATO CSV
                df5.to_csv(r"C:\Users\rocha\OneDrive\Documentos\Python\sem_end.csv",index=False)
                st.write("Arquivo exportado com sucesso...")
    else:
        st.sidebar.subheader("Não se aplica para este CD...")
    conn.close()
if selected == "Entre Estações":
    st.title("BALANCEAMENTO ENTRE ESTAÇÕES")
    st.sidebar.header("PARÂMETROS:")

    # Criando um formulário para entrar com os parâmetros final
    form = st.sidebar.form("Parâmetros:")
    sql4 = "select distinct area_picking from enderecos order by area_picking"
    df4 = pd.read_sql(sql4,conn)
    txt_area_picking = form.selectbox("Area Picking:",df4)
    txt_desvio_medio = form.text_input("Desvio Médio:")
    txt_limite_interacoes = form.text_input("Limite de Interações:")
    submit_button = form.form_submit_button("Submit")
    if submit_button :
        # Executar a procedure de balanceamento entre estaçoes
        spbalanceamento_entre_estacoes = 'processar_balanceamento_entre_estacoes_final'
        param_procedure = (txt_area_picking, txt_desvio_medio, txt_limite_interacoes,)
        sql.executar_procedure(spbalanceamento_entre_estacoes,param_procedure,conn)

    #executar a select para pegar o desvio médio entre estaçoes
    sql1 = "select cast(avg(abs(desvio_perc)) as decimal(5,2)) as desvio_medio from vw_base_acessos_ranking"
    df1 = pd.read_sql(sql1,conn)
    st.write("Desvio médio entre estações: " + str(df1["desvio_medio"].values[0]))

    st.header("Gráficos de Balanceamento Entre Estações")
    #executar a select para pegar os dados do desvio de balancemaento entre estaçoes
    sql2 = "select area_picking,estacao,acessos,desvio_perc,ideal_acessos_estacao_com_desviador from vw_base_acessos_ranking order by estacao"
    df2 = pd.read_sql(sql2,conn)
    # Fazendo o gráfico 
    fig1 = go.Figure(data=[
    go.Bar(name='Atual', x=df2["estacao"], y=df2["acessos"]),
    go.Line(name='Ideal', x=df2["estacao"], y=df2["ideal_acessos_estacao_com_desviador"])])
    # Change the bar mode
    fig1.update_layout(barmode='group')
    st.plotly_chart(fig1)

    fig2 = px.bar(df2, x="estacao", y="desvio_perc")
    st.plotly_chart(fig2)

    st.header("Relação de DE-PARA:")
    #executar a select para pegar a relação de DE_PARA
    sql3 = "select * from vw_produtos_de_para order by estacao_de,endereco_de"
    df3 = pd.read_sql(sql3,conn)
    if df3.empty:
        st.write("Não há dados pra serem visualizados...")
    else:
        st.write(df3)
        button_exportar = st.button("Exportar")
        if button_exportar:
            # exportar o arquivo de-para para o formato xls
            writer = pd.ExcelWriter(r"C:\Users\rocha\OneDrive\Documentos\Python\de-para.xlsx", engine= "openpyxl")
            df3.to_excel(writer,sheet_name = 'DE_PARA')
            writer.save()
            writer.close()
            # exportar o arquivo de-para para o formato csv
            df3.to_csv(r"C:\Users\rocha\OneDrive\Documentos\Python\de-para.csv",index=False)
            st.write("Arquivo exportado com sucesso...")

    conn.close()
if selected == "Dentro da Estação":
    st.title("BALANCEAMENTO DENTRO DAS ESTAÇÕES")
    st.sidebar.header("PARÂMETROS:")

    # Criando um formulário para entrar com os parâmetros
    form = st.sidebar.form("Parâmetros:")
    sql4 = "select distinct area_picking from enderecos order by area_picking"
    df4 = pd.read_sql(sql4,conn)
    txt_area_picking = form.selectbox("Area Picking:",df4)
    txt_limite_interacoes = form.text_input("Limite de Interações:")
    submit_button = form.form_submit_button("Submit")
    if submit_button:
        # Executar a procedure de balanceamento entre estaçoes
        spbalanceamento_dentro_estacoes = 'processar_balanceamento_dentro_estacao_final'
        param_procedure = (txt_area_picking, txt_limite_interacoes,)
        sql.executar_procedure(spbalanceamento_dentro_estacoes,param_procedure,conn)

    #executar a select para pegar o desvio médio entre estaçoes
    sql1 = "select cast(avg(abs(desvio_perc)) as decimal(5,2)) as desvio_medio from vw_base_acessos_ranking"
    df1 = pd.read_sql(sql1,conn)
    st.write("Desvio médio entre estações: " + str(df1["desvio_medio"].values[0]))

    st.header("Gráfico de balanceamento dentro da Estação")
    #executar a select para pegar a informação de Acessos por Posicao Estante
    sql2 = "select area_picking,posicao_estante,acessos, acessos_ideal from vw_base_acessos_area_picking_posicao_estante_realizada order by posicao_estante"
    df2 = pd.read_sql(sql2,conn)
    df2 = df2[(df2["area_picking"] == txt_area_picking)]
    # Fazendo o gráfico 
    fig = go.Figure(data=[
    go.Bar(name='Atual', x=df2["posicao_estante"], y=df2["acessos"]),
    go.Bar(name='Ideal', x=df2["posicao_estante"], y=df2["acessos_ideal"])])
    # Change the bar mode
    fig.update_layout(barmode='group')
    st.plotly_chart(fig)

    st.header("Relação de DE-PARA:")
    #executar a select para pegar a relação de DE_PARA
    sql3 = "select * from vw_produtos_de_para order by estacao_de,endereco_de"
    df3 = pd.read_sql(sql3,conn)
    if df3.empty:
        st.write("Não há dados pra serem visualizados...")
    else:
        st.write(df3)
        button_exportar = st.button("Exportar")
        if button_exportar:
            # exportar o arquivo de-para para o formato xls
            writer = pd.ExcelWriter(r"C:\Users\rocha\OneDrive\Documentos\Python\de-para.xlsx", engine= "openpyxl")
            df3.to_excel(writer,sheet_name = 'DE_PARA')
            writer.save()
            writer.close()
            # exportar o arquivo de-para para o formato csv
            df3.to_csv(r"C:\Users\rocha\OneDrive\Documentos\Python\de-para.csv",index=False)
            st.write("Arquivo exportado com sucesso...")

    conn.close()
