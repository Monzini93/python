from sqlalchemy import create_engine, text # type: ignore
import pandas as pd
from datetime import datetime
import mysql.connector
import os
import numpy as np
import gera_relatorios # type: ignore

conn = mysql.connector.connect(
    host='campsoftdb.c376ljpyburn.sa-east-1.rds.amazonaws.com',
    user='yuri',
    password='y*u&r%i.',
    database='campsoft'
)
cursor = conn.cursor()

id_do_provedor = str(input("Digite o ID(s) do(s) Provedor(es) (Separados por Virgula): "))
ano_calculo = int(input("Digite o Ano do cálculo: "))
mes_calculo = int(input("Digite o Mês do cálculo: "))

# Inicializar uma lista para armazenar as linhas
#linhas = []

# Registrar o horário de início
start_time = datetime.now()
print(f"START_TIME: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

# Configurar a URL de conexão do banco de dados
db_url = f"mysql+mysqlconnector://{'yuri']}:{'y*u&r%i.'}@{'campsoftdb.c376ljpyburn.sa-east-1.rds.amazonaws.com']}/campsoft"

# Criar um engine SQLAlchemy
engine = create_engine(db_url)

consulta_sql = f"""
SELECT id_provedor, provedor_nome FROM provedor
WHERE id_provedor IN ({str(id_do_provedor)});
"""

with engine.connect() as connection:
    df_provedores = pd.read_sql(consulta_sql, connection)

for index,row in df_provedores.iterrows():
    print(row['id_provedor'])
    # Consulta SQL
    consulta_sql1 = f"""
    SELECT
     YEAR(@data_in) AS `ano`,
     MONTH(@data_in) AS `mes`,
     q1.id_provedor_usuario_licenca,
     q1.id_provedor,
     p.provedor_nome,
     q1.id_provedor_usuario,
     q1.id_conteudista_produto,
     cp.conteudista_produto_nome,
     q1.data_inicio,
     MIN(q1.data_final) AS data_final,
     DATEDIFF(MIN(q1.data_final), q1.data_inicio) AS `data_diferenca`,
     pud.nome,
     pud.cpf_cnpj,
     pud.email
    FROM
    (
     SELECT
        q2.id_provedor_usuario_licenca,
        q2.id_provedor,
        q2.id_provedor_usuario,
        q2.id_conteudista_produto,
        q2.data_inicio,
        MIN(q2.data_final) AS data_final
     FROM
        (
        SELECT
         pul.id_provedor_usuario_licenca,
         pul.id_provedor,
         pul.id_provedor_usuario,
         pul.id_conteudista_produto,
         pul.data_inicio,
         CASE
            WHEN pul.status = 'deletado' THEN pul.data_inicio
            WHEN pul.status = 'inativo'
            AND pul.data_final IS NULL THEN pul.data_inicio
            ELSE pul.data_final
         END AS data_final
        FROM provedor_usuario_licenca pul
        WHERE pul.id_provedor = {row['id_provedor']} AND (seguranca = 0 OR seguranca IS NULL)
     UNION ALL
        SELECT
         pulh.id_provedor_usuario_licenca,
         pulh.id_provedor,
         pulh.id_provedor_usuario,
         pulh.id_conteudista_produto,
         pulh.data_inicio,
         MIN(pulh.data_final) AS data_final
        FROM provedor_usuario_licenca_historico pulh
        WHERE pulh.id_provedor = {row['id_provedor']} AND status = 'inativo' AND (seguranca = 0 OR seguranca IS NULL)
        GROUP BY pulh.id_provedor_usuario_licenca
             ) AS q2
         GROUP BY q2.id_provedor_usuario_licenca
    )AS q1
    LEFT JOIN provedor p ON p.id_provedor = q1.id_provedor
    INNER JOIN provedor_usuario_dados pud ON pud.id_provedor_usuario = q1.id_provedor_usuario
    LEFT JOIN conteudista_produto cp ON cp.id_conteudista_produto = q1.id_conteudista_produto
    WHERE
     (q1.data_inicio < DATE(@data_in) AND q1.data_final > DATE(@data_fim))
     OR (q1.data_inicio >= DATE(@data_in) AND q1.data_inicio <= DATE(@data_fim))
     OR (q1.data_final <= DATE(@data_fim) AND q1.data_final >= DATE(@data_in) AND DAY(q1.data_final) > DAY(q1.data_inicio))
     OR (q1.data_inicio < DATE(@data_fim) AND q1.data_final IS NULL)
    GROUP BY
     q1.id_provedor_usuario_licenca
    ORDER BY
     q1.id_conteudista_produto,
     pud.nome;
    """

# Executar a consulta e carregar os resultados em um DataFrame
with engine.connect() as connection:
    connection.execute(text(f"SET @data_in = '{str(ano_calculo)}-{str(mes_calculo)}-01';"))
    connection.execute(text(f"SET @data_fim = DATE_FORMAT(LAST_DAY('{str(ano_calculo)}-{str(mes_calculo)}-01'),'%Y-%m-%d');"))
    df_licencas = pd.read_sql(consulta_sql1, connection)

# Filtrar o DataFrame para remover linhas onde 'data_diferenca' é igual a 0
df_licencas = df_licencas.loc[(df_licencas['data_diferenca'] > 0) | (df_licencas['data_diferenca'].isnull())]

df_licencas_unicos = df_licencas.drop_duplicates(subset=['id_conteudista_produto', 'id_provedor_usuario'])
gera_relatorios.gera_latorio(row['id_provedor'],row['provedor_nome'],df_licencas_unicos)

# Agrupar por 'id_conteudista_produto' e contar os 'id_provedor_usuario' únicos
#df_resultado = df_licencas.groupby('id_conteudista_produto')['id_provedor_usuario'].nunique().reset_index()

df_resumido = (
    df_licencas_unicos.groupby(['ano', 'mes', 'conteudista_produto_nome', 'id_conteudista_produto'])
    .agg({'id_provedor_usuario': pd.Series.nunique})
    .reset_index()
    .rename(columns={'id_provedor_usuario': 'assinaturas'})
)

print("------------------------------------------------------------")
print("Licenças Recalculadas:")
print(df_resumido)

select_assinaturas = f"""
SELECT
    ano,mes,id_conteudista_produto,qtd_licencas_empacotadas_aferidas+qtd_licencas_unitarias_aferidas AS licencas
FROM provedor_licenca_aferida
WHERE ano = {str(ano_calculo)}
AND mes = {str(mes_calculo)}
AND id_provedor = {str(id_do_provedor)}
"""
with engine.connect() as connection:
    df_licencas_antigas = pd.read_sql(select_assinaturas, connection)

print("------------------------------------------------------------")
print("Licenças Antigas:")
print(df_licencas_antigas)

verificacao_atualiar = str(input("Deseja atualizar os dados do banco de dados? (Y/N): "))

if verificacao_atualiar == 'Y' or verificacao_atualiar == 'y':
    print(f"DELETE FROM provedor_licenca_aferida WHERE ano = {str(ano_calculo)} AND mes = {str(mes_calculo)} AND id_provedor = {str(id_do_provedor)};")
    sql_delete = f"DELETE FROM provedor_licenca_aferida WHERE ano = {str(ano_calculo)} AND mes = {str(mes_calculo)} AND id_provedor = {str(id_do_provedor)};"
    cursor.execute(sql_delete)
    conn.commit()

    for index_assinaturas, row_assinaturas in df_resumido.iterrows():
        sql_insert = f"""
         INSERT INTO `provedor_licenca_aferida`
            (`id_provedor_licenca_aferida`,`ano`, `mes`, `id_provedor`, `id_conteudista_produto`, `qtd_licencas_empacotadas_aferidas`)
         VALUES(
            BIN_TO_UUID(UUID_TO_BIN(MD5(CONCAT({row_assinaturas['ano']}, '-',
            {row_assinaturas['mes']}, '-',
            {id_do_provedor} , '-',
            '{row_assinaturas['id_conteudista_produto']}')))),
            {row_assinaturas['ano']},
            {row_assinaturas['mes']},
            {id_do_provedor},
            '{row_assinaturas['id_conteudista_produto']}',
            {row_assinaturas['assinaturas']}
         );
        """
        cursor.execute(sql_insert)
        conn.commit()

else:
    print('Não atualiza')

# Registrar o horário de término
end_time = datetime.now()
print(f"FINISH_TIME: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

# Calcular a duração total
duration = end_time - start_time
print(f"DURATION: {duration}")