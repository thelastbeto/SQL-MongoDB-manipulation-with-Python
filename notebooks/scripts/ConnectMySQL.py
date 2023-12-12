import os
from dotenv import load_dotenv
import functions as f 
import pandas as pd

# Carregamento do arquivo .env no ambiente de trabalho
load_dotenv()

# Capturando os valores de nossas variáveis com o os.getenv
host = os.getenv("DB_HOST")
user = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

path = "../data/dados_transformados.csv"

cnx = f.connectMySQL(host, user, password)

if cnx:
    # Criando um cursor:
    # (O cursor só deve ser criado caso o banco de dados seja corretamente conectado).
    cursor = cnx.cursor(buffered = True)

    # Criando uma base de dados:
    showDb = f.showDatabases(cursor)

    for db in cursor:
        print(f"DB disponível em sua conexão: {db}")    
else:
    print("Connection failed. Check your credentials.")

############### Criando uma base de dados #########################

createDb = f.createDatabase(cursor, "dbprodutos_teste")

cursor.execute("""
               
CREATE TABLE IF NOT EXISTS dbprodutos_teste.tb_livros(
               id VARCHAR(100),
               Produto VARCHAR(100),
               Categoria_Produto VARCHAR(100),
               Preco FLOAT(10,2),
               Frete FLOAT(10,2),
               Data_Compra DATE,
               Vendedor VARCHAR(100),
               Local_Compra VARCHAR(100),
               Avaliacao_Compra INT,
               Tipo_Pagamento VARCHAR(100),
               Qntd_Parcelas INT,
               Latitude FLOAT(10,2),
               Longitude FLOAT(10,2),
                
                PRIMARY KEY (id)
                
                );
               """)

# Verificando a tabela criada:

cursor.execute("USE dbprodutos_teste;")
cursor.execute("SHOW TABLES;")

for tb in cursor:
    print(f"Tabela disponível no DB selecionado: {tb}")
    
################ INSERINDO OS DADOS NA TABELA #################

# Neste for, utilizaremos 2 variáveis, pois o iterrows retorna dois tipos de conteudo.
# Retorno i -> cada um dos índices do nosso DataFrame.
# Retorno row -> cada um dos dados de cada linha que temos no DataFrame. 

# Leitura dos dados e transformando em DF:

df_livros = pd.read_csv(path)

for i, row in df_livros.iterrows():
    print(tuple(row))
    
# transformando com o list comprehension 
lista_dados = [tuple(row) for i, row in df_livros.iterrows()]
lista_dados

sql = "INSERT INTO dbprodutos.tb_livros VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

cursor.executemany(sql, lista_dados)
cnx.commit()

print(cursor.rowcount, "dados inseridos")

################### VISUANDO OS DADOS INSERIDOS #################

cursor.execute("SELECT * FROM dbprodutos.tb_livros;")

for row in cursor:
    print(row)
    
cursor.close()
cnx.close()