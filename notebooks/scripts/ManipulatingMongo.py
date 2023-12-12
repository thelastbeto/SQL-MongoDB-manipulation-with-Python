import os
import functions as f
import pandas as pd

uri = os.getenv("MONGODB_URI")

client = f.connectMongoDB(uri)

path = "../data/dados_transformados.csv"
path2 = "../data/produtos_transformados.csv"

if client:
# Criando e testando o banco de dados e collection:
    db = client["db_produtos"]
    collection = db["produtos"]

    # Conferindo se o db foi criado:
    print(client.list_database_names())

    # Criando dados para preenchimento da coleção
    product = {"Produto": "Computador",
            "Quantidade": 77,}

    # collection.insert_one(product)

    # Verificando se nosso dado foi inserido
    (collection.find_one())

    # Conferindo a lista de DBs após criação de uma coleção.
    client.list_database_names()

########### Extraindo dados da API ##################

response = f.extraiDados("https://labdados.com/produtos")
print(response.json())

########### Adicionando dados extraídos na coleção #################

docs = collection.insert_many(response.json())

# Retorna a quantidade pelo número IDs inseridos em nossa base de dados.
print(f"Quantidade de arquivos inseridos em nossa base: {len(docs.inserted_ids)}")

# Conferindo o total de documentos:
print(f"Agrupamento do total de documentos: {collection.count_documents({})}") # As chaves servem para contar quantos docs temos atualmente em nossa base de dados

############ Lendo os dados no MongoDB ###################

db = client["db_produtos"]
collection = db["produtos"]

# find() funciona de maneira semelhante ao for do Python
for doc in collection.find():
    print(f"Leitura dos nossos arquivos presentes na collection: {doc}")
    
########## Alterando os campos de latitude e longitude ################

collection.update_many({}, {"$rename": {"lat":"Latitude", "lon":"Longitude"}})
print(f"Retorno das renomeações das colunas lat e lon: {collection.find_one()}")

############# Aplicando transformações com filtros da categorias de livros ##############

collection.distinct("Categoria do Produto")

# Vamos pegar os campos onde as categorias do produtos são livros:

query = {"Categoria do Produto": "livros"}

lista_livros = []
for doc in collection.find(query):
    # print(doc)
    lista_livros.append(doc)

############ Salvando os dados em um DataFrame com o Pandas ##################

df_livros = pd.DataFrame(lista_livros)
print(df_livros.head())

############### Formatando as datas ###############

df_livros['Data da Compra'] = pd.to_datetime(df_livros['Data da Compra'], format = "%d/%m/%Y")
print(df_livros.info())

df_livros['Data da Compra'] = df_livros['Data da Compra'].dt.strftime("%Y-%m-%d")
print(df_livros.head())

########## Salvando arquivos em CSV ###############
df_livros.to_csv(path, index = False)

########### FILTRANDO PRODUTOS VENDIDOS A PARTIR DE 2021 #######################

query = {"Data da Compra": {"$regex": "/202[1-9]"}}

lista_produtos = []
for doc in collection.find(query):
    lista_produtos.append(doc)

### Transformando em DF:

df_produtos = pd.DataFrame(lista_produtos)
print(df_produtos)

# Formatando as datas e salvando em csv

df_produtos['Data da Compra'] = pd.to_datetime(df_produtos['Data da Compra'], format = "%d/%m/%Y")
df_produtos['Data da Compra'] = df_produtos['Data da Compra'].dt.strftime("%Y-%m-%d")
print(f"DataFrame de Produtos após transformação das datas: {df_produtos.head()}")

df_produtos.to_csv(path2, index = False)

# Fechando conexão com o banco:

client.close()