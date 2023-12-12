import mysql.connector
import requests
import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi



# Capturando os valores de nossas variáveis com o os.getenv

def connectMySQL(host, user, password):
    cnx = None
    
    try:
        cnx = mysql.connector.connect(
        host = host,
        user = user,
        password = password
    )
        print(cnx)
        print("Você foi conectado com sucesso, bb!")
    except Exception as e:
        print(f"Ocorreu um erro aqui: {e.message}")
        
    return cnx

def connectMongoDB(uri):

    client = None
    
    uri = uri

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e) 
    
    return client

def extraiDados(url):
    response = requests.get(url)
    
    return response

def createDatabase(cursor, db):
    return cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db};")

def showDatabases(cursor):
    return cursor.execute("SHOW DATABASES;")

    
    
    