import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0 or len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        titles = graph.run("MATCH (:Name)-[r:ACTED_IN]-(t:Title) WITH t, count(r) AS nCount WITH max(nCount) AS maxCount MATCH (:Name)-[r:ACTED_IN]-(t:Title) WITH t, maxCount, count(r) AS nCount WHERE nCount = maxCount RETURN t.tconst, t.primaryTitle")
        dataString = "Les films ayant le plus d'acteurs :\n"
        if titles.data():
            for i, title in enumerate(titles):
                dataString += f"{i+1} -> {title['t.tconst']}, {title['t.primaryTitle']}\n"
        else:
            dataString += "Aucune donnée disponible ( ಠ ʖ̯ ಠ)\n"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString)
