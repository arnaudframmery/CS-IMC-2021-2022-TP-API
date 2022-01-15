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
        genres = graph.run("MATCH (n:Name)-[r1]-(t:Title)-[r2]-(g:Genre) WITH n, t, g, r1 WHERE type(r1) = \"ACTED_IN\" OR type(r1) = \"DIRECTED\" WITH n, t, g, count(DISTINCT type(r1)) AS rCount WHERE rCount > 1 RETURN DISTINCT(g.genre)")
        dataString = "Les genres pour lesquels au moins un film a une même personne qui a été la fois directeur et acteur :\n"
        if len(genres) != 0:
            for i, genre in enumerate(genres):
                dataString += f"{i+1} -> {genre}\n"
        else:
            dataString += "Aucune donnée disponible ( ಠ ʖ̯ ಠ)\n"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString)
