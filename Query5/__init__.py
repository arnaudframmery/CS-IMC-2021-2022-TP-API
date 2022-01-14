import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    genre = req.params.get('genre')
    acteur = req.params.get('acteur')
    directeur = req.params.get('directeur')
    
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
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()

            if not genre and not acteur and not directeur:
                cursor.execute("SELECT AVG(runtimeMinutes) FROM [dbo].[tTitles] title WHERE runtimeMinutes IS NOT NULL")
            
            if genre and not acteur and not directeur:
                cursor.execute(f"SELECT AVG(runtimeMinutes) FROM [dbo].[tTitles] title JOIN [dbo].[tGenres] genre ON genre.tconst = title.tconst WHERE runtimeMinutes IS NOT NULL AND genre.genre = '{genre}'")
            
            if not genre and acteur and not directeur:
                cursor.execute(
                    f"SELECT AVG(runtimeMinutes) FROM ( \
                    SELECT DISTINCT title.tconst, runtimeMinutes \
                    FROM [dbo].[tTitles] title \
                    INNER JOIN [dbo].[tPrincipals] principal ON principal.tconst = title.tconst \
                    INNER JOIN [dbo].[tNames] names ON principal.nconst = names.nconst \
                    WHERE runtimeMinutes IS NOT NULL \
                    AND principal.category = 'acted in' \
                    AND names.primaryName = '{acteur}') AS tmp"
                )

            rows = cursor.fetchall()
            for row in rows:
                dataString += f"SQL: averageRuntimeMinute={row[0]}\n"

    except:
        errorMessage = "Erreur de connexion a la base SQL"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + " Connexions réussies a Neo4j et SQL!")
