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
    paramsMap = {
        "genre": genre,
        "acteur": acteur,
        "directeur": directeur
    }
    paramsString = ', '.join([f"{key}: {value}" for key, value in paramsMap.items() if value])
    
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
    dataString = f"Durée moyenne des films {f'({paramsString})' if paramsString != '' else ''} : "

    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()

            queryBase = "SELECT runtimeMinutes FROM [dbo].[tTitles] title "
            queryJoin = ""
            queryWhere = "WHERE runtimeMinutes IS NOT NULL "
            queryGroupBy = "GROUP BY title.tconst, primaryTitle, runtimeMinutes "
            queryHaving = ""

            if genre:
                queryJoin += "INNER JOIN [dbo].[tGenres] genre ON genre.tconst = title.tconst "
                queryWhere += f"AND genre.genre = '{genre}' "
            
            if acteur or directeur:
                queryJoin += "INNER JOIN [dbo].[tPrincipals] principal ON principal.tconst = title.tconst "
                queryJoin += "INNER JOIN [dbo].[tNames] names ON principal.nconst = names.nconst "
                queryWhere += "AND ( "

                if acteur:
                    queryWhere += f"(category = 'acted in' AND primaryName = '{acteur}') "
                else:
                    queryWhere += f"1=0 " # False
                
                queryWhere += f"OR "

                if directeur:
                    queryWhere += f"(category = 'directed' AND primaryName = '{directeur}') "
                else:
                    queryWhere += f"1=0 " # False
                
                queryWhere += f") "

            if acteur and directeur:

                queryHaving += "HAVING count(DISTINCT(category)) > 1 "
            
            cursor.execute("SELECT AVG(runtimeMinutes) FROM ( " + queryBase + queryJoin + queryWhere + queryGroupBy + queryHaving + ") AS tmp")

            rows = cursor.fetchall()
            if len(rows) != 0:
                dataString += f"{rows[0][0]} min\n"
            else:
                dataString += "Aucune donnée disponible ( ಠ ʖ̯ ಠ) essayez avec d'autres paramètres !\n"

    except:
        errorMessage = "Erreur de connexion a la base SQL"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString)
