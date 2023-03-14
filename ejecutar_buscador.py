import os
import requests
import time
import pymssql
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

server   = os.getenv("SQL_SERVER")
user     = os.getenv("SQL_USER")
password = os.getenv("SQL_PASSWORD")
database = os.getenv("SQL_DATABASE")

conn = pymssql.connect(server=server, user=user, password=password, database=database)  
cursor = conn.cursor()


def leer_query(filename):
    return open(f"./queries/{filename}", 'r').read()


def make_api_request(query):
    # url = "http://172.16.101.71:5100/searchB2B"
    url = "http://172.16.101.71:5100/searchB2B"
    headers = {
    'Content-Type': 'application/json'
    }
    json_data = {"query": query, "documents": True, "id_usuario": "Test", "id_cliente": "Test"}
    response = requests.request("POST", url=url, headers=headers, json=json_data)
    return response.json()

query_db = leer_query("busquedas_db.sql")
busquedas = pd.read_sql(query_db, conn)
searchable = busquedas[busquedas["fecha_hora"] >= busquedas.fecha_hora.max() - pd.DateOffset(days=1)]

insert_data = []
i = 0
for idx, row in searchable.iterrows():
    flag = 0 
    json_data = None
    while flag < 4:
        try:
            json_data = make_api_request(row["busqueda_real"])
            flag = 4
        except:
            flag += 1
            time.sleep(1)

    if not json_data:
        print(row)
        continue

    hits = json_data["data"]["results"]
    insert_data.append(
        {
        "busqueda": row["busqueda"],
        "fecha_hora": row["fecha_hora"],
        "resultados_db": row["resultados"],
        "busqueda_real": row["busqueda_real"],
        "resultados_es": len(hits)
        }
    )
    i += 1
    if (i > 0 and i % 1000 == 0):
        statement = """INSERT INTO BI.metricas.buscador_elastic (busqueda, busqueda_real, fecha_hora, resultados_bd, resultados_es) VALUES (%s, %s, %s, %s, %s)"""
        data = list(map(lambda x: [x["busqueda"], x["busqueda_real"], x["fecha_hora"], x["resultados_db"], x["resultados_es"]], insert_data))
        cursor.executemany(statement, data)
        i = 0
        insert_data = []

statement = """INSERT INTO BI.metricas.buscador_elastic (busqueda, busqueda_real, fecha_hora, resultados_bd, resultados_es) VALUES (%s, %s, %s, %s, %s)"""
data = list(map(lambda x: (x["busqueda"], x["busqueda_real"], x["fecha_hora"], x["resultados_db"], x["resultados_es"]), insert_data))
cursor.executemany(statement, data)
conn.commit()
cursor.close()
conn.close()
