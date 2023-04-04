import os
import requests
import time
import pymssql
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
import xmltodict

load_dotenv()

server   = os.getenv("SQL_SERVER_75")
user     = os.getenv("SQL_USER_75")
password = os.getenv("SQL_PASSWORD_75")
database = os.getenv("SQL_DATABASE_75")

server_70   = os.getenv("SQL_SERVER")
user_70    = os.getenv("SQL_USER")
password_70 = os.getenv("SQL_PASSWORD")
database_70 = os.getenv("SQL_DATABASE")

conn = pymssql.connect(server=server, user=user, password=password, database=database)  
cursor = conn.cursor()

conn_70 = pymssql.connect(server=server_70, user=user_70, password=password_70, database=database_70)  
cursor_70 = conn_70.cursor()



def leer_query(filename):
    return open(f"./queries/{filename}", 'r').read()


def make_api_request(query):
    url = "http://172.16.101.71:5100/searchB2B"
    headers = {
    'Content-Type': 'application/json'
    }
    json_data = {"query": query, "documents": False, "id_usuario": "Test", "id_cliente": "Test"}
    response = requests.request("POST", url=url, headers=headers, json=json_data)
    json_response = response.json()
    tree = xmltodict.parse(json_response["data"]["results"])
    return tree["busqueda"]

query_db = leer_query("busquedas_db.sql")
busquedas = pd.read_sql(query_db, conn_70)
searchable = busquedas.copy()

insert_data = []
insert_p_data = []
i = 0
j = 0
for idx, row in tqdm(searchable.iterrows(), total=searchable.shape[0]):
    # j += 1
    # if j <= 1600:
    #     time.sleep(0.01)
    #     continue

    flag = 0 
    json_data = None
    while flag < 10:
        try:
            json_data = make_api_request(row["busqueda_real"])
            flag = 10
        except:
            flag += 1
            time.sleep(1)

    try:
        hits = json_data["fila"] if json_data else []
        if isinstance(hits, dict):
            hits = [hits]
    except Exception as e:
        with open("errors.txt", 'a') as error_file:
            error_file.write(f'{row["busqueda"]};_;{row["busqueda_real"]};_;{row["fecha_hora"]};_;{e}\n')
        raise e
    insert_data.append(
        {
        "busqueda": row["busqueda"] if row["busqueda"] else row["busqueda_real"],
        "fecha_hora": row["fecha_hora"],
        "busqueda_real": row["busqueda_real"],
        "resultados_es": len(hits)
        }
    )
    insert_p_data = list(map(lambda r :
        {
        "busqueda": row["busqueda"] if row["busqueda"] else row["busqueda_real"],
        "fecha_hora": row["fecha_hora"],
        "busqueda_real": row["busqueda_real"],
        "Numero_Refax": r["numero_refax"],
        "Aplicacion": r["aplicacion"] if "aplicacion" in r else 0
        },
        hits[:30]
        )
    )
    
    statement = """INSERT INTO [BI].[elastic].[Prod_Resultados_Elastic] (Fecha, Termino_Busqueda, Termino_Busqueda_Dicc, Numero_Refax, Aplicacion) VALUES (%s, %s, %s, %s, %s)"""
    data = list(map(lambda x: (x["fecha_hora"], x["busqueda_real"], x["busqueda"], x["Numero_Refax"], x["Aplicacion"]), insert_p_data))
    cursor = conn.cursor()
    cursor.executemany(statement, data)
    conn.commit()
    cursor.close()
    
    i += 1
    if (i > 0 and i % 1000 == 0):
        statement = """INSERT INTO BI.metricas.buscador_elastic (busqueda, busqueda_real, fecha_hora, resultados) VALUES (%s, %s, %s, %s)"""
        data = list(map(lambda x: (x["busqueda"], x["busqueda_real"], x["fecha_hora"], x["resultados_es"]), insert_data))
        cursor_70.executemany(statement, data)
        conn_70.commit()
        cursor_70.close()
        i = 0
        insert_data = []
        cursor_70 = conn_70.cursor()


statement = """INSERT INTO BI.metricas.buscador_elastic (busqueda, busqueda_real, fecha_hora, resultados) VALUES (%s, %s, %s, %s)"""
data = list(map(lambda x: (x["busqueda"], x["busqueda_real"], x["fecha_hora"], x["resultados_es"]), insert_data))
cursor_70.executemany(statement, data)
conn_70.commit()
cursor_70.close()
conn_70.close()
