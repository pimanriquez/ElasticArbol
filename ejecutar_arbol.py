import os
import pymssql
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

server   = os.getenv("SQL_SERVER")
user     = os.getenv("SQL_USER")
password = os.getenv("SQL_PASSWORD")
database = os.getenv("SQL_DATABASE")

conn = pymssql.connect(server=server, user=user, password=password, database=database)  

def leer_query(filename):
    return open(f"./queries/{filename}", 'r').read()

busquedas_query = leer_query("busquedas_comparacion.sql")
busquedas = pd.read_sql(busquedas_query, conn)

busquedas["busqueda_mod"] = busquedas["busqueda_real"]
busquedas["busqueda_mod"] = busquedas["busqueda_mod"].str.replace("0", "O").str.upper().str.replace(r'\s+', ' ', regex=True).str.replace("-", "").str.strip()
busquedas["busqueda"] = busquedas["busqueda"].str.replace("0", "O").str.upper().str.replace(r'\s+', ' ', regex=True).str.strip()

exacto_encontrados = busquedas[(busquedas["busqueda"] == busquedas["busqueda_mod"]) & (busquedas["resultados_bd"] > 0)]
exacto_no_encontrados = busquedas[(busquedas["busqueda"] != busquedas["busqueda_mod"]) | (busquedas["resultados_bd"] == 0)]

diccionario_encontrados = exacto_no_encontrados[(exacto_no_encontrados["busqueda"] != exacto_no_encontrados["busqueda_mod"]) & (exacto_no_encontrados["resultados_bd"] > 0)]
diccionario_no_encontrados = exacto_no_encontrados[(exacto_no_encontrados["busqueda"] == exacto_no_encontrados["busqueda_mod"]) | (exacto_no_encontrados["resultados_bd"] == 0)]

elastic_encontrados = diccionario_no_encontrados[(diccionario_no_encontrados["resultados_es"] > 0)]
elastic_no_encontrados = diccionario_no_encontrados[(diccionario_no_encontrados["resultados_es"] == 0)]

es_encontrado = busquedas[busquedas['resultados_es'] == 0]
bd_encontrado = busquedas[busquedas['resultados_bd'] == 0]

# Resultados:
print(f"""
Resultados árbol
Total busqueda: {len(busquedas)}
    Encontrado (exacto): \t{len(exacto_encontrados)}, {int(len(exacto_encontrados) * 1000 / len(busquedas)) / 10}
    No encontrado (exacto): \t{len(exacto_no_encontrados)}, {int(len(exacto_no_encontrados) * 1000 / len(busquedas)) / 10}
        Encontrado (diccionario):\t{len(diccionario_encontrados)}, {int(len(diccionario_encontrados) * 1000 / len(busquedas)) / 10}
        No encontrado (diccionario):\t{len(diccionario_no_encontrados)}, {int(len(diccionario_no_encontrados) * 1000 / len(busquedas)) / 10}
            Encontrado (elastic): \t\t{len(elastic_encontrados)}, {int(len(elastic_encontrados) * 1000 / len(busquedas)) / 10}
            No encontrado (elastic): \t\t{len(elastic_no_encontrados)}, {int(len(elastic_no_encontrados) * 1000 / len(busquedas)) / 10}
""")
print(f"""
Encontrados SP: \t{len(busquedas[busquedas['resultados_bd'] > 0])}
NO encontrados SP: \t{len(busquedas[busquedas['resultados_bd'] == 0])}, {int(len(busquedas[busquedas['resultados_bd'] == 0]) * 1000 / len(busquedas)) / 10}%
Encontrados ES: \t{len(busquedas[busquedas['resultados_es'] > 0])}
No encontrados ES: \t{len(busquedas[busquedas['resultados_es'] == 0])}, {int(len(busquedas[busquedas['resultados_es'] == 0]) * 1000 / len(busquedas)) / 10}%

Diferencia de 0res: \t{len(bd_encontrado) - len(es_encontrado)}
""")