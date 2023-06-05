#%%
import os
import pandas as pd 
import pymssql
from dotenv import load_dotenv
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

conn = pymssql.connect(server=server, user=user, password=password, database=database)  
cursor = conn.cursor()

# Búsquedas del servidor 75
# Base de datos
busquedasBD = open(f"./queries/metrics/busquedas_bd_75.sql", 'r').read()
resBD = pd.read_sql(busquedasBD, conn)

# Elastic
busquedasElastic = open(f"./queries/metrics/busquedas_elastic_75.sql", 'r').read()
resElastic = pd.read_sql(busquedasElastic, conn)

# Búsquedas del servidor 70
# Cantidad de resultados por búsqueda para base de datos y elastic
busquedasCant = open(f"./queries/metrics/busquedas_cantidades_70.sql", 'r').read()
resCant = pd.read_sql(busquedasCant, conn_70)

# Productos con sus aplicaciones y modelos
busquedasNumAp = open(f"./queries/metrics/numeros_aplicaciones_70.sql", 'r').read()
resNumAp = pd.read_sql(busquedasNumAp, conn_70)

tblBd = resBD.merge(resNumAp, on=["numero_refax", "id_aplicacion"], how="outer")
tblElastic = resElastic.merge(resNumAp, on=["numero_refax", "id_aplicacion"], how="outer")

tblBd["concatenacion"] = tblBd["nombre_producto"].astype(str) + " " + tblBd["modelo"].astype(str)
tblElastic["concatenacion"] = tblElastic["nombre_producto"].astype(str) + " " + tblElastic["modelo"].astype(str)

# Convertir los números refax, aplicaciones y concatenaciones a lista
tblBd = tblBd.groupby("termino_busqueda").agg({"numero_refax": lambda x: list(x), "id_aplicacion": lambda x: list(x), "concatenacion": lambda x: list(x)})
tblElastic = tblElastic.groupby("termino_busqueda").agg({"numero_refax": lambda x: list(x), "id_aplicacion": lambda x: list(x), "concatenacion": lambda x: list(x)})

tblBd = tblBd.rename(columns={"numero_refax": "numeros_refax_bd", "id_aplicacion": "ids_aplicacion_bd", "concatenacion": "concatenaciones_bd"})
tblElastic = tblElastic.rename(columns={"numero_refax": "numeros_refax_elastic", "id_aplicacion": "ids_aplicacion_elastic", "concatenacion": "concatenaciones_elastic"})

merge_ = tblBd.merge(tblElastic, how="outer", on="termino_busqueda")

# Resultados
merge_ = merge_.merge(resCant, how="outer", on="termino_busqueda")
mejora = merge_[(merge_['cnt_resultados_bd'] == 0) & (merge_['ctn_resultados_elastic'] > 0)]
no_mejora = merge_[(merge_['cnt_resultados_bd'] > 0) & (merge_['ctn_resultados_elastic'] == 0)]

merge_.to_excel("./salida/busquedas.xlsx", sheet_name='busquedas')
mejora.to_excel("./salida/mejora.xlsx", sheet_name='Elastic')
no_mejora.to_excel("./salida/no mejora.xlsx", sheet_name='Base datos')