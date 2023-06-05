import requests
import xmltodict, json


def make_api_request(query):
    url = "http://localhost:5000/searchB2B"
    headers = {
    'Content-Type': 'application/json'
    }
    json_data = {"query": query, "documents": False, "id_usuario": "Test", "id_cliente": "Test"}
    response = requests.request("POST", url=url, headers=headers, json=json_data)
    json_response = response.json()
    tree = xmltodict.parse(json_response["data"]["results"])
    return tree["busqueda"]

json_data = make_api_request("RADIADOR GREAT WALL ")
hits = json_data["fila"] if json_data else []
print(hits)