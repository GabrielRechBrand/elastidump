# connection.py
from elasticsearch import Elasticsearch
from config.config import ES_NODE_URL, ES_API_KEY

def get_es_client():
    return Elasticsearch(ES_NODE_URL, api_key=ES_API_KEY)
