import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer
import time
import logging

default_args = {
    'owner': 'lucas',
    'start_date': datetime(2024, 7, 29, 10, 0)
}


# Essa função faz uma requisição HTTP para a API "randomuser.me" para obter dados de um usuário
# aleatório, converte para JSON e extrai o primeiro resultado da lista
def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res['results'][0]


# Recebe os dados do usuário obtidos pela get_data
# Extrai e formata diversas informações do usuário em um dicionário data
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data


# Chama as funções get_data e format_data para obter e formatar os dados do usuário.
# Converte o dicionário de dados formatados em uma string JSON com indentação de 3 espaços e imprime no console
def stream_data():
    # Iniciar o KafkaProducer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Pegando o tempo atual em segundos
    curr_time = time.time()

    # Testando por 2 minutos o stream
    while True:
        if time.time() > curr_time + 120:  # 2 minutos
            break
        try:
            res = get_data()
            res = format_data(res)
            # Publicar e enviar os dados que obtemos da API para a fila
            producer.send('users_created', res)
            logging.info(f"Sent: {res}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
