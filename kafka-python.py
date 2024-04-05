from confluent_kafka import Producer, Consumer, KafkaError

import time
import random
import datetime

# Simulação do sensor
class arSensor():
    def __init__(self, idSensor='sensor_001', tipoPoluente='PM2.5', nivel=35.2, timestamp='2024-04-04T12:34:56Z'):
        self.idSensor = idSensor
        self.tipoPoluente = tipoPoluente
        self.nivel = nivel
        self.timestamp = timestamp
    
    def __str__(self):
        return f'\nSensor: {self.idSensor} \n Tipo de Poluente: {self.tipoPoluente} \n nivel: {self.nivel} \n Timestamp: {self.timestamp} \n '
    

    def generateData(self):
        current_datetime = datetime.datetime.now()
        timestamp = current_datetime.timestamp()
        datetime_object = datetime.datetime.fromtimestamp(timestamp)
        self.timestamp = datetime_object

        poluente = ['PM2.5', 'PM10', 'CO2']
        self.tipoPoluente = random.choice(poluente)

        if self.tipoPoluente == 'PM2.5':
            self.nivel = round(random.triangular(15, 40, 28), 2)
        elif self.tipoPoluente == 'PM10':
            self.nivel = round(random.triangular(2, 20, 8), 2)
        elif self.tipoPoluente == 'CO2':
            self.nivel = round(random.triangular(50, 150, 95), 2)


# Configurações do produtor
producer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092,localhost:49092',
    'client.id': 'python-producer'
}

# Configurações do consumidor
consumer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092,localhost:49092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Criar produtor
producer = Producer(**producer_config)

# Função de callback para confirmação de entrega
def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Instanciar sensor
newSensor = arSensor()
newSensor.generateData()

# Enviar mensagem
topic = 'qualidadeAr'
message = str(newSensor)

producer.produce(topic, message.encode('utf-8'), callback=delivery_callback)

# Aguardar a entrega de todas as mensagens
producer.flush()

# Criar consumidor
consumer = Consumer(**consumer_config)

# Assinar tópicog
consumer.subscribe([topic])

# Consumir mensagens
try:
    while True:
        newSensor.generateData()
        # Enviar mensagem
        topic = 'qualidadeAr'
        message = str(newSensor)
        producer.produce(topic, message.encode('utf-8'), callback=delivery_callback)

        time.sleep(3)

        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f'Received message: {msg.value().decode("utf-8")}')
except KeyboardInterrupt:
    pass
finally:
    # Fechar consumidor
    consumer.close()