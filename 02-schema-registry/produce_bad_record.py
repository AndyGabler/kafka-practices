from kafka import KafkaProducer

"""
Simple Python script for demo purposes to attempt to produce a bad record to the game result topic that the
 Java apps are using.
"""

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('nfl.game.result.v3', b'NOT A GOOD RECORD')
producer.flush()