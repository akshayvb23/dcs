import pika
import json

user_email = "akshay@uci.edu"
request = {"user_email": user_email, "query_type": "RECEIVED"}


def response_callback(ch, method, properties, body):
    print("getting the most recent mailbox:")
    print(body)


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
# A queue to send the mail query response
channel.queue_declare(queue='ResponseQ', durable=True)
channel.basic_publish(exchange='',
                      routing_key='RequestQ',
                      body=json.dumps(request))

channel.basic_consume(queue='ResponseQ',
                      auto_ack=True,
                      on_message_callback=response_callback)
channel.start_consuming()
