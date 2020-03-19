import pika
import json

def response_callback(ch, method, properties, body):
    print("getting the most recent mailbox:")
    print(body)


def start_client(user):
    connection = pika.BlockingConnection(
    pika.ConnectionParameters('34.94.60.242', 5672, "/", pika.PlainCredentials('rabbit', '1')))
    channel = connection.channel()
    request = {"user_email": user, "query type": "RECEIVED"}
    channel.basic_publish(exchange='',
                          routing_key='RequestQ',
                          body=json.dumps(request))
    channel.basic_consume(queue='ResponseQ',
                          auto_ack=True,
                          on_message_callback=response_callback)
    channel.start_consuming()
