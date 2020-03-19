import pika
import json

def response_callback(ch, method, properties, body):
    print("getting the most recent mailbox:")
    print(str(body.decode("utf-8")))


def start_client(user):
    print("sending a request for user "+ user)
    connection = pika.BlockingConnection(
    pika.ConnectionParameters('10.168.0.2', 5672, "/", pika.PlainCredentials('rabbit', '1')))
    channel = connection.channel()
    request = {"user_email": user, "query_type": "RECEIVED"}
    channel.basic_publish(exchange='',
                          routing_key='RequestQ',
                          body=json.dumps(request))
    channel.basic_consume(queue='ResponseQ',
                          auto_ack=True,
                          on_message_callback=response_callback)
    channel.start_consuming()
