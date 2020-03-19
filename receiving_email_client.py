import pika
import json
import client_constants

REQUEST_COUNTER = 0

def response_callback(ch, method, properties, body):
    print("\nThe fetched mailbox is as follows: ")
    print(str(body.decode("utf-8")))


def start_client(user, num_servers, process_id):
    global REQUEST_COUNTER
    print("\nSending a request to the mail server to fetch the mailbox for user " + user)
    connection = pika.BlockingConnection(
    pika.ConnectionParameters('10.168.0.2', 5672, "/", pika.PlainCredentials('rabbit', '1')))
    channel = connection.channel()
    request_queue = client_constants.REQUEST_QUEUE + str((REQUEST_COUNTER % num_servers) + 1)
    response_queue = client_constants.RESPONSE_QUEUE + str(process_id)
    request = {"user_email": user, "query_type": "RECEIVED", "response_queue": response_queue}
    channel.queue_declare(queue=response_queue, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=request_queue,
                          body=json.dumps(request))
    REQUEST_COUNTER += 1
    channel.basic_consume(queue=response_queue,
                          auto_ack=True,
                          on_message_callback=response_callback)
    channel.start_consuming()
