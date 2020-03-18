import pika
import json

'''user_email = "akshay@uci.edu"
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
channel.start_consuming()'''

def response_callback(ch, method, properties, body):
    print("getting the most recent mailbox:")
    print(body)



def update_email_send_list():
    file_handle = open("email_list", 'w')
    email_list = []
    for line in file_handle.readlines():
        email_list.append(line)
    return email_list


def main():
    # TODO: get a host list from the config file
    email_list = update_email_send_list()
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # channel.queue_declare(queue='receiveMailQ', durable=True)
    # channel_list.append(channel)
    # List of emails the sender sends an email to

    # get the sender email from the command line argument
    for user in email_list:
        # add the code to handle both sent and received email
        request = {"user_email": user, "query type": "RECEIVED"}
        channel.basic_publish(exchange ='',
                              routing_key = 'RequestQ',
                              body = json.dumps(request))
        channel.basic_consume(queue='ResponseQ',
                              auto_ack=True,
                              on_message_callback=response_callback)
        channel.start_consuming()


