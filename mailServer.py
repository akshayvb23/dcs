import pika
import json
import uuid
import os.path
from os import path
from email.parser import BytesParser, Parser
from email.policy import default

sentMap = {}
recievedMap = {}
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
META_DATA = "metadata"


def send_callback(ch, method, properties, body):
    receivedEmail = json.loads(body)
    sender = receivedEmail.get('sender')
    receiver = receivedEmail.get('receipt')
    email_subject = receivedEmail.get('subject')
    print("[From]:" + sender)
    print("[Subject]" + email_subject)
    print("[Recipient]:" + receiver)
    # print(receivedEmail.get('message'))
    ''' Create a new file '''
    file_name = "eml_" + str(uuid.uuid1())
    f = open(file_name, "a")
    f1 = open(META_DATA, "a")
    if sender not in sentMap:
        sentMap[sender] = [file_name]
    else:
        sentMap[sender].append(file_name)
    if receiver not in recievedMap:
        recievedMap[receiver] = file_name
    else:
        recievedMap[receiver].append(file_name)

    # write the sender receiver information to persistent memory
    f1.writelines(sender + ":" + receiver + ":" + file_name)
    f1.close()
    # store email in file
    f.write("[From]:" + sender)
    f.write("[Recipient]:" + receiver)
    f.write("[Email Subject]:" + email_subject)
    f.write(receivedEmail.get('message'))
    f.close()


def request_callback(ch, method, properties, body):
    ''' Query types
        SENT: Get a list of all emails sent by the user
        RECEIPT: Get a list of all emails received by the user
    '''

    request = json.loads(body)
    user = request.get('user_email')
    query_type = request.get('query_type')
    message = ""
    if query_type == "SENT":
        if user in sentMap:
            for file in sentMap[user]:
                f = open(file, 'r')
                message = f.read()
        else:
            message = 'Sorry! no mails sent by this user'
    elif query_type == 'RECEIVED':
        print("Received request from user " + user)
        if user in recievedMap:
            for file in recievedMap[user]:
                f = open(file, 'r')
                message = f.read()
        else:
            message = "Sorry! no emails received by this user"
    channel.basic_publish(exchange='',
                          routing_key='ResponseQ',
                          body=message)


''' This is a function to load all the information about the users 
to allow querying the files during runtime without writing to the file '''


def update_local_cache_from_meta_data():
    if path.exists(META_DATA) is False:
        return

    with open(META_DATA) as f:
        lines = f.readlines()
    for line in lines:
        if line is not "":
            data_item_list = line.split(":")
            sender = data_item_list[0]
            receiver = data_item_list[1]
            file_name = data_item_list[2]
            if sender not in sentMap:
                sentMap[sender] = [file_name]
            else:
                sentMap[sender].append(file_name)
            if receiver not in recievedMap:
                recievedMap[receiver] = [file_name]
            else:
                recievedMap[receiver].append(file_name)
        print(recievedMap)


if __name__ == "__main__":
    # TODO: create a persistent data for the mapping of sender and receiver to the file name
    update_local_cache_from_meta_data()
    channel.queue_declare(queue='MailQ', durable=True)
    # A queue to receive mailbox access request
    channel.queue_declare(queue='RequestQ', durable=True)
    # A queue to send the mail query response
    channel.queue_declare(queue='ResponseQ', durable=True)

    channel.basic_consume(queue='RequestQ',
                          auto_ack=True,
                          on_message_callback=request_callback)

    channel.basic_consume(queue='MailQ',
                          auto_ack=True,
                          on_message_callback=send_callback)
    channel.start_consuming()
