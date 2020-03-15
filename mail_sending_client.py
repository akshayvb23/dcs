import sys

import pika
import time
import json
import generate_random_string
import email_address_generator
from email.message import EmailMessage

send_sizes = [10, 20]


# send_sizes = [10, 20, 30, 40, 50, 100, 200, 500, 1000, 100000, 1000000]


def send_email(sender, receiver, subject_line, message_body, channel):
    email = {"sender": sender, "receipt": receiver, "subject": subject_line, "message": message_body}
    jsonEmail = json.dumps(email)
    channel.basic_publish(exchange='',
                          routing_key='MailQ',
                          body=jsonEmail)
    print("[x] sent out an email")


def start_sender(sender_email_id, send_list):
    # TODO: get a host list from the config file
    host_list = ['localhost']
    channel_list = []
    connection = pika.BlockingConnection(pika.ConnectionParameters(host_list[0]))
    channel = connection.channel()
    channel.queue_declare(queue='receiveMailQ', durable=True)
    channel_list.append(channel)
    # List of emails the sender sends an email to
    # update_email_send_list()
    # get the sender email from the command line argument
    sender = sender_email_id
    for receiver in send_list:
        for size in send_sizes:
            subject = generate_random_string.randomString()
            message_body = generate_random_string.randomString(size)
            # TODO: select a channel list in a round-robin manner
            send_email(sender, receiver, subject, message_body, channel_list[0])
