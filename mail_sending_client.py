import sys

import pika
import time
import json
import generate_random_string
import email_address_generator
from email.message import EmailMessage
import client_constants

send_sizes = [10, 20]
# send_sizes = [10, 20, 30, 40, 50, 100, 200, 500, 1000, 100000, 1000000]
SEND_COUNT = 0

def send_email(sender, receiver, subject_line, message_body, channel, queue):
    email = {"sender": sender, "receipt": receiver, "subject": subject_line, "message": message_body}
    jsonEmail = json.dumps(email)
    channel.basic_publish(exchange='', routing_key=queue, body=jsonEmail)
    print("[x] sent out an email")


'''This is started from the register thread'''


def start_sender(sender_email_id, send_list, channel, num_servers):
    # TODO: get a host list from the config file
    # connection = pika.BlockingConnection(pika.ConnectionParameters('34.94.60.242', 5672, "/", pika.PlainCredentials('rabbit','1')))
    # channel = connection.channel()
    # get the sender email from the command line argument
    sender = sender_email_id
    for receiver in send_list:
        for size in send_sizes:
            if receiver is sender_email_id:
                continue
            else:
                global SEND_COUNT
                subject = generate_random_string.randomString()
                message_body = generate_random_string.randomString(size)
                # TODO: select a channel list in a round-robin manner
                queue = client_constants.MAIL_QUEUE + str((SEND_COUNT % num_servers) + 1)
                print("\nSending mail (sender: " + sender + " and receiver: " + receiver + ") through queue " + queue)
                send_email(sender, receiver, subject, message_body, channel, queue)
                SEND_COUNT += 1
