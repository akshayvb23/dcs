import os
import sys
import threading

import pika
import json
import email_address_generator
import mail_sending_client
import client_constants

def register_user(username, password, ch, queue):
    login = {'username': str(username), 'password': password}
    login_info = json.dumps(login)
    print("\nSending a registration request for the user " + email_id)
    ch.basic_publish(exchange='', routing_key=queue, body=login_info)


def register_callback(self, ch, method, properties, body):
    print(body)


if __name__ == "__main__":
    if os.path.isfile('email_list'):
        os.remove('email_list')

    num_servers = int(sys.argv[1])
    register_count = 0

    #connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    connection = pika.BlockingConnection(pika.ConnectionParameters('10.168.0.2', 5672, "/", pika.PlainCredentials('rabbit','1')))
    channel = connection.channel()
    """ generate_random_emails(number_of_emails, length_of_username 
    and save to email_list file """
    email_list = email_address_generator.generate_random_emails(3, 7)
    file_handle = open("email_list", 'w')
    default_password = "password"
    for email_id in email_list:
        # Don't really need to write to the file but doing so for future use
        file_handle.write(email_id + '\n')
        queue = client_constants.REGISTER_QUEUE + str((register_count % num_servers) + 1)
        print("Reg queue for user " + email_id + " is " + queue)
        register_user(email_id, default_password, channel, queue)
        register_count += 1

    file_handle.close()
    thread_list = []
    for sender in email_list:
        print(sender)
        t = threading.Thread(target=mail_sending_client.start_sender, args=(email_list[0], email_list, channel, num_servers))
        thread_list.append(t)

    print(len(thread_list))
    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    print("phew done ! ")

