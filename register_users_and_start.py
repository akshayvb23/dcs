import os
import threading

import pika
import json
import email_address_generator
import mail_sending_client


def register_user(username, password, ch):
    login = {'username': str(username), 'password': password}
    login_info = json.dumps(login)
    print("sending request for " + email_id)
    ch.basic_publish(exchange='',
                     routing_key='RegisterQ',
                     body=login_info)


def register_callback(self, ch, method, properties, body):
    print(body)


if __name__ == "__main__":
    if os.path.isfile('email_list'):
        os.remove('email_list')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    """ generate_random_emails(number_of_emails, length_of_username 
    and save to email_list file """
    email_list = email_address_generator.generate_random_emails(2, 7)
    file_handle = open("email_list", 'w')
    default_password = "password"
    for email_id in email_list:
        # Don't really need to write to the file but doing so for future use
        file_handle.write(email_id + '\n')
        register_user(email_id, default_password, channel)
    file_handle.close()
    thread_list = []
    for sender in email_list:
        t = threading.Thread(target=mail_sending_client.start_sender, args=(email_list[0], email_list,))
        thread_list.append(t)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    print("phew done ! ")

