#!/usr/bin/env python
# coding: utf-8

# In[ ]:
import sys
from shutil import copyfile

import pika
import json
import time
import glob
import os
import hashlib  # to compute hash of the password
import subprocess
import shutil

ROOT_MAIL_DIRECTORY = "/var/mail"
#ROOT_MAIL_DIRECTORY = "/Users/tanvigupta/mail"
SEPARATOR = "----------------------------------------------"


def execute_shell_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
    output = process.stdout.readline()
    return_code = process.poll()
    if return_code == 0:
        return True
    return False


class HDFS:
    def check_if_directory_exists(self, directory):
        command = ['../bin/hdfs', 'dfs', '-test', '-d', directory]
        return execute_shell_command(command)

    def create_directory(self, directory):
        command = ['../bin/hdfs', 'dfs', '-mkdir', '-p', directory]
        return execute_shell_command(command)

    def store_file(self, filename, destination_directory):
        command = ['../bin/hdfs', 'dfs', '-put', filename, destination_directory]
        return execute_shell_command(command)

    def copy_mail_directory_to_local_storage(self, source_directory, destination_directory):
        command = ['../bin/hdfs', 'dfs', '-get', source_directory, destination_directory]
        return execute_shell_command(command)


class EmailServer:

    def __init__(self):
        self.hdfs = HDFS()

    def start(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters('10.168.0.2', 5672, "/", pika.PlainCredentials('rabbit', '1')))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=MAIL_QUEUE, durable=True)
        self.channel.queue_declare(queue=REQUEST_QUEUE, durable=True)
        self.channel.queue_declare(queue=RESPONSE_QUEUE, durable=True)
        self.channel.queue_declare(queue=REGISTER_QUEUE, durable=True)

    def run(self):
        self.channel.basic_consume(queue=REGISTER_QUEUE, auto_ack=True, on_message_callback=self.register_callback)
        self.channel.basic_consume(queue=REQUEST_QUEUE, auto_ack=True, on_message_callback=self.request_callback)
        self.channel.basic_consume(queue=MAIL_QUEUE, auto_ack=True, on_message_callback=self.send_callback)
        self.channel.start_consuming()

    def create_sender_directory_for_receiver_if_not_exists(self, sender, receiver):
        sender_directory = ROOT_MAIL_DIRECTORY + "/" + receiver + "/received/" + sender + "/"
        if self.hdfs.check_if_directory_exists(sender_directory):
            return
        self.hdfs.create_directory(sender_directory)

    def store_mail_in_receiver_box_in_hdfs(self, sender, receiver, filename):
        received_mail_directory = ROOT_MAIL_DIRECTORY + "/" + receiver + "/received/" + sender + "/"
        self.hdfs.store_file(filename, received_mail_directory)

    def store_mail_in_sender_box_in_hdfs(self, sender, filename):
        sent_mail_directory = ROOT_MAIL_DIRECTORY + "/" + sender + "/sent/"
        self.hdfs.store_file(filename, sent_mail_directory)

    def retrieve_received_mails_from_hdfs(self, receiver, sender=""):
        if sender != "":
            mail_directory = ROOT_MAIL_DIRECTORY + "/" + receiver + "/received/" + sender
            destination = "/var/tmp/" + receiver + "/received/" + sender
        else:
            mail_directory =  ROOT_MAIL_DIRECTORY + "/" + receiver + "/received"
            destination = "/var/tmp/" + receiver

        if not os.path.isdir(destination):
            os.makedirs(destination, exist_ok=True)
        self.hdfs.copy_mail_directory_to_local_storage(mail_directory, destination)
        return destination + "/received"

    def retrieve_sent_mails_from_hdfs(self, sender):
        mail_directory = ROOT_MAIL_DIRECTORY + "/" + sender + "/sent/"
        destination = "/var/tmp/" + sender + "/sent/"
        if not os.path.isdir(destination):
            os.path.mkdir(destination)
        self.hdfs.copy_mail_directory_to_local_storage(mail_directory, destination)
        return destination

    def construct_mailbox(self, mail_directory):
        mailbox = ""
        file_list = glob.glob(mail_directory + "/*/*")
        for file_entry in file_list:
            with open(file_entry, "rb") as file_handle:
                mailbox += str(file_handle.read()) + "\n" + SEPARATOR + "\n"
            os.remove(file_entry)
        return mailbox

    def register_callback(self, ch, method, properties, body):
        # TODO: directory creation and storing authentication info
        try:
            register = json.loads(body)
            username = register.get('username')
            # computing hash of the password
            password = hashlib.md5(register.get('password').encode('utf-8')).hexdigest()
            print("\nReceived registration request from user " + username)

            # inbox directory
            self.hdfs.create_directory(ROOT_MAIL_DIRECTORY + '/' + username + '/sent/')
            # outbox directory
            self.hdfs.create_directory(ROOT_MAIL_DIRECTORY + '/' + username + '/received/')

            print("\nSuccessfully registered the user " + username)

        except:
            print("Received invalid json file")
            print(sys.exec_info())

    def send_callback(self, ch, method, properties, body):

        """
        This method is called when a mail is received by the mail server
        commit update : All mail is received from registered users which an inbox and an outbox
        """

        receivedEmail = json.loads(body)
        sender = receivedEmail.get('sender')
        receivers = receivedEmail.get('receipt')
        email_subject = receivedEmail.get('subject')
        email_message = str(receivedEmail.get('message').encode("utf-8"))

        print("\nEmail arrived at the server. Here is the summary of the mail:")
        print("\n[From]: " + sender)
        print("[Subject]: " + email_subject)
        print("[Recipients]: " + receivers)
        print("[Message]: " + email_message)
        print("\n")

        ''' Create a new file '''
        filename = "/var/tmp/" + str(time.time()) + ".eml"
        file_handle = open(filename, 'w')
        file_handle.write("[From]: " + sender + "\n")
        file_handle.write("[Recipients]: " + str(receivers) + "\n")
        file_handle.write("[Email Subject]: " + email_subject + "\n")
        file_handle.write("[Time]: " + time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + "\n")
        file_handle.write("[Message]: " + email_message  + "\n")
        file_handle.close()

        # Tanvi testing
        # shutil.copy(filename, ROOT_MAIL_DIRECTORY + '/' + sender + '/sent/')
        # shutil.copy(filename, ROOT_MAIL_DIRECTORY + '/' + receivers + '/received/')
        # In the case that an email is sent out to multiple receivers
        #for receiver in receivers:

        self.create_sender_directory_for_receiver_if_not_exists(sender, receivers)
        self.store_mail_in_receiver_box_in_hdfs(sender, receivers, filename)
        self.store_mail_in_sender_box_in_hdfs(sender, filename)
        print("\nSuccessfully stored the email in the HDFS")
        os.remove(filename)

    def request_callback(self, ch, method, properties, body):
        """ Query types
            SENT: Get a list of all emails sent by the user
            RECEIPT: Get a list of all emails received by the user
        """

        request = json.loads(body)
        user = request.get('user_email')
        query_type = request.get('query_type')
        
        mailbox = ""
        if query_type == "SENT":
            print("\nReceived a request to fetch the outbox for the user: " + user)
            retrieved_directory = self.retrieve_sent_mails_from_hdfs(user)
            mailbox = self.construct_mailbox(retrieved_directory)
        elif query_type == 'RECEIVED':
            print("\nReceived a request to fetch the inbox for the user: " + user)
            retrieved_directory = self.retrieve_received_mails_from_hdfs(user)
            mailbox = self.construct_mailbox(retrieved_directory)

        print("\nSending the requested mailbox back to client. Mailbox is as follows:\n")
        print(mailbox)
        self.channel.basic_publish(exchange='', routing_key='ResponseQ', body=mailbox)


if __name__ == "__main__":
    # TODO: create a persistent data for the mapping of sender and receiver to the file name

    if (len(sys.argv) <= 1):
        print("Please provide the server id as argument")

    SERVER_ID = str(sys.argv[1])
    MAIL_QUEUE = "MailQ" + SERVER_ID
    REGISTER_QUEUE = "RegisterQ" + SERVER_ID
    REQUEST_QUEUE = "RequestQ" + SERVER_ID
    RESPONSE_QUEUE = "ResponseQ"

    EMAIL_SERVER = EmailServer()
    EMAIL_SERVER.start()
    EMAIL_SERVER.run()
