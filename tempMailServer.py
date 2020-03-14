import pika
import json
import time
import glob
import os
import subprocess
from email.parser import BytesParser, Parser
from email.policy import default

ROOT_MAIL_DIRECTORY = "/var/mail"
SEPERATOR = "--------------------"

def execute_shell_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
    output = process.stdout.readline()
    return_code = process.poll()
    if return_code == 0:
        return True
    return False


class HDFS:
    def check_if_directory_exists(self, directory):
        command = ['bin/hdfs', 'dfs', '-test', '-d', directory]
        return execute_shell_command(command)

    def create_directory(self, directory):
        command = ['bin/hdfs', 'dfs', '-mkdir', '-p', directory]
        return execute_shell_command(command)

    def store_file(self, filename, destination_directory):
        command = ['bin/hdfs', 'dfs', '-put', filename, destination_directory]
        return execute_shell_command(command)

    def copy_mail_directory_to_local_storage(self, source_directory, destination_directory):
        command = ['bin/hdfs', 'dfs', '-copyToLocal', source_directory, destination_directory]
        return execute_shell_command(command)


class EmailServer:

    def __init__(self):
        self.hdfs = HDFS()

    def start(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = connection.channel()
        self.channel.queue_declare(queue='MailQ', durable=True)
        # A queue to receive mailbox access request
        self.channel.queue_declare(queue='RequestQ', durable=True)
        # A queue to send the mail query response
        self.channel.queue_declare(queue='ResponseQ', durable=True)
        # A queue to receive registartion requests
        self.channel.queue_declare(queue='RegisterQ', durable=True)

    def run(self):
        self.channel.basic_consume(queue='RegisterQ', auto_ack=True, on_message_callback=self.register_callback)
        self.channel.basic_consume(queue='RequestQ', auto_ack=True, on_message_callback=self.request_callback)
        self.channel.basic_consume(queue='MailQ', auto_ack=True, on_message_callback=self.send_callback)
        self.channel.start_consuming()

    def create_sender_directory_for_receiver_if_not_exists(self, sender, receiver):
        sender_directory = ROOT_MAIL_DIRECTORY + receiver + "/" + sender
        if self.hdfs.check_if_directory_exists(sender_directory):
            return
        self.hdfs.create_directory(sender_directory)

    def store_mail_in_receiver_box_in_hdfs(self, sender, receiver, filename):
        received_mail_directory = ROOT_MAIL_DIRECTORY + "/" + receiver + "/received/" + sender
        self.hdfs.store_file(filename, received_mail_directory)

    def store_mail_in_sender_box_in_hdfs(self, sender, filename):
        sent_mail_directory = ROOT_MAIL_DIRECTORY + "/" + sender + "/sent/"
        self.hdfs.store_file(filename, sent_mail_directory)

    def retrieve_received_mails_from_hdfs(self, receiver, sender = ""):
        mail_directory = ROOT_MAIL_DIRECTORY + "/" + receiver + "/received/" + sender
        destination = "/var/tmp/" + receiver +  "/received/" + sender
        if not os.path.isdir(destination):
            os.path.mkdir(destination)
        self.copy_mail_directory_to_local_storage(mail_directory, destination)
        return destination

    def retrieve_sent_mails_from_hdfs(self, sender):
        mail_directory = ROOT_MAIL_DIRECTORY + "/" + sender + "/sent/"
        destination = "/var/tmp/" + sender +  "/sent/"
        if not os.path.isdir(destination):
            os.path.mkdir(destination)
        self.copy_mail_directory_to_local_storage(mail_directory, destination)
        return destination

    def construct_mailbox(self, mail_directory):
        mailbox = ""
        file_list = glob.glob("*")
        for file in file_list:
            with open(file, "rb") as file_handle:
                mailbox += file_handle.read() + "\n" + SEPERATOR
        return mailbox

    def register_callback(self, ch, method, properties, body):
        # TODO: directory creation and storing authentication info
        pass

    def send_callback(self, ch, method, properties, body):

        """
        This method is called when a mail is received by the mail server
        """

        receivedEmail = json.loads(body)
        sender = receivedEmail.get('sender')
        receivers = receivedEmail.get('receipt')
        email_subject = receivedEmail.get('subject')

        print("[From]:" + sender)
        print("[Subject]" + email_subject)
        print("[Recipients]:" + receivers)
        print(receivedEmail.get('message'))

        ''' Create a new file '''
        filename = "/var/tmp" + str(time.time()) + ".eml"
        file_handle = open(filename, 'w')
        file_handle.write("[From]: " + sender)
        file_handle.write("[Recipients]: " + str(receivers))
        file_handle.write("[Email Subject]: " + email_subject)
        file_handle.write("[Time]: " + strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
        file_handle.write(receivedEmail.get('message'))
        file_handle.close()

        for receiver in receivers_list:
            self.create_sender_directory_for_receiver_if_not_exists(sender, receiver)
            self.store_mail_in_receiver_box_in_hdfs(sender, receiver, filename)
        self.store_mail_in_sender_box_in_hdfs(sender, filename)
        os.remove(filename)

    def request_callback(self, ch, method, properties, body):
        ''' Query types
            SENT: Get a list of all emails sent by the user
            RECEIPT: Get a list of all emails received by the user
        '''

        request = json.loads(body)
        user = request.get('user_email')
        query_type = request.get('query_type')

        mailbox = ""
        if query_type == "SENT":
            retrieved_directory = self.retrieve_sent_mails_from_hdfs(user)
            mailbox = construct_mailbox(retrieved_directory)
        elif query_type == 'RECEIVED':
            retrieved_directory = self.retrieve_received_mails_from_hdfs(user)
            mailbox = construct_mailbox(retrieved_directory)

        self.channel.basic_publish(exchange='', routing_key='ResponseQ', body=mailbox)


if __name__ == "__main__":
    # TODO: create a persistent data for the mapping of sender and receiver to the file name
    EMAIL_SERVER = EmailServer()
    EMAIL_SERVER.start()
    EMAIL_SERVER.run()
