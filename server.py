from __future__ import print_function
import time
import os
import asyncore
from smtpd import SMTPServer
import subprocess

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

class EmailServer(SMTPServer):

    hdfs = HDFS()
    def create_sender_directory_for_receiver_if_not_exists(self, sender, receiver):
        sender_directory = "/var/mail/" + receiver + "/" + sender
        if self.hdfs.check_if_directory_exists(sender_directory):
            return
        self.hdfs.create_directory(sender_directory)

    def store_mail_in_hdfs(self, sender, receiver, filename):
        mail_directory = "/var/mail/" + receiver + "/" + sender
        self.hdfs.store_file(filename, mail_directory)

    def process_message(self, peer, sender, receivers_list, data, mail_options=None, rcpt_options=None):
        filename = str(time.time()) + ".eml"
        file_handle = open(filename, 'w')
        file_handle.write(str(data))
        file_handle.close()
        for receiver in receivers_list:
            self.create_sender_directory_for_receiver_if_not_exists(sender, receiver)
            self.store_mail_in_hdfs(sender, receiver, filename)
        os.remove(filename)

        
def run():
    foo = EmailServer(('localhost', 1025), None)
    try:
        asyncore.loop()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    run()

