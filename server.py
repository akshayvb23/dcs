from __future__ import print_function
from datetime import datetime
import asyncore
from smtpd import SMTPServer
import subprocess

def execute_shell_command(command):
    print("Executing command: " + str(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)

    while True:
        output = process.stdout.readline()
        print(output.strip())
        # Do something else
        return_code = process.poll()
        if return_code is not None:
            print('RETURN CODE', return_code)
            # Process has finished, read rest of the output 
            for output in process.stdout.readlines():
                print(output.strip())
            break

class EmlServer(SMTPServer):
    num = 0
    def process_message(self, peer, sender_username, rcpttos, data, mail_options=None, rcpt_options=None):
        filename = '%s-%d.eml' % (datetime.now().strftime('%Y%m%d%H%M%S'), self.num)
        f = open(filename, 'w')
        f.write(str(data))
        f.close()
        print('%s saved.' % filename)
        self.num += 1
        hdfs_user_dir = "/var/mail/" + sender_username
        command = ['bin/hdfs', 'dfs', '-mkdir', hdfs_user_dir]
        execute_shell_command(command)


def run():
    foo = EmlServer(('localhost', 1025), None)
    try:
        asyncore.loop()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    run()
