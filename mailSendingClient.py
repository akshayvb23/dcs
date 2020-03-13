import pika
import time
import json
from email.message import EmailMessage


sender = "tanvi@uci.edu"
receipt = "akshay@uci.edu"
subject = "hello there"
message = "Hello Akshay, this is Tanvi. I am trying out this thing. Lets see how it goes !"

email = {"sender": sender, "receipt": receipt, "subject": subject, "message": message}
jsonEmail = json.dumps(email)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='receiveMailQ', durable=True)

#while True:
channel.basic_publish(exchange='',
                      routing_key='MailQ',
                      body=jsonEmail)
    #time.sleep(2)
print(" [x] Sent 'email'")
connection.close()
