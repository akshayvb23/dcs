import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

msg = MIMEMultipart()
msg['From'] = 'abc@cs230.project'
msg['To'] = 'def@cs230.project'
msg['Subject'] = 'DCS Project'
message = 'Testing for DCS Project'
msg.attach(MIMEText(message))

mailserver = smtplib.SMTP('localhost', 1025)
# identify ourselves to smtp gmail client
#mailserver.ehlo()
# secure our email with tls encryption
#mailserver.starttls()
# re-identify ourselves as an encrypted connection
mailserver.ehlo()
#mailserver.login('me@gmail.com', 'mypassword')

mailserver.sendmail('abc@cs230.project','def@cs230.project',msg.as_string())

mailserver.quit()
