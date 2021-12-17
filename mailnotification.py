import smtplib
from email.message import EmailMessage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
#import errormessages
from datetime import datetime, time
import sys
#sys.setrecursionlimit(100)
import socket
import inspect
import email.utils
from email import encoders

#define mail address below
mailaddress = "somebody@something.com"

#You can have more than one entries below
smtp_servers = {
    "<server_this_script_runs_on>": "smtp_servername"
}


def attachment(body,key,filename,html_object,recipient=mailaddress,subject="Netapp Health Check report"):
    caller_filename = inspect.stack()[1].filename
    s = smtplib.SMTP(smtp_servers[socket.getfqdn()])
    msg = MIMEMultipart('alternative')
    msg.attach(MIMEText(body))
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(html_object)
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(part)
    sender = caller_filename + ' <root@' + socket.gethostname() + '>'
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject
    msg['Date'] = email.utils.formatdate(localtime=True)
    s.sendmail(sender,recipient, msg.as_string())

#You may use the below function to send emails only during specific time windows
'''
def is_time_between(begin_time, end_time, check_time=None):
    # If check time is not given, default to current UTC time
    check_time = check_time or datetime.utcnow().time()
    if begin_time < end_time:
        return check_time >= begin_time and check_time <= end_time
    else: # crosses midnight
        return check_time >= begin_time or check_time <= end_time
'''
