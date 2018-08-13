import smtplib
import time
from email.mime.text import MIMEText
from urllib.request import urlopen

from bs4 import BeautifulSoup


def sendMail(subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = 'christmas_alerts@pythonscraping.com'
    msg['To'] = 'ryan@pythonscraping.com'

    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()


bsObj = BeautifulSoup(urlopen('https://isitchristmas.com'))
while (bsObj.find('a', {
    'id': 'answer'
}).attrs['title'] == 'NO'):
    print('it is not Christmas yet.')
    time.sleep(3600)
sendMail('it is Christmas!')
