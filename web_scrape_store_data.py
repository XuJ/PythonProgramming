## 1 ##
from urllib.request import urlopen
from urllib.request import urlretrieve

from bs4 import BeautifulSoup

html = urlopen('http://pythonscraping.com')
bsObj = BeautifulSoup(html)
imageLocation = bsObj.find('a', {
    'id': 'logo'
}).find('img')['src']
urlretrieve(imageLocation, 'logo.jpg')


## 2 ##
import os
import string
from urllib.request import urlopen
from urllib.request import urlretrieve

from bs4 import BeautifulSoup

downloadDirectory = 'downloaded'
baseUrl = 'http://pythonscraping.com'


def getAbsoluteURL(baseUrl, source):
    if source.startswith('http://www.'):
        url = 'http://' + source[11:]
    elif source.startswith('http://'):
        url = source
    elif source.startswith('www.'):
        url = 'http://' + source[4:]
    else:
        url = baseUrl + '/' + source
    if baseUrl not in url:
        return None
    return url


def getDownloadPath(baseUrl, absoluteUrl, downloadDirectory):
    path = absoluteUrl.replace('www.', '')
    path = path.replace(baseUrl, '')
    path = makeSafeFilename(path)
    path = downloadDirectory + path
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    return path


def makeSafeFilename(s):
    valid_chars = "/-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.replace(' ', '_')
    return filename


html = urlopen('http://www.pythonscraping.com')
bsObj = BeautifulSoup(html)
downloadList = bsObj.findAll(src=True)

for download in downloadList:
    fileUrl = getAbsoluteURL(baseUrl, download['src'])
    if fileUrl is not None:
        print(fileUrl)
        urlretrieve(fileUrl, getDownloadPath(baseUrl, fileUrl, downloadDirectory))

## 3 ##
import csv
from urllib.request import urlopen
from bs4 import BeautifulSoup

html = urlopen('http://en.wikipedia.org/wiki/Comparison_of_text_editors')
bsObj = BeautifulSoup(html)
# the main comparison table is currently the first table on the page
table = bsObj.findAll('table', {
    'class': 'wikitable'
})[0]
rows = table.findAll('tr')

csvFile = open('files/editors.csv', 'wt', encoding='utf8')
writer = csv.writer(csvFile)
try:
    for row in rows:
        csvRow = []
        for cell in row.findAll(['td', 'th']):
            csvRow.append(cell.get_text())
        print(csvRow)
        writer.writerow(csvRow)
finally:
    csvFile.close()

## 4 ##
import pymysql

conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root', passwd=None, db='mysql')
cur = conn.cursor()
cur.execute('USE scraping')
cur.execute('SELECT * FROM pages WHERE id=1')
print(cur.fetchone())
cur.close()
conn.close()

## 5 ##
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime
import random
import pymysql
import re

conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root', passwd=None, db='mysql',
                       charset='utf8')
cur = conn.cursor()
cur.execute('USE scraping')

random.seed(datetime.datetime.now())


def store(title, content):
    cur.execute('INSERT INTO pages (title, content) VALUES (\'%s\', \'%s\')', (title, content))
    cur.connection.commit()


def getLinks(aritcleUrl):
    html = urlopen('http://en.wikipedia.org' + aritcleUrl)
    bsObj = BeautifulSoup(html)
    title = bsObj.find('h1').find('span').get_text()
    content = bsObj.find('div', {
        'id': 'mw-content-text'
    }).find('p').get_text()
    store(title, content)
    return bsObj.find('div', {
        'id': 'bodyContent'
    }).findAll('a', href=re.compile('^(/wiki/)((?!:).)*$'))


links = getLinks('/wiki/Kevin_Bacon')
try:
    while len(links) > 0:
        newArticle = links[random.randint(0, len(links) - 1)].attrs['href']
        print(newArticle)
        links = getLinks(newArticle)
finally:
    cur.close()
    conn.close()
