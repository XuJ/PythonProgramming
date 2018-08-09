import datetime
import json
import random
import re
from urllib.request import urlopen

from bs4 import BeautifulSoup

random.seed(datetime.datetime.now())


def getLinks(articleUrl):
    html = urlopen('http://en.wikipedia.org' + articleUrl)
    bsObj = BeautifulSoup(html)
    return bsObj.find('div', {
        'id': 'bodyContent'
        }).findAll('a', href=re.compile('^(/wiki/)((?!:).)*$'))


def getHistoryIPs(pageUrl):
    # format of revision history page is:
    # http://en.wikipedia.org/w/index.php?title=Title_in_URL&action=history
    pageUrl = pageUrl.replace('/wiki/', '')
    historyUrl = 'http://en.wikipedia.org/w/index.php?title=' + pageUrl + '&action=history'
    print('history url is: ' + historyUrl)
    html = urlopen(historyUrl)
    bsObj = BeautifulSoup(html)
    # find only the links with class 'mw-anonuserlink' which has IP addresses
    # instead of username
    ipAddresses = bsObj.findAll('a', {
        'class': 'mw-anonuserlink'
        })
    addressList = set()
    for ipAddress in ipAddresses:
        addressList.add(ipAddress.get_text())
    return addressList


def getCountry(ipAddress):
    response = urlopen('http://api.ipstack.com/{}?access_key={}&format=1'.format(ipAddress, access_key)).read()
    responseJson = json.loads(response)
    return responseJson.get('country_code')


links = getLinks('/wiki/Lactose_intolerance')
access_key = '58948e6b274dbcb2ac897107d56f376d'

for link in links:
    print('-' * 30)
    historyIPs = getHistoryIPs(link.attrs['href'])
    for historyIP in historyIPs:
        country = getCountry(historyIP)
        print(historyIP + ' is from ' + country)
    newLink = links[random.randint(0, len(links) - 1)].attrs['href']
    links = getLinks(newLink)
