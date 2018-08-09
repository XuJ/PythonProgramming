import datetime
import random
import re
from urllib.request import urlopen

from bs4 import BeautifulSoup

pages = set()
random.seed(datetime.datetime.now())


# Retrieves a list of all internal links found on a page
def getInternalLinks(bsObj, startingPage):
    internalLinks = []
    includeUrl = splitAddress(startingPage)[0]
    # Finds all links that begin with a '/'
    for link in bsObj.findAll('a', href=re.compile('^(/|.*' + includeUrl + ')')):
        if link.attrs['href'] is not None:
            if link.attrs['href'] not in internalLinks:
                internalLinks.append(link.attrs['href'])
    return internalLinks


# Retrieves a list of all external links found a page
def getExternalLinks(bsObj, startingPage):
    externalLinks = []
    excludeUrl = splitAddress(startingPage)[0]
    # Finds all links that start with 'http' or 'www' that do
    # not contain the current URL
    for link in bsObj.findAll('a', href=re.compile('^(http|www)((?!' + excludeUrl + ').)*$')):
        if link.attrs['href'] is not None:
            if link.attrs['href'] not in externalLinks:
                externalLinks.append(link.attrs['href'])
    return externalLinks


def splitAddress(address):
    addressParts = address.replace('http://', '').split('/')
    return addressParts


def getRandomExternalLink(startingPage):
    html = urlopen(startingPage)
    bsObj = BeautifulSoup(html)
    externalLinks = getExternalLinks(bsObj, splitAddress(startingPage)[0])
    if len(externalLinks) == 0:
        internalLinks = getInternalLinks(bsObj, startingPage)
        return getExternalLinks(internalLinks[random.randint(0, len(internalLinks) - 1)])
    else:
        return externalLinks[random.randint(0, len(externalLinks) - 1)]


def followExternalOnly(startingSite):
    externalLink = getRandomExternalLink(startingSite)
    print('random external link is: ' + externalLink)
    followExternalOnly(externalLink)


allExtLinks = set()
allIntLinks = set()


def getAllExternalLinks(siteUrl):
    html = urlopen(siteUrl)
    bsObj = BeautifulSoup(html)
    internalLinks = getInternalLinks(bsObj, siteUrl)
    externalLinks = getExternalLinks(bsObj, siteUrl)
    for link in externalLinks:
        if link not in allExtLinks:
            allExtLinks.add(link)
            print(link)
    for link in internalLinks:
        if link not in allIntLinks:
            allIntLinks.add(link)
            print('about to get link: ' + link)
            getAllExternalLinks(link)


getAllExternalLinks('http://www.pythonscraping.com')
