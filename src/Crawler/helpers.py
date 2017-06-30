import re
from urllib import robotparser

import requests

from src.Crawler.HTMLUtils import HTMLUtils
from src.Crawler.Spider import Spider
from src.Crawler.common import keyword_list, FILE_TYPES

utils = HTMLUtils()


def linkCheck(url):
    try:
        r = requests.get(url)
        return r.status_code == 200 and 'text/html' in r.headers['Content-Type']
    except:
        print("Request error.")
        return False

def crawlAllowed(url):
    robot_path = utils.getDomain(url) + "/robots.txt"
    robots = robotparser.RobotFileParser()
    robots.set_url(robot_path)
    robots.read()
    reply = robots.can_fetch("*", url)
    print("Robots say {} for {}".format(reply, url))
    return reply


def crawlPage(linkobj, depth):
    url = linkobj['href']
    title = None
    relevant = False

    for doctype in FILE_TYPES:
        if doctype in url:
            return None

    try:
        title = linkobj['title'].lower()
        for keyword in keyword_list:
            if keyword in title:
                relevant = True
                break

    except Exception:
        print("No title exception while crawl: ", linkobj)

    if relevant and crawlAllowed(url) and linkCheck(url):
        print("Crawled page ", url, title)
        return Spider(url, depth, title)

    return None
