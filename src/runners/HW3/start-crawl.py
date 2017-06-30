import datetime
import json
from collections import defaultdict
from time import sleep

import ujson

from src.Crawler.Frontier import Frontier
from src.Crawler.HTMLUtils import HTMLUtils
from src.Crawler.Spider import Spider
from src.Crawler.common import CRAWL_LIMIT, seedUrls
from src.Crawler.helpers import crawlPage
import shelve

inlinks = defaultdict(set)
outlinks = defaultdict(set)
visitedUrls = []

front1 = Frontier()
front2 = Frontier()

docs = []

utils = HTMLUtils()
depth = 0
i = 0
for seedUrl in seedUrls:
    canon_url = utils.getCanonicalURL(seedUrl)
    print(canon_url)
    seedObj = Spider(canon_url, depth)
    # front1.add_url_no(seedObj, inlinks)
    front1.add_url_nopriority(seedObj)
    docs.append(seedObj)
    i += 1
depth += 1

print("Loaded front 1 with seed.", len(front1.urls))


file_count = 0
while i < CRAWL_LIMIT and not (front1.is_empty() and front2.is_empty()):
    if not front1.is_empty():
        try:
            # current_url = front1.pop()
            current_url = front1.pop_priority(inlinks)
            for outlink in current_url.outlinks:
                if i < CRAWL_LIMIT:
                    inlinks[outlink['href'].encode("UTF-8")].add(current_url.url.encode("UTF-8"))
                    outlinks[current_url.url.encode("UTF-8")].add(outlink['href'].encode("UTF-8"))
                    start_time = datetime.datetime.now()
                    if outlink['href'] not in visitedUrls:
                        visitedUrls.append(outlink['href'])
                        newurlobj = crawlPage(outlink, depth)
                        if newurlobj:
                            front2.add_url_nopriority(newurlobj)
                            i += 1
                            print("crawled {} pages.".format(i))
                            # write result to data structure
                            docs.append(newurlobj)
                    end_time = datetime.datetime.now()
                    # offset_time = (start_time + datetime.timedelta(seconds=1)) - end_time
                    # if end_time < (start_time + datetime.timedelta(seconds=1)):
                    #     sleep(0.5)
                else:
                    break
        except Exception:
            print("Some Exception")
    else:
        print("Front 2 count: ", len(front2.urls))
        front1.update(front2.urls)
        front2.clear()
        depth += 1
        print("Loaded front 2 to front 1. Depth: ", depth)

    if len(docs) > 1000:
        with open("../../../output/dataj20/data_" + str(file_count), "wb") as out:
            data = ""
            for doc in docs:
                data += ujson.dumps(doc.to_dict(), encode_html_chars=True) + "\n"
            out.write(data.encode("UTF-8"))
        docs = []
        file_count += 1

if len(docs) > 0:
    with open("../../../output/dataj20/data_" + str(file_count), "wb") as out:
        data = ""
        for doc in docs:
            data += ujson.dumps(doc.to_dict(), encode_html_chars=True) + "\n"
        out.write(data.encode("UTF-8"))
    docs = []
    file_count += 1

print("Writing inlinks file..")
with open("../../../output/hw3/inlinksj20.map", "wb") as out:
    data = ujson.dumps(inlinks, encode_html_chars=True) + "\n"
    out.write(data.encode("UTF-8"))


print("Completed Inlinks write.")

print("Writing outlinks file..")
with open("../../../output/hw3/outlinksj20.map", "wb") as out:
    data = ujson.dumps(outlinks, encode_html_chars=True) + "\n"
    out.write(data.encode("UTF-8"))

print("Completed Outlinks write.")
