import re

import requests
from bs4 import BeautifulSoup

from src.Crawler.HTMLUtils import HTMLUtils


class Spider:
    def __init__(self, url, depth, title=None):
        self.utils = HTMLUtils()
        self.request = requests.get(url)
        self.html = BeautifulSoup(self.request.text.strip(), "lxml")
        self.text = None
        self.url = url
        self.domain = None
        self.headers = self.request.headers
        self.outlinks = []
        self.depth = depth
        self.title = title
        self.getDomain()
        self.getText()
        self.getOutlinks()

    def getDomain(self):
        self.domain = self.utils.getDomain(self.url)

    def getText(self):
        for s in self.html.body(['script', 'style']):
            s.decompose()
        self.text = re.sub("[\n+\s+\t+\r+]", " ", self.html.body.text)
        if not self.title:
            self.title = self.html.head.title.text

    def getOutlinks(self):
        for link in self.html.body.find_all("a"):
            try:
                if 'image' in link.attrs['class'] or re.search(r'file:|template_?\w*:|special:\w*', link.attrs['href']):
                    continue
            except Exception:
                pass
            try:
                if "title" not in link.attrs.keys():
                    link.attrs['title'] = link.text
                if "href" in link.attrs.keys():
                    if link.attrs["href"][0] == "/":
                        link.attrs["type"] = "rel"
                        canon_link = self.utils.getCanonicalURL(self.utils.getDomain(self.url) + link.attrs["href"])
                        link.attrs["href"] = canon_link
                        self.outlinks.append(link.attrs)
                    elif "http" in link.attrs["href"]:
                        if "rel" in link.attrs.keys() and 'nofollow' in link.attrs['rel']:
                            # print("nofollow: ", link.attrs["href"])
                            continue
                        link.attrs["type"] = "abs"
                        canon_link = self.utils.getCanonicalURL(link.attrs["href"])
                        link.attrs["href"] = canon_link
                        self.outlinks.append(link.attrs)
                    elif link.attrs["href"][0] != "#":
                        pass
            except Exception:
                pass

    def prepare(self):
        self.outlinks = ",".join(map(lambda v: v['href'], self.outlinks))

    def to_string(self):
        try:
            st = "<DOC>\n"
            st += "<DOCNO>\n{}\n</DOCNO>\n".format(self.url)
            st += "<TITLE>\n{}\n</TITLE>\n".format(self.title)
            st += "<DEPTH>\n{}\n</DEPTH>\n".format(self.depth)
            st += "<CONTENT>\n{}\n</CONTENT>\n".format(self.text)
            st += "<HTTPHEADER>\n{}\n</HTTPHEADER>\n".format(self.headers)
            st += "<HTMLSOURCE>\n{}\n</HTMLSOURCE>\n".format(self.html)
            st += "<OUTLINKS>\n{}\n</OUTLINKS>\n".format("\t".join(list(map(lambda o: o['href'], self.outlinks))))
            st += "</DOC>\n\n"
            return st.encode('UTF-8')
        except:
            return "\n".encode('UTF-8')

    def to_dict(self):
        return {'url': str(self.url).encode("UTF-8"), 'domain': str(self.domain).encode("UTF-8"),
            'title': str(self.title).encode("UTF-8"), 'depth': str(self.depth).encode("UTF-8"),
                 'content': str(self.text).encode("UTF-8"), 'httpheader': str(self.headers).encode("UTF-8"),
                 'htmlsource': str(self.html).encode("UTF-8"),
                 'outlinks': list(map(lambda o: str(o['href']).encode("UTF-8"), self.outlinks))}
