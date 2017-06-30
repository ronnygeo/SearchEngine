import re
from urllib.parse import urlparse, unquote, urlunparse


class HTMLUtils:
    def __init__(self):
        pass

    def getCanonicalURL(self, url):
        # decode and parse url into parts
        parsedUrl = urlparse(unquote(url, encoding="UTF-8"))

        # if scheme is empty get domain from path
        if parsedUrl.scheme == "":
            netloc = parsedUrl.path.lower()
            path = ""
            scheme = "http"
        else:
            netloc = parsedUrl.netloc.lower()
            path = parsedUrl.path
            scheme = parsedUrl.scheme.lower()

        domain = netloc
        # remove port from domain
        port = re.search(r':\d+$', netloc)
        if port and ((scheme == 'http' and port.group(0) == ':80') or (scheme == 'https' and port.group(0) == ':443')):
            domain = re.sub(r':\d+$', '', netloc)

        # if there is no www add www but check if there is any other subdomain
        domain_parts = (domain.split("."))
        if (len(domain_parts) == 2):
            domain_parts.insert(0, "www")
        domain = ".".join(domain_parts)

        # replace trailing slashes with one / from domain
        domain = re.sub("\/+", "", domain)
        # remove . but not from <file>.<html>
        path = re.sub("[^\w+\.\w+]\.+", "", path)
        # replace trailing slashes with one / from path
        path = re.sub("\/+", "/", path)

        return urlunparse((scheme, domain, path, "", "", ""))

    def getDomain(self, url):
        parsedUrl = urlparse(unquote(self.getCanonicalURL(url), encoding="UTF-8"))
        return urlunparse((parsedUrl.scheme, parsedUrl.netloc, "", "", "", ""))


    def getHeaders(self):
        pass