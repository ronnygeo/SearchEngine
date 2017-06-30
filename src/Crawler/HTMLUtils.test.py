from src.Crawler.HTMLUtils import HTMLUtils

test_cases = [
                ("http://www.example.com:80", "http://www.example.com/", "Removing port number")
                , ("http://www.example.com/%7Ehome", "http://www.example.com/~home/", "Decoding octet")
                , ("http://www.example.com", "http://www.example.com/", "Checking add trailing /")
                , ("http://www.example.com/a/./b/../c", "http://www.example.com/a/b/c/", "removing trailing dots")
                , ("http://www.example.com/a//b/", "http://www.example.com/a/b/", "Removing double slashes")
                , ("http://www.example.com/index.html", "http://www.example.com/", "Removing default pages")
                , ("http://www.example.com/a#b/c", "http://www.example.com/a/", "Removing fragments")
                # , ("http://www.example.com/a/b","../c", "http://example.com/c/", "Making absolute path from relative")
                , ("HTTP://www.exampLe.com/A/C", "http://www.example.com/a/c/", "Lowercasing stuff")
                , ("http://example.com/", "http://www.example.com/", "If url has no subdomain, add www")
                , ("https://www.example.com/", "http://www.example.com/", "Convert https and other things to http")
                , ("example.com", "http://www.example.com/", "Only base domain")
                , ("en.example.com", "http://en.example.com/", "Only base domain with server")
             ]


utils = HTMLUtils()
for test in test_cases:
    assert utils.getCanonicalURL(test[0]) == test[1], test[2]