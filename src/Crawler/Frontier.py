class Frontier:
    def __init__(self):
        self.urls = []

    def is_empty(self):
        return len(self.urls) == 0

    def update(self, new_urls):
        self.urls += new_urls

    def add_url(self, newurl, inlinks):
        inserted = False
        if newurl in inlinks.keys():
            new_in = len(inlinks[newurl.url])
            for i in range(len(self.urls)):
                if new_in <= len(inlinks[self.urls[i].url]):
                    self.urls.insert(i, newurl)
                    inserted = True
                    break
        if not inserted:
            self.urls.append(newurl)

    def pop(self):
        return self.urls.pop()

    def add_url_nopriority(self, newurl):
        self.urls.append(newurl)

    def pop_priority(self, inlinks):
        temp = []
        for i in self.urls:
            new_in = len(inlinks[i.url])
            inserted = False
            for j in range(len(temp)):
                if new_in <= len(inlinks[temp[j].url]):
                    temp.insert(j, i)
                    inserted = True
                    break
            if not inserted:
                temp.append(i)
        popped_item = temp.pop()
        self.urls = temp
        return popped_item

    def clear(self):
        self.urls = []