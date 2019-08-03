class Fetcher(object):
    def __init__(self, url):
        self.url = url

    def __iter__(self):
        return self

    def next(self):
        pass
