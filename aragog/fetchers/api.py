import logging

import requests

from aragog.fetchers.base import Fetcher

_LOG = logging.getLogger(__name__)


class DRFFetcher(Fetcher):
    def __init__(self, url, credentials=None):
        self.credentials = credentials
        self.total = None
        self.next_url = url
        self.current_iterable = None
        super(DRFFetcher, self).__init__(url)

    def get_next_data(self):
        _LOG.info('fetching url:[%s]', self.next_url)
        response = requests.get(self.next_url, auth=self.credentials)
        response.raise_for_status()
        data = response.json()
        self.next_url = data.get('next')
        if self.total is None:
            self.total = data['count']
        return iter(data['results'])

    def next(self):
        if self.current_iterable is None:
            self.current_iterable = self.get_next_data()
        try:
            return next(self.current_iterable)
        except StopIteration as ex:
            if self.next_url:
                self.current_iterable = self.get_next_data()
                return next(self.current_iterable)
            else:
                raise ex


class GenericAPIFetcher(Fetcher):
    def __init__(self, url, credentials=None):
        self.credentials = credentials
        self.url = url
        self.iterable = None
        super(GenericAPIFetcher, self).__init__(url)

    def __iter__(self):
        _LOG.info('fetching url:[%s]', self.url)
        response = requests.get(self.url, auth=self.credentials)
        response.raise_for_status()
        data = response.json()
        self.iterable = iter(data)
        return self

    def next(self):
        return next(self.iterable)
