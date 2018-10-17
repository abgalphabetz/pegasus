# -*- coding: utf-8 -*-
from datetime import datetime, date

import scrapy

from racing.context import settings
from racing.context.helper import splash_request
from racing.context.settings import url_template, race_no_url_pattern


class SmartLinksSpider(scrapy.Spider):
    name = 'smart-links'
    allowed_domains = [settings.domain]

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.init_date_string = '2016/09/03'
        self.start_date = date(int(self.year), 9, 3)
        self.start_date_plus_one_year = date(int(self.year) + 1, 9, 1)
        self.start_date_string = self.start_date.strftime('%Y/%m/%d')

    def start_requests(self):
        yield splash_request(url_template.format(self.lang, self.init_date_string))

    def parse(self, response):
        # first parse all available race dates
        if response.url.endswith(self.init_date_string):
            dates = response.css('select#selectId option').xpath('text()').extract()
            for d in dates:
                race_date = datetime.strptime(d, '%d/%m/%Y').date()
                if race_date < self.start_date_plus_one_year:
                    yield splash_request(url_template.format(self.lang, race_date.strftime('%Y/%m/%d')))
        else:
            if not race_no_url_pattern.search(response.url):
                yield {'link': response.url}
                links = response.css('table.js_racecard tbody tr td a').xpath('@href').re('.*RaceNo=.*')
                for link in links:
                    yield {'link': response.urljoin(link)}
