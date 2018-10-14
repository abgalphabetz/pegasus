# -*- coding: utf-8 -*-
import re
from datetime import datetime, date

import scrapy

from racing.context import settings
from racing.context.helper import splash_request, race_date_from_url
from racing.context.settings import url_template


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
            if not re.search('.*RaceNo=.*', response.url):
                race_date = race_date_from_url(response.url)
                yield {race_date: response.url}
                links = response.css('table.js_racecard tbody tr td a').xpath('@href').re('.*RaceNo=.*')
                for link in links:
                    yield {race_date: response.urljoin(link)}
