# -*- coding: utf-8 -*-
from datetime import date, timedelta

import scrapy

from racing.context.helper import splash_request
from racing.context.settings import url_template, domain


class BruteForceLinksSpider(scrapy.Spider):
    name = 'bruteforce-links'
    allowed_domains = [domain]

    def start_requests(self):
        lang = self.lang
        year = self.year

        race_date = date(int(year), 9, 1)
        start_date_plus_one_year = race_date + timedelta(days=330)

        while race_date < start_date_plus_one_year:
            race_date_url = url_template.format(lang, race_date.strftime("%Y/%m/%d"))
            yield splash_request(self.parse, race_date_url)
            race_date += timedelta(days=1)

    def parse(self, response):
        if response.css('div.localResults'):
            yield {'link': response.url}
            links = response.css('table.js_racecard tbody tr td a').xpath('@href').re('.*RaceNo=.*')
            for link in links:
                yield {'link': response.urljoin(link)}
