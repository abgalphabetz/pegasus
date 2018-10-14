# -*- coding: utf-8 -*-
from datetime import datetime, date, timedelta
import re

import scrapy
from scrapy_splash import SplashRequest


class BruteForceLinksSpider(scrapy.Spider):
    name = 'bruteforce-links'
    allowed_domains = ['racing.hkjc.com']
    url_base = "http://racing.hkjc.com/racing/information"
    url_path = "/{0}/Racing/LocalResults.aspx?RaceDate={1}"
    start_url_base = url_base + url_path
    url_pattern = re.compile(".*RaceDate=(.{10}).*")

    def start_requests(self):
        lang = self.lang
        year = self.year

        race_date = date(int(year), 9, 1)
        start_date_plus_one_year = race_date + timedelta(days=330)

        while race_date < start_date_plus_one_year:
            race_date_url = BruteForceLinksSpider.start_url_base.format(lang, "{:%Y/%m/%d}".format(race_date))
            yield self._splash_request(race_date_url)
            race_date += timedelta(days=1)

    def parse(self, response):
        if response.css('div.localResults'):
            race_date = self.race_date_from_url(response.url)
            yield {race_date: response.url}
            links = response.css('table.js_racecard tbody tr td a').xpath('@href').re('.*RaceNo=.*')
            for link in links:
                yield {race_date: response.urljoin(link)}

    def _splash_request(self, link):
        return SplashRequest(link, self.parse, endpoint='render.html', args={'wait': 15, 'html': 1, 'images': 0, 'timeout': 60})

    def race_date_from_url(self, url):
        m = BruteForceLinksSpider.url_pattern.match(url)
        return m.groups()[0]
