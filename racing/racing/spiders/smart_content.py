# -*- coding: utf-8 -*-
from datetime import datetime, date, timedelta
import re

import scrapy
from scrapy_splash import SplashRequest


class SmartContentSpider(scrapy.Spider):
    name = 'smart-content'
    allowed_domains = ['racing.hkjc.com']
    url_base = "http://racing.hkjc.com/racing/information"
    url_template = "http://racing.hkjc.com/racing/information/{0}/Racing/LocalResults.aspx?RaceDate={1}"
    url_pattern = re.compile(url_base + "/(.*)")

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.start_date = date(int(self.year), 9, 3)
        self.start_date_plus_one_year = date(int(self.year)+1, 9, 1)
        self.start_date_string = self.start_date.strftime('%Y/%m/%d')

    def start_requests(self):
        yield self._splash_request(SmartContentSpider.url_template.format(self.lang, self.start_date_string))

    def parse(self, response):
        # first parse all available race dates
        if response.url.endswith(self.start_date_string):
            dates = response.css('select#selectId option').xpath('text()').extract()
            for d in dates:
                race_date = datetime.strptime(d, '%d/%m/%Y').date()
                if race_date < self.start_date_plus_one_year:
                    yield self._splash_request(SmartContentSpider.url_template.format(self.lang, race_date.strftime('%Y/%m/%d')))
        elif response.css('div.localResults'):
            m = SmartContentSpider.url_pattern.match(response.url)
            yield {m.groups()[0]: response.text}

            if not re.search('.*RaceNo=.*', response.url):
                links = response.css('table.js_racecard tbody tr td a').xpath('@href').re('.*RaceNo=.*')
                for link in links:
                    yield self._splash_request(response.urljoin(link))

    def _splash_request(self, link):
        return SplashRequest(link, self.parse, endpoint='render.html', args={'wait': 15, 'html': 1, 'images': 0, 'timeout': 60})
