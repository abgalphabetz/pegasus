# -*- coding: utf-8 -*-
import datetime
import os

import scrapy
import pandas as pd
from scrapy_splash import SplashRequest

from racing.metadata import info


class DownloaderSpider(scrapy.Spider):
    name = 'downloader'
    allowed_domains = [info.domain]

    def start_requests(self):
        file = f"links-{self.year}.jl"
        season = pd.read_json(file, lines=True)
        directory = f"{self.year}/"

        if not os.path.exists(directory):
            os.mkdir(directory)

        for link in season.link:
            english_filename = self._filename(link)
            if not os.path.exists(english_filename):
                yield self._splash_request(link)

            chinese_filename = english_filename.replace('English', 'Chinese')
            if not os.path.exists(chinese_filename):
                yield self._splash_request(link.replace('English', 'Chinese'))

    def parse(self, response):
        if response.css('div.localResults'):
            filename = self._filename(response.url)
            with open(filename, 'w+') as f:
                f.write(response.text)

    def _splash_request(self, link, splash_url=None):
        return SplashRequest(link, self.parse, splash_url=splash_url, endpoint='render.html', args={'wait': 10, 'html': 1, 'images': 0, 'timeout': 60})

    def _filename(self, url):
        m = info.full_url_pattern.search(url)
        (lang, date, cource, race_no) = m.group(1), m.group(2), m.group(3), m.group(4)
        yyyymmdd = datetime.datetime.strptime(date, '%Y/%m/%d').strftime('%Y%m%d')
        return f"{self.year}/{lang}-{yyyymmdd}-{cource}-{race_no if race_no else 1}.html"
