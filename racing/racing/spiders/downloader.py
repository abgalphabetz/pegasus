# -*- coding: utf-8 -*-
import datetime
import os

import pandas as pd
import scrapy

from racing.context import settings
from racing.context.helper import splash_request


class DownloaderSpider(scrapy.Spider):
    name = 'downloader'
    allowed_domains = [settings.domain]

    def start_requests(self):
        file = f"links-{self.year}.jl"
        season = pd.read_json(file, lines=True)
        directory = f"{self.year}/"

        if not os.path.exists(directory):
            os.mkdir(directory)

        for link in season.link:
            english_filename = self._filename(link)
            if not os.path.exists(english_filename):
                yield splash_request(link)

            chinese_filename = english_filename.replace('English', 'Chinese')
            if not os.path.exists(chinese_filename):
                yield splash_request(link.replace('English', 'Chinese'))

    def parse(self, response):
        if response.css('div.localResults'):
            filename = self._filename(response.url)
            with open(filename, 'w+') as f:
                f.write(response.text)

    def _filename(self, url):
        m = settings.full_url_pattern.search(url)
        (lang, date, cource, race_no) = m.group(1), m.group(2), m.group(3), m.group(4)
        yyyymmdd = datetime.datetime.strptime(date, '%Y/%m/%d').strftime('%Y%m%d')
        return f"{self.year}/{lang}-{yyyymmdd}-{cource}-{race_no if race_no else 1}.html"
