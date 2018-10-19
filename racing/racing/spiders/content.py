# -*- coding: utf-8 -*-
import glob
import math
import os

import pandas as pd
import scrapy

from racing.context import settings


class ContentSpider(scrapy.Spider):
    name = 'content'

    def start_requests(self):
        lang = self.lang
        year = self.year
        if not year:
            raise ValueError("Year must be provided.")

        content_dir = os.path.join(settings.content_dir, year)
        content_files = os.path.join(content_dir, f"{lang}-*")
        for file in glob.glob(content_files):
            path_to_file = os.path.join(content_dir, file)
            yield scrapy.Request(f"file://{path_to_file}")
        # yield scrapy.Request(f"file:///Users/arthur/Workspace/hkjc/content/2015/English-20151025-ST-2.html")

    def parse(self, response):
        tables = pd.read_html(response.text, thousands=None)
        metainfo = self.try_read(f"metainfo - {response.url}", self.read_race_and_course_meta_data, tables[1])
        dividend = self.try_read(f"dividend - {response.url}", self.read_dividend, tables[3])
        # yield {'link': f'{response.url}', 'dividend': dividend}
        yield {'link': f'{response.url}', 'metainfo': metainfo, 'dividend': dividend}

    @staticmethod
    def try_read(description, func, *args):
        try:
            return func(*args)
        except Exception as e:
            print(f"{description} - {e}")

    @staticmethod
    def read_race_and_course_meta_data(table: pd.DataFrame):
        times = [f'time{i+1}' for i in range(1, table.columns.size - 3 + 1)]
        table.columns = ['raceinfo', 'courseinfolabel', 'courseinfo'] + times
        race_info = table.iloc[1].raceinfo
        race_type = table.iloc[2].raceinfo
        prize = table.iloc[3].raceinfo

        course_going = table.iloc[1].courseinfo
        course_turf = table.iloc[2].courseinfo

        time1 = table.iloc[3].courseinfo
        sectional_time1 = table.iloc[4].courseinfo

        timesN = [table.iloc[3][t] for t in times]
        sectional_timesN = [table.iloc[4][t] for t in times]

        return {
            'race_info': race_info,
            'race_type': race_type,
            'prize': prize,
            'course_going': course_going,
            'course_turf': course_turf,
            'times': [time1] + timesN,
            'sectional_time': [sectional_time1] + sectional_timesN
        }

    @staticmethod
    def read_dividend(table: pd.DataFrame):
        label = ["pool", "winning_combination", "dividend_in_hkd"]
        table.columns = label

        def tidy_up(t):
            size = len(t)

            current_pool = None
            for i in range(0, size):
                current_row = t.iloc[i]
                if not (isinstance(current_row.dividend_in_hkd, (int, float)) and math.isnan(current_row.dividend_in_hkd)):
                    current_pool = current_row.pool
                    continue

                t.at[i, 'dividend_in_hkd'] = current_row.winning_combination if 'COMPOSITE WIN' not in current_pool else None
                t.at[i, 'winning_combination'] = current_row.pool
                t.at[i, 'pool'] = current_pool

        tidy_up(table)

        dividends = []
        for pool, winning_combination, dividend in table[table.pool.str.contains('COMPOSITE WIN') == False].values:
            dividends.append({
                label[0]: pool,
                label[1]: winning_combination,
                label[2]: dividend
            })

        return dividends
