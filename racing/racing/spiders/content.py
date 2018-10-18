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
        # yield scrapy.Request(f"file:///Users/arthur/Workspace/hkjc/content/2015/English-20160228-ST-8.html")

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

        def fix_it(t, indice, no_of_rows=1):
            idx = indice.index
            last_idx = len(t) - 1
            for i in idx:
                if i >= last_idx:
                    continue

                for j in range(1, no_of_rows+1):
                    k = i + j
                    d = t.iloc[k].dividend_in_hkd
                    if isinstance(d, (int, float)) and math.isnan(d):
                        t.at[k, 'dividend_in_hkd'] = t.iloc[k].winning_combination
                        t.at[k, 'winning_combination'] = t.iloc[k].pool
                        t.at[k, 'pool'] = t.iloc[i].pool

        fix_it(table, table[table.pool == 'PLACE'], 2)
        fix_it(table, table[table.pool == 'QUINELLA PLACE'], 2)
        fix_it(table, table[table.pool == 'TREBLE'])
        fix_it(table, table[table.pool == 'SIX UP'])
        fix_it(table, table[table.pool == 'QUARTET'])
        fix_it(table, table[table.pool.notnull() & table.pool.str.contains('DOUBLE')])
        fix_it(table, table[table.pool.notnull() & table.pool.str.contains('COMPOSITE WIN')], 6)

        dividends = []
        for pool, winning_combination, dividend in table[table.pool.str.contains('COMPOSITE WIN') == False].values:
            dividends.append({
                label[0]: pool,
                label[1]: winning_combination,
                label[2]: dividend
            })

        return dividends
