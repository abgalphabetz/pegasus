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

        def fix_it(t, indice):
            idx = indice.index
            last_idx = len(t) - 1
            for i in idx:
                if i >= last_idx:
                    break

                for j in range(i+1, last_idx+1):
                    d = t.iloc[j].dividend_in_hkd
                    if not (isinstance(d, (int, float)) and math.isnan(d)):
                        break

                    wc = t.iloc[j].winning_combination
                    current_pool = t.iloc[i].pool
                    t.at[j, 'dividend_in_hkd'] = wc if 'COMPOSITE WIN' not in current_pool else None
                    t.at[j, 'winning_combination'] = t.iloc[j].pool
                    t.at[j, 'pool'] = t.iloc[i].pool

        fix_it(table, table[table.pool == 'PLACE'])
        fix_it(table, table[table.pool == 'QUINELLA PLACE'])
        fix_it(table, table[table.pool == 'TREBLE'])
        fix_it(table, table[table.pool == 'SIX UP'])
        fix_it(table, table[table.pool == 'QUARTET'])
        fix_it(table, table[table.pool.notnull() & table.pool.str.contains('DOUBLE')])
        fix_it(table, table[table.pool.notnull() & table.pool.str.contains('COMPOSITE WIN')])

        dividends = []
        for pool, winning_combination, dividend in table[table.pool.str.contains('COMPOSITE WIN') == False].values:
            dividends.append({
                label[0]: pool,
                label[1]: winning_combination,
                label[2]: dividend
            })

        return dividends
