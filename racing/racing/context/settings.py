import re

domain = 'racing.hkjc.com'
url_base = f"http://{domain}/racing/information"
url_template = url_base + "/{0}/Racing/LocalResults.aspx?RaceDate={1}"
race_date_url_pattern = re.compile(".*RaceDate=(.{10}).*")
full_url_pattern = re.compile(
    "/(Chinese|English)/Racing/LocalResults.aspx.*RaceDate=(.{10})(?:&Racecourse=(.*)&RaceNo=(.*))?")
race_no_url_pattern = re.compile('.*RaceNo=.*')
content_dir = '/Users/arthur/Workspace/hkjc/data'
