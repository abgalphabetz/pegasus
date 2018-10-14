from scrapy_splash import SplashRequest

from racing.context.settings import race_date_url_pattern


def splash_request(parse, link):
    return SplashRequest(link, parse, endpoint='render.html', args={'wait': 15, 'html': 1, 'images': 0, 'timeout': 60})


def race_date_from_url(url):
    m = race_date_url_pattern.match(url)
    return m.groups()[0]