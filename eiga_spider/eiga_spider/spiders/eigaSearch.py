# -*- coding: utf-8 -*-
import scrapy
import json
import urllib
from eiga_spider.items import EigaSpiderItem

def createSearchUrls():
    with open('movies.json') as data_file:
        titles = json.load(data_file)
    return ['http://eiga.com/search/' + urllib.quote_plus(title.encode('utf8')) + '/' for title in titles]

class EigasearchSpider(scrapy.Spider):
    name = "eigaSearch"
    allowed_domains = ["eiga.com"]
    start_urls = createSearchUrls()

    def parse(self, response):
        title = urllib.unquote_plus(response.url.split('/')[-2]).decode('utf8')
        for result in response.xpath('//*[@id="rslt-movie"]/div/dl[1]/dt/span/a/@href'):
            if result is not None:
                movie_url = 'http://eiga.com' + result.extract()
                request = scrapy.Request(movie_url, callback=self.parse_movie)
                request.meta['original_title'] = title
                return request
            else:
                print "can not search movie: %s" % title

    def parse_movie(self, response):
        japan_title = response.xpath('//*[@id="main"]/div[3]/div/div[1]/h1/text()').extract_first()
        release_date = response.xpath('//*[@id="main"]/div[3]/div/div[1]/span/strong/text()').extract_first()
        title = response.xpath('//*[@id="main"]/div[3]/div/div[1]/div[5]/table[1]/tbody/tr[1]/td/text()').extract_first()

        item = EigaSpiderItem()
        item['title'] = response.meta['original_title']
        item['japan_title'] = japan_title
        item['eiga_link'] = response.url
        item['release_date'] = release_date
        yield item
