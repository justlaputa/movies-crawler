# -*- coding: utf-8 -*-
import scrapy
import json
import urllib
from eiga_spider.items import MovieItem

class OnReleaseSpider(scrapy.Spider):
    name = "eiga_on_release"
    allowed_domains = ["eiga.com"]
    start_urls = ["http://eiga.com/now/"]

    def parse(self, response):
        for href in response.xpath('//div[@id="now_movies"]/div[@class="m_unit"]/h3/a/@href'):
            url = response.urljoin(href.extract())
            print url
            yield scrapy.Request(url, callback=self.parse_movie)

        next_page = response.xpath('//div[@id="now_movies"]/div[@class="page_info"]/div[@class="pagination"]/a[@rel="next"][1]')
        next_page_href = next_page.xpath('@href').extract()[0]
        next_page_url = response.urljoin(next_page_href)
        next_page_number = next_page.xpath('text()').extract()[0]
        next_page_number = int(next_page_number)

        if next_page_number <= 1:
            yield scrapy.Request(next_page_url, callback=self.parse)

    def parse_movie(self, response):
        movie = MovieItem()
        movieInfoBox = response.xpath('//div[@class="moveInfoBox"]')

        movie['eiga_url'] = response.url
        movie['eiga_movie_id'] = response.url.split('/')[-2]
        movie['title_jp'] = movieInfoBox.xpath('h1[@itemprop="name"]/text()').extract_first()
        movie['release_date_jp'] = movieInfoBox.xpath('span[@class="opn_date"]/strong/text()').extract_first()
        movie['staff'] = self.extract_staff(movieInfoBox.xpath('div[@class="staffcast"]/div[@class="staffBox"]')[0])
        movie['cast'] = self.extract_cast(movieInfoBox.xpath('div[@class="staffcast"]/div[@class="castBox"]')[0])
        movie['movie_data'] = self.extract_movie_data(movieInfoBox.xpath('div[@class="dataBox"]')[0])

        yield movie

    def extract_staff(self, staff_selector):
        pass

    def extract_cast(self, staff_selector):
        pass

    def extract_movie_data(self, staff_selector):
        pass
