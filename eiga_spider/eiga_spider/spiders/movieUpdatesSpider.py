# -*- coding: utf-8 -*-
import scrapy
import json
import urllib
import pymongo
from eiga_spider.items import MovieIdItem


class UpdateMovieSpider(scrapy.Spider):
    name = "movie_updates"
    allowed_domains = ["eiga.com"]
    testing_page = 1000

    custom_settings = {
        'ITEM_PIPELINES': {
            'eiga_spider.pipelines.MovieUpdatesPipeline': 300
        }
    }

    def start_requests(self):
        opened_request = scrapy.Request('http://eiga.com/now/', self.parse_now_playing)
        scheduled_request = scrapy.Request('http://eiga.com/coming/', self.parse_scheduled_page)

        return [opened_request, scheduled_request]

    def parse_now_playing(self, response):
        self.logger.debug('parsing now playing page')
        for item in self.parse_one_playing_page(response):
            yield item

        (next_page_no, next_page_url) = self.get_next_playing_page_info(response)

        if next_page_url is not None and next_page_no <= self.testing_page:
            yield scrapy.Request(next_page_url, self.parse_now_playing)

    def parse_scheduled_page(self, response):
        self.logger.debug('parsing scheduled page')

        if self._is_first_scheduled_page(response):
            for item in self.parse_one_scheduled_page(response):
                yield item

        schedule_month_tablink = \
            [response.urljoin(href.extract()) for href in response.xpath('//div[@class="ctsBox"]/div[@class="tabLink"][1]/ul/li/a/@href')]

        last = min(self.testing_page, len(schedule_month_tablink))

        for url in schedule_month_tablink[:last-1]:
            yield scrapy.Request(url, self.parse_one_scheduled_page)

    def parse_one_scheduled_page(self, response):
        items = []
        for href in response.xpath('//div[@class="ctsBox"]/div[@class="m_unit"]/div/h4[1]/a/@href'):
            movie_item = MovieIdItem()
            movie_item['url'] = response.urljoin(href.extract())
            movie_item['id'] = movie_item['url'].split('/')[-2]
            movie_item['in_theater'] = None

            items.append(movie_item)
        return items

    def parse_one_playing_page(self, response):
        items = []
        for href in response.xpath('//div[@id="now_movies"]/div[@class="m_unit"]/h3/a/@href'):
            movie_item = MovieIdItem()
            movie_item['url'] = response.urljoin(href.extract())
            movie_item['id'] = movie_item['url'].split('/')[-2]
            movie_item['in_theater'] = True

            items.append(movie_item)
        return items

    def get_next_playing_page_info(self, response):
        next_page = response.xpath('//div[@id="now_movies"]/div[@class="page_info"]/div[@class="pagination"]/a[@rel="next"][1]')
        if len(next_page) > 0:
            next_page_href = next_page.xpath('@href').extract_first()
            next_page_url = response.urljoin(next_page_href)
            next_page_number = next_page.xpath('text()').extract_first()
            next_page_number = int(next_page_number)
        else:
            next_page_number = -1
            next_page_url = None

        return (next_page_number, next_page_url)

    def _is_first_scheduled_page(self, response):
        first_link = response.xpath('//div[@class="ctsBox"]/div[@class="tabLink"][1]/ul/li[1]/a')
        return len(first_link) == 0
