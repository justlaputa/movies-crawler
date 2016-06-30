# -*- coding: utf-8 -*-
import scrapy
import json
import urllib
import pymongo
from eiga_spider.items import MovieItem


class OnScheduleSpider(scrapy.Spider):
    name = 'eiga_on_schedule'
    allowed_domains = ['eiga.com']
    start_urls = ['http://eiga.com/coming/']
    is_first_page = True
    parse_movie_count = 0
    max_parse_movie = 10000

    def parse(self, response):

        if self.is_first_page:
            self.parse_one_page(response)
            self.is_first_page = False

        schedule_month_tablink = \
            [response.urljoin(href.extract()) for href in response.xpath('//div[@class="ctsBox"]/div[@class="tabLink"][1]/ul/li/a/@href')]

        for url in schedule_month_tablink[:-1]:
            yield scrapy.Request(url, self.parse_one_page)

    def parse_one_page(self, response):
        for href in response.xpath('//div[@class="ctsBox"]/div[@class="m_unit"]/div/h4[1]/a/@href'):
            if self.parse_movie_count >= self.max_parse_movie:
                print('max movies parsed, skip remaining')
                return

            movie_url = response.urljoin(href.extract())
            yield scrapy.Request(movie_url, self.parse_movie)

    def parse_movie(self, response):
        movie = MovieItem()

        movieInfoBox = response.xpath('//div[@class="moveInfoBox"]')
        mainBox = movieInfoBox.xpath('./div[@class="mainBox"]')

        poster_url = mainBox.xpath('./div[@class="pictBox"]/a/@href').extract_first()
        poster_url = response.urljoin(poster_url)

        movie['eiga_url'] = response.url
        movie['eiga_movie_id'] = response.url.split('/')[-2]
        movie['title_jp'] = movieInfoBox.xpath('h1[@itemprop="name"]/text()').extract_first()
        movie['release_date_jp'] = movieInfoBox.xpath('span[@class="opn_date"]/strong/text()').extract_first()
        movie['staff'] = self.extract_staff(movieInfoBox.xpath('div[@class="staffcast"]/div[@class="staffBox"]')[0])
        movie['cast'] = self.extract_cast(movieInfoBox.xpath('div[@class="staffcast"]/div[@class="castBox"]')[0])
        movie['movie_data'] = self.extract_movie_data(movieInfoBox.xpath('div[@class="dataBox"]')[0])
        movie['in_theater'] = False

        #continue to crawler the movie's poster url in a new request
        poster_request = scrapy.Request(poster_url, callback=self.parse_poster)
        poster_request.meta['movie'] = movie

        yield poster_request

    def parse_poster(self, response):
        poster_url = response.xpath('//div[@id="movie_photo"]/a/img/@src').extract_first()
        movie = response.meta['movie']

        movie['poster_url'] = poster_url

        self.parse_movie_count += 1
        print('[%s/%s] movies parsed' % (self.parse_movie_count, self.max_parse_movie))
        yield movie

    def extract_staff(self, staff_selector):
        pass

    def extract_cast(self, staff_selector):
        pass

    def extract_movie_data(self, data_selector):
        data = {}
        for data_item in data_selector.xpath('.//tr'):
            if len(data_item.xpath('th')) == 0:
                if data_item.xpath('td[1]/a/@class').extract_first() == 'official':
                    data['official_site'] = data_item.xpath('td[1]/a/@content').extract_first()
                else:
                    continue
            else:
                data_name = data_item.xpath('th/text()').extract_first()
                if data_name is None or data_name.strip() == '':
                    continue
                else:
                    data[data_name] = data_item.xpath('td/text()').extract_first()
        return data
