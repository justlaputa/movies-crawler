# -*- coding: utf-8 -*-
import scrapy
import json
import urllib
import pymongo
from eiga_spider.items import MovieItem


class OnReleaseSpider(scrapy.Spider):
    name = "eiga_on_release"
    allowed_domains = ["eiga.com"]

    def start_requests(self):
        mongo_uri = self.settings.get('MONGO_URI'),
        mongo_db = self.settings.get('MONGO_DATABASE', 'movies')
        client = pymongo.MongoClient(mongo_uri)
        db = client[mongo_db]
        movies = db['eiga_movies'].find({}, {'eiga_movie_id': 1})
        self.exists_movies = [m['eiga_movie_id'] for m in movies]
        client.close()

        return [scrapy.Request("http://eiga.com/now/", self.parse)]

    def parse(self, response):
        for href in response.xpath('//div[@id="now_movies"]/div[@class="m_unit"]/h3/a/@href'):
            url = response.urljoin(href.extract())
            movie_id = url.split('/')[-2]

            if movie_id in self.exists_movies:
                print('eiga movie [%s] already scrapied, stop crawlling' % movie_id)
                return

            yield scrapy.Request(url, callback=self.parse_movie)

        next_page = response.xpath('//div[@id="now_movies"]/div[@class="page_info"]/div[@class="pagination"]/a[@rel="next"][1]')
        next_page_href = next_page.xpath('@href').extract()[0]
        next_page_url = response.urljoin(next_page_href)
        next_page_number = next_page.xpath('text()').extract()[0]
        next_page_number = int(next_page_number)

        if next_page_number <= 10:
            yield scrapy.Request(next_page_url, callback=self.parse)

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

        #continue to crawler the movie's poster url in a new request
        request = scrapy.Request(poster_url, callback=self.parse_poster)
        request.meta['movie'] = movie

        yield request

    def parse_movie_gallery(self):
        pass

    def parse_poster(self, response):
        poster_url = response.xpath('//div[@id="movie_photo"]/a/img/@src').extract_first()
        movie = response.meta['movie']

        movie['poster_url'] = poster_url

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
