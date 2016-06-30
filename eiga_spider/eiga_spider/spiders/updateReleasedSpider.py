# -*- coding: utf-8 -*-
import scrapy
import json
import urllib
import pymongo
from eiga_spider.items import MovieItem


class OnReleaseSpider(scrapy.Spider):
    name = "update_released_movies"
    allowed_domains = ["eiga.com"]
    parsed_movies_count = 0
    max_parse_limit = 10000
    max_parse_page_limit = 100
    released_movie_ids = None
    movies_db_collection = None
    db_client = None

    def start_requests(self):
        mongo_uri = self.settings.get('MONGO_URI'),
        mongo_db = self.settings.get('MONGO_DATABASE', 'movies')
        self.db_client = pymongo.MongoClient(mongo_uri)
        self.movies_db_collection = self.db_client[mongo_db]['eiga_movies']
        movies = self.movies_db_collection.find({'in_theater': True}, {'eiga_movie_id': 1})

        self.released_movie_ids = {m['eiga_movie_id'] for m in movies}

        return [scrapy.Request("http://eiga.com/now/", self.parse)]


    def parse(self, response):
        self.parse_current_page(response)

        if self.parsed_movies_count >= self.max_parse_limit:
            print('reach max parse limit, stop parsing')
            return

        (next_page_no, next_page_url) = self._get_next_page(response)

        if next_page_no > self.max_parse_page_limit:
            print('reach max page limit, stop parsing')
            return

        if next_page_url is None:
            print('reach last page, stop parsing')
            return

        yield scrapy.Request(next_page_url, self.parse)

    def closed(self, reason):
        print('mark %s off-theater movies' % len(self.released_movie_ids))
        for id in self.released_movie_ids:
            print('mark movie (%s) as off-theater' % id)
            self.movies_db_collection.find_one_and_update({'eiga_movie_id': id}, {'$set': {'in_theater': False}})

        print('finished')
        print('closing db connection...')
        self.db_client.close()

    def parse_current_page(self, response):
        for href in response.xpath('//div[@id="now_movies"]/div[@class="m_unit"]/h3/a/@href'):
            self.parsed_movies_count += 1
            print('parsing movies count: %d' % self.parsed_movies_count)

            movie_id = href.extract().split('/')[-2]

            if movie_id in self.released_movie_ids:
                print('movie [%s] already marked as released' % movie_id)
                self.released_movie_ids.remove(movie_id)
            else:
                print('movie [%s] is new released, update db' % movie_id)
                result = self.movies_db_collection.find_one_and_update({'eiga_movie_id': movie_id}, {'$set': {'in_theater': True}})
                if result is None:
                    print('released movie (%s) not found in db' % movie_id)
                else:
                    print('marked [%s] as in theater' % result['title_jp'])


    def _get_next_page(self, response):
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
