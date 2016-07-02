# -*- coding: utf-8 -*-
import scrapy
import json
import urllib
import pymongo
from eiga_spider.items import MovieItem


class UpdateMovieSpider(scrapy.Spider):
    name = "update_movies"
    allowed_domains = ["eiga.com"]

    parsed_opened_count = 0
    parsed_scheduled_count = 0
    max_parse_opened_limit = 1000
    max_parse_scheduled_limit = 1000
    max_parse_opened_page_limit = 100
    opened_movie_ids = None
    newly_added_movies = []
    not_found_movie_ids = []
    newly_opened_movie_ids = []
    closed_movie_ids = []

    db_collection_eiga_movies = None
    db_client = None


    def start_requests(self):
        mongo_uri = self.settings.get('MONGO_URI'),
        mongo_db = self.settings.get('MONGO_DATABASE', 'movies')
        self.db_client = pymongo.MongoClient(mongo_uri)
        self.db_collection_eiga_movies = self.db_client[mongo_db]['eiga_movies']
        movies = self.db_collection_eiga_movies.find({'in_theater': True}, {'eiga_movie_id': 1})

        self.opened_movie_ids = {m['eiga_movie_id'] for m in movies}

        opened_request = scrapy.Request('http://eiga.com/now/', self.parse_opened_page)
        scheduled_request = scrapy.Request('http://eiga.com/coming/', self.parse_scheduled_page)

        return [opened_request, scheduled_request]

    def closed(self, reason):
        if len(self.opened_movie_ids) > 0:
            self.logger.info('some movies are out of theater: %s', str(self.opened_movie_ids))
            for movie_id in self.opened_movie_ids:
                result = \
                    self.db_collection_eiga_movies.find_one_and_update({'eiga_movie_id': movie_id},
                                                                       {'$set': {'in_theater': False}})

                if result is None:
                    self.logger.warning('not found movie (%s)', movie_id)
                    self.not_found_movie_ids.append(movie_id)
                else:
                    self.closed_movie_ids.append(movie_id)

        self.logger.info('finished all crawler')
        self.logger.info('newly opened movies: %s', str(self.newly_opened_movie_ids))
        self.logger.info('closed movies: %s', str(self.closed_movie_ids))
        self.logger.info('not found movies: %s', str(self.not_found_movie_ids))

        self.logger.info('closing db connection...')
        self.db_client.close()

    def parse_opened_page(self, response):
        if self.parsed_opened_count >= self.max_parse_opened_limit:
            self.logger.info('reach max parse opened movies limit: %d, stop here', self.max_parse_opened_limit)
            return

        for movie_request in self.parse_current_opened_page(response):
            yield movie_request

        (next_page_no, next_page_url) = self._get_next_opened_page_url(response)

        if next_page_no > self.max_parse_opened_page_limit:
            self.logger.info('reach max parse opened page limit: %d, stop here', self.max_parse_opened_page_limit)
            return

        if next_page_url is None:
            self.logger.info('Reach the last opened page, stop crawling open movies')
            return

        yield scrapy.Request(next_page_url, self.parse_opened_page)

    def parse_scheduled_page(self, response):
        if self._is_first_scheduled_page(response):
            self.parse_one_scheduled_page(response)
            self.is_first_page = False

        schedule_month_tablink = \
            [response.urljoin(href.extract()) for href in response.xpath('//div[@class="ctsBox"]/div[@class="tabLink"][1]/ul/li/a/@href')]

        for url in schedule_month_tablink[:-1]:
            yield scrapy.Request(url, self.parse_one_scheduled_page)

    def parse_current_opened_page(self, response):
        for href in response.xpath('//div[@id="now_movies"]/div[@class="m_unit"]/h3/a/@href'):
            if self.parsed_opened_count >= self.max_parse_opened_limit:
                self.logger.info('reach max parse opened movies limit: %d, stop here', self.max_parse_opened_limit)
                break

            self.parsed_opened_count += 1

            movie_url = response.urljoin(href.extract())
            movie_id = movie_url.split('/')[-2]
            exist_movie = self._find_movie_in_db(movie_id)

            if exist_movie is not None:
                self.logger.info('Count: parsing %d opened movies', self.parsed_opened_count)
                self.logger.debug('opened movie [%s](%s) is found from db',
                                  exist_movie['title_jp'], exist_movie['eiga_movie_id'])

                if exist_movie.get('in_theater'):
                    self.logger.info('movie (%s) already marked as open in db, skip', movie_id)
                    self.opened_movie_ids.remove(movie_id)
                else:
                    self.logger.info('movie (%s) is newly opened, update db', movie_id)
                    self.db_collection_eiga_movies.update_one({'_id': exist_movie['_id']},
                                                              {'$set': {'in_theater': True}},
                                                              upsert=True)
                    self.newly_opened_movie_ids.append(movie_id)
            else:
                self.logger.info('movie (%s) not found in db, insert new movie', movie_id)
                yield self.add_new_movie(movie_url, opened=True)

    def parse_one_scheduled_page(self, response):
        for href in response.xpath('//div[@class="ctsBox"]/div[@class="m_unit"]/div/h4[1]/a/@href'):
            if self.parsed_scheduled_count >= self.max_parse_scheduled_limit:
                self.logger.info('Reached maximum allowed scheduled movie parse count(%s), stop here', self.max_parse_scheduled_limit)
                break

            self.parsed_scheduled_count += 1

            movie_url = response.urljoin(href.extract())
            movie_id = movie_url.split('/')[-2]

            exist_movie = self._find_movie_in_db(movie_id)

            if exist_movie is not None:
                self.logger.info('Count: %d scheduled movies parsed', self.parsed_scheduled_count)
                if exist_movie.get('in_theater'):
                    self.logger.warning('Found inconsistent movie [%s](%s): should be on schedule by found in theater',
                                        exist_movie['title_jp'], exist_movie['eiga_movie_id'])
                    self.logger.info('mark movie (%s) as not opened', exist_movie['eiga_movie_id'])
                    self.db_collection_eiga_movies.update_one({'_id': exist_movie['_id']},
                                                              {'$set': {'in_theater': False}},
                                                              upsert=True)
            else:
                self.logger.info('movie (%s) not found in db, insert new movie', movie_id)
                yield self.add_new_movie(movie_url, opened=False)

    def _is_first_scheduled_page(self, response):
        first_link = response.xpath('//div[@class="ctsBox"]/div[@class="tabLink"][1]/ul/li[1]/a')
        return len(first_link) == 0

    def _get_next_opened_page_url(self, response):
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

    def _find_movie_in_db(self, movie_id):
        return self.db_collection_eiga_movies.find_one({'eiga_movie_id': movie_id})

    def add_new_movie(self, url, opened=False):
        request = scrapy.Request(url, self.parse_movie)
        request.meta['opened'] = opened

        return request

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
        movie['in_theater'] = response.meta['opened']

        #continue to crawler the movie's poster url in a new request
        request = scrapy.Request(poster_url, callback=self.parse_poster)
        request.meta['movie'] = movie

        yield request

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
