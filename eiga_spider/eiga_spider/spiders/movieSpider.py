import scrapy
import json
import urllib
import pymongo
from eiga_spider.items import MovieItem


class MovieSpider(scrapy.Spider):
    name = "get_movies"
    allowed_domains = ["eiga.com"]

    custom_settings = {
        'FEED_URI': 'updated_movies.json',
        'ITEM_PIPELINES': {'scrapy.pipelines.images.ImagesPipeline': 1},
        'IMAGES_STORE': './images/'
    }

    def __init__(self, in_theater_ids, out_theater_ids):
        self.in_theater_ids = in_theater_ids
        self.out_theater_ids = out_theater_ids

    def start_requests(self):
        in_theater_requests = [scrapy.Request('http://eiga.com/movie/%s/' % id, self.parse_in_theater_movie)
                               for id in self.in_theater_ids]
        out_theater_requests = [scrapy.Request('http://eiga.com/movie/%s/' % id, self.parse_out_theater_movie)
                                for id in self.out_theater_ids]

        return in_theater_requests + out_theater_requests

    def parse_in_theater_movie(self, response):
        movie, poster_url = self._extract_movie_with_poster(response)
        movie['in_theater'] = True

        poster_request = scrapy.Request(poster_url, self.parse_poster)
        poster_request.meta['movie'] = movie
        yield poster_request

    def parse_out_theater_movie(self, response):
        movie, poster_url = self._extract_movie_with_poster(response)
        movie['in_theater'] = False

        poster_request = scrapy.Request(poster_url, self.parse_poster)
        poster_request.meta['movie'] = movie
        yield poster_request

    def parse_poster(self, response):
        poster_url = response.xpath('//div[@id="movie_photo"]/a/img/@src').extract_first()
        movie = response.meta['movie']

        movie['poster_url'] = poster_url
        movie['image_urls'] = [poster_url]

        yield movie

    def _extract_movie_with_poster(self, response):
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

        return (movie, poster_url)

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

    def extract_staff(self, staff_selector):
        pass

    def extract_cast(self, staff_selector):
        pass
