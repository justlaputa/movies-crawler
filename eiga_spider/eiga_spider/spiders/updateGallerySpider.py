# -*- coding: utf-8 -*-
import scrapy
import json
import urllib
import pymongo
from eiga_spider.items import MovieItem


class updateGallerySpider(scrapy.Spider):
    name = "update_eiga_gallery"
    allowed_domains = ["eiga.com"]

    def start_requests(self):
        mongo_uri = self.settings.get('MONGO_URI'),
        mongo_db = self.settings.get('MONGO_DATABASE', 'movies')
        client = pymongo.MongoClient(mongo_uri)
        db = client[mongo_db]
        movies = db['eiga_movies'].find({'gallery': {'$exists': False}}, {'eiga_movie_id': 1})
        client.close()

        return [scrapy.Request("http://eiga.com/movie/%s/gallery/" % m['eiga_movie_id'], self.parse_gallery) for m in movies]

    def parse_gallery(self, response):
        movie = MovieItem()
        movie['eiga_movie_id'] = response.url.split('/')[-3]
        movie['gallery'] = []

        movie['gallery'].append(self._extract_one_gallery(response))

        all_gallerys = response.xpath('//div[@id="main"]//div[@class="galleryBox"]/a[position()>1]/@href')
        total_gallery = len(all_gallerys) + 1
        print('added gallery [1/%d] to movies %s' % (total_gallery, movie['eiga_movie_id']))

        for gallery in all_gallerys:
            gallery_url = response.urljoin(gallery.extract())
            request = scrapy.Request(gallery_url, self.parse_one_gallery)
            request.meta['movie'] = movie
            request.meta['total_gallery'] = total_gallery
            yield request

    def parse_one_gallery(self, response):
        movie = response.meta['movie']

        movie['gallery'].append(self._extract_one_gallery(response))
        print('add [%d/%d] to movie #%s' %(len(movie['gallery']), response.meta['total_gallery'], movie['eiga_movie_id']))

        if len(movie['gallery']) == response.meta['total_gallery']:
            print('finish all gallery')
            yield movie

    def _extract_one_gallery(self, response):
        gallery = response.xpath('//div[@id="photo"]/a/img/@src').extract_first()
        return gallery.split('?')[0]
