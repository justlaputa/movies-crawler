# -*- coding: utf-8 -*-
import scrapy
import json
import urllib
import pymongo
from eiga_spider.items import MovieItem


class updateGallerySpider(scrapy.Spider):
    name = "update_eiga_gallery"
    allowed_domains = ["eiga.com"]

    custom_settings = {
        'ITEM_PIPELINES': {
            'eiga_spider.pipelines.NewMoviesPipeline': 300
        }
    }

    def __init__(self, movie_ids):
        if isinstance(movie_ids, str):
            movie_ids = movie_ids.split(',')
        elif not isinstance(movie_ids, list):
            raise Exception('invalid parameters to initialize spider: use list or comma separated string')

        self.movie_ids = movie_ids

    def start_requests(self):
        return [scrapy.Request("http://eiga.com/movie/%s/gallery/" % id, self.parse_gallery)
                for id in self.movie_ids]

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
