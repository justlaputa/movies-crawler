# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class EigaSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    japan_title = scrapy.Field()
    eiga_link = scrapy.Field()
    release_date = scrapy.Field()

class MovieItem(scrapy.Item):
    eiga_movie_id = scrapy.Field()
    eiga_url = scrapy.Field()
    poster_url = scrapy.Field()
    gallery = scrapy.Field()
    original_title = scrapy.Field()
    release_date_jp = scrapy.Field()
    in_theater = scrapy.Field()
    title_jp = scrapy.Field()
    staff = scrapy.Field()
    cast = scrapy.Field()
    movie_data = scrapy.Field()

class MovieIdItem(scrapy.Item):
    id = scrapy.Field()
    url = scrapy.Field()
    in_theater = scrapy.Field()