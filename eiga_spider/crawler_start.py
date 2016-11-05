# -*- coding: utf-8 -*-

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import pymongo
import logging
import datetime
import requests
import hashlib
import os
import sys
from trakt_movie import TraktMovie
from country_parser import CountryParser
from movie_convert import MovieConvert
from movie_updates import MovieUpdates
from movie_crawler import MovieCrawler
from movie_trakt import MovieTrakt

import boto3
from boto3.s3.transfer import S3Transfer
import pdb

logging.basicConfig(level = logging.DEBUG)

class MovieCrawler():
    def __init__(self, settings, db, webdb):
        self.settings = settings
        self.db = db
        self.movie_col = self.db['eiga_movies']
        self.update_col = self.db['updates']
        self.web_movie_col = webdb['movies']
        client = boto3.client('s3', 'ap-northeast-1')
        self.s3_transfer = S3Transfer(client)

        self.movie_updates = MovieUpdates(settings, db)
        self.movie_crawler = MovieCrawler(settings, db)
        self.movie_trakt = MovieTrakt(self.movie_col)
        self.movie_convert = MovieConvert()

    @classmethod
    def initialize(cls):
        scrapy_settings = get_project_settings()
        mongo_url = scrapy_settings.get('MONGO_URI')
        mongo_db = scrapy_settings.get('MONGO_DATABASE')
        client = pymongo.MongoClient(mongo_url)
        db = client[mongo_db]
        webdb_client = pymongo.MongoClient(scrapy_settings.get('WEB_MONGO_URI'))
        webdb = webdb_client[scrapy_settings.get('WEB_MONGO_DATABASE')]

        return cls(settings=scrapy_settings, db=db, webdb=webdb)

    def start(self):
        self.movie_updates.update()

        new_movie_ids = self.movie_updates.get_new_movie_ids()

        self.update_opened_movies_col(self.movie_col,
                                      self.movie_updates.get_opened_movie_ids())
        self.update_closed_movies_col(self.movie_col,
                                      self.movie_updates.get_closed_movie_ids())

        self.movie_crawler.run(self.movie_updates.get_new_in_movie_ids(),
                               self.movie_updates.get_new_out_movie_ids())
        
#        self.run_get_gallery_spider(new_movie_ids)

        self.update_external_info(new_movie_ids)

#        self.download_movie_images(new_movie_ids)
#        self.upload_images_s3(new_movie_ids)

        self.update_web_movies(updates)

    def update_opened_movies_col(self, collection, opened_ids):
        found_movies = collection.find({'eiga_movie_id': {'$in': opened_ids}, 'in_theater': False},
                                       {'eiga_movie_id': 1, 'in_theater': 1})
        if found_movies.count() != len(opened_ids):
            opened_ids_copy = set(opened_ids)
            logging.warning('the movies in db does not match the opened ids, please check db records')
            for movie in found_movies:
                logging.debug('(%s, in theater: %s)', movie['eiga_movie_id'], movie['in_theater'])
                opened_ids_copy.remove(movie['eiga_movie_id'])
            logging.debug('not found movies: [%s]', ','.join(opened_ids_copy))
            raise Exception('opened movie ids does not match in db')

        logging.info('updating newly opened movies in db: %s', opened_ids)
        collection.update_many({'eiga_movie_id': {'$in': opened_ids}}, {'$set': {'in_theater': True}})

    def update_closed_movies_col(self, collection, closed_ids):
        found_movies = collection.find({'eiga_movie_id': {'$in': closed_ids}, 'in_theater': True},
                                       {'eiga_movie_id': 1, 'in_theater': 1})

        if found_movies.count() != len(closed_ids):
            closed_ids_copy = set(closed_ids)
            logging.warning('the movies in db does not match the opened ids, please check db records')
            for movie in found_movies:
                logging.debug('(%s, in theater: %s)' % (movie['eiga_movie_id'], movie['in_theater']))
                closed_ids_copy.remove(movie['eiga_movie_id'])
            logging.debug('not found movies: [%s]', ','.join(closed_ids_copy))
            raise Exception('closed movie ids does not match in db')

        logging.info('updating newly closed movies in db: %s', closed_ids)
        collection.update_many({'eiga_movie_id': {'$in': closed_ids}}, {'$set': {'in_theater': False}})

    def update_external_info(self, movie_ids):
        for eiga_id in movie_ids:
            eiga_movie = self.movie_col.find_one({'eiga_movie_id': eiga_id})
            if movie is None:
                logging.warning('movie (%s) not found in db, skip', eiga_id)
                continue
            trakt_movie = self.movie_trakt.get_trakt_info(eiga_movie)

            if trakt_movie is None:
                logging.info(
                    'Could not find Trakt info for movie (%s)' % eiga_movie['title_jp'])
                continue
            logging.debug('got trakt info: %s', trakt_movie.to_json())
            self.movie_col.find_one_and_update({'_id': movie['_id']}, {
                '$set': {
                    'external': {'trakt': trakt_movie.to_json()}
                }
            }, upsert=True)

    def download_movie_images(self, movie_ids):
        for eiga_id in movie_ids:
            movie = self.movie_col.find_one({'eiga_movie_id': eiga_id})
            if movie is None:
                logging.warning('movie (%s) not found in db, skip', eiga_id)
                continue

            image_urls = self.get_movie_images_urls(movie)
            logging.debug('extract all images urls from movie(%s): %s', eiga_id, image_urls)
            images = self._download_images(image_urls)
            self.movie_col.find_one_and_update({'_id': movie['_id']}, {
                '$set': {'images': images}
            }, upsert=True)

    def get_movie_images_urls(self, movie):
        urls = {
            'posters': [],
            'fanarts': []
        }
        if movie['poster_url']:
            urls['posters'].append(movie['poster_url'])

        if movie['gallery']:
            urls['fanarts'].extend(movie['gallery'])

        if movie['external']['trakt']:
            trakt = movie['external']['trakt']
            for _, url in trakt['images']['poster'].iteritems():
                urls['posters'].append(url)
            for _, url in trakt['images']['fanart'].iteritems():
                urls['fanarts'].append(url)
        return urls

    def _download_images(self, urls):
        images = []
        for url in urls['posters']:
            images.append(self._download_one_image(url, 'images/posters', ''))
        for url in urls['fanarts']:
            images.append(self._download_one_image(url, 'images/fanarts', ''))
        return images

    def _download_one_image(self, url, dir, prefix):
        image = {
            'url': url,
            'file': '',
            'download_date': datetime.datetime.now(),
            'meta': {}
        }

        r = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36'
        })

        if not r.ok:
            logging.warning('could not download image from url: %s', url)
            logging.warning(r)
            return None

        fileext = url.split('/')[-1].split('.')[-1]

        sha256sum = hashlib.sha256(r.content).hexdigest()
        filename = '%s%s.%s' % (prefix, sha256sum, fileext)
        pathname = os.path.join(dir, filename)

        logging.info('download images file: %s', pathname)
        with open(pathname, 'wb') as out_file:
            out_file.write(r.content)

        image['file'] = filename

        return image


    def upload_images_s3(self, movie_ids):
        for movie in self.movie_col.find({'eiga_movie_id': {'$in': movie_ids}}):
            self._upload_movie_images_to_s3(movie)

    def _upload_movie_images_to_s3(self, movie):
        logging.info('uploading [%s](%s) images file to s3', movie['title_jp'], movie['eiga_movie_id'])
        if 'images' not in movie or len(movie['images']) == 0:
            logging.info('images file list not found in movie [%s](%s), skip', movie['title_jp'], movie['eiga_movie_id'])
            return
        images = movie['images']
        image_map = {}
        for image in images:
            if 'url' in image and len(image['url']) > 0:
                image_map[image['url']] = image

        if 'poster_url' in movie and len(movie['poster_url']) > 0:
            poster = image_map.get(movie['poster_url'])
            if poster:
                logging.debug('uploading poster: %s', poster['url'])
                self._upload_poster_to_s3(poster['file'])
            else:
                logging.debug('poster file not found for %s', poster['url'])
        else:
            logging.debug('poster url not found')
        if 'gallery' in movie and len(movie['gallery']) > 0:
            for gallery_url in movie['gallery']:
                gallery_img = image_map.get(gallery_url)
                if gallery_img:
                    logging.debug('uploading %s', gallery_url)
                    self._upload_fanart_to_s3(gallery_img['file'])
                else:
                    logging.debug('gallery file not found for %s', gallery_url)
        else:
            logging.debug('galleries not found')

        if 'external' in movie and movie['external'].get('trakt'):
            trakt_images = movie['external']['trakt']['images']
            if trakt_images.get('poster'):
                for _, url in trakt_images['poster'].iteritems():
                    trakt_poster_image = image_map.get(url)
                    if trakt_poster_image:
                        logging.debug('uploading trakt poster: %s', url)
                        self._upload_poster_to_s3(trakt_poster_image['file'])
                    else:
                        logging.debug('poster file not found for trakt image %s', url)
            else:
                logging.debug('no poster found in trakt info')

            if trakt_images.get('fanart'):
                for _, url in trakt_images['fanart'].iteritems():
                    trakt_fanart_image = image_map.get(url)
                    if trakt_fanart_image:
                        logging.debug('uploading trakt fanart: %s', url)
                        self._upload_fanart_to_s3(trakt_fanart_image['file'])
                    else:
                        logging.debug('fanart file not found for trakt image %s', url)
            else:
                logging.debug('no fanart found in trakt info')
        else:
            logging.debug('no trakt info found in movie [%s](%s)', movie['title_jp'], movie['eiga_movie_id'])

    def _upload_poster_to_s3(self, filename):
        if filename.endswith('.png'):
            content_type = 'image/png'
        else:
            content_type = 'image/jpeg'
        self.s3_transfer.upload_file('images/posters/%s' % filename,
                                     'japan-movies', 'posters/%s' % filename,
                                     extra_args={'ACL': 'public-read', 'ContentType': content_type})

    def _upload_fanart_to_s3(self, filename):
        if filename.endswith('.png'):
            content_type = 'image/png'
        else:
            content_type = 'image/jpeg'
        self.s3_transfer.upload_file('images/fanarts/%s' % filename,
                                     'japan-movies', 'fanarts/%s' % filename,
                                     extra_args={'ACL': 'public-read', 'ContentType': content_type})

    def update_web_movies(self, updates):
        new_movie_ids = updates['new']['in_theater'] + updates['new']['out_theater']
        new_movies = self.movie_col.find({'eiga_movie_id': {'$in': new_movie_ids}})
        self._update_new_movie(new_movies)

    def _update_new_movie(self, new_movies):
        for movie in new_movies:
            web_movie = self.movie_convert.get_web_movie(movie)

def main(args):
    if len(args) > 1:
        ids = args[1].split(',')
    crawler = MovieCrawler.initialize()
    crawler.download_movie_images(ids)
    crawler.upload_images_s3(ids)

if __name__ == '__main__':
    sys.exit(main(sys.argv))
