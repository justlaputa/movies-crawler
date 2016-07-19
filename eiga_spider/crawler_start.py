from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import pymongo
import logging

logging.basicConfig(level = logging.DEBUG)

class MovieCrawler():
    def __init__(self, settings, db):
        self.settings = settings
        self.db = db
        self.movie_col = self.db['eiga_movies']
        self.update_col = self.db['updates']

    @classmethod
    def initialize(cls):
        scrapy_settings = get_project_settings()
        mongo_url = scrapy_settings.get('MONGO_URI')
        mongo_db = scrapy_settings.get('MONGO_DATABASE')
        client = pymongo.MongoClient(mongo_url)
        db = client[mongo_db]

        return cls(settings=scrapy_settings, db=db)

    def start(self):
        self.run_update_movie_spider()

        updates = self.get_latest_updates()
        new_movie_ids = updates['new']['in_theater'] + updates['new']['out_theater']

        self.update_opened_movies_col(self.movie_col, updates['opened'])
        self.update_closed_movies_col(self.movie_col, updates['closed'])
        self.run_get_movie_spider(in_theater_ids=updates['new']['in_theater'],
                                  out_theater_ids=updates['new']['out_theater'])
        self.run_get_gallery_spider(new_movie_ids)
        self.download_movie_images(new_movie_ids)
        self.upload_images_s3(new_movie_ids)
        self.update_web_movies_col(updates)

    def run_update_movie_spider(self):
        process = CrawlerProcess(self.settings)
        process.crawl('check_new_movies')
        process.start()
        process.stop()

    def get_latest_updates(self):
        result = self.update_col.find_one({'updated': False}, sort=[('createdAt', -1)])
        if result is None:
            raise Exception('No updates found from db')

        return result

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

    def run_get_movie_spider(self, in_theater_ids, out_theater_ids):
        process = CrawlerProcess(self.settings)
        process.crawl('')

    def run_get_gallery_spider(self, movie_ids):
        pass

    def download_movie_images(self, movie_ids):
        pass

    def upload_images_s3(self, movie_ids):
        pass

    def update_web_movies_col(self, updates):
        pass

if __name__ == '__main__':
    main = MovieCrawler.initialize()
    main.start()