from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import sys

import pymongo

class MovieUpdates():
    """
    Run scrapy crawler "check_new_movies" to update current
    movie database, this will only update the movie status include
    newly opened, closed. will not download any movie metadata
    """
    def __init__(self, settings, db):
        self.scrapy_settings = settings
        self.db_collection = db['updates']
        self.updates = {}

    def update(self):
        """run scrapy crawler to get movie updates"""

        process = CrawlerProcess(self.scrapy_settings)
        process.crawl('movie_updates')
        process.start()
        process.stop()

        self.updates = self.db_collection.find_one(
            {'updated': False}, sort=[('createdAt', -1)])

        if self.updates is None:
            raise Exception('No updates found from db')

    def get_new_movie_ids(self):
        """return all newly found movie ids"""

        return self.updates['new']['in_theater'] + self.updates['new']['out_theater']

    def get_opened_movie_ids(self):
        return self.updates['opened']

    def get_closed_movie_ids(self):
        return self.updates['closed']

    def get_new_in_movie_ids(self):
        return self.updates['new']['in_theater']

    def get_new_out_movie_ids(self):
        return self.updates['new']['out_theater']

def main(args):
    scrapy_settings = get_project_settings()
    mongo_uri = scrapy_settings.get('MONGO_URI'),
    mongo_db = scrapy_settings.get('MONGO_DATABASE', 'movies')
    client = pymongo.MongoClient(mongo_uri)
    db = client[mongo_db]

    movie_updates = MovieUpdates(scrapy_settings, db)
    movie_updates.update()

if __name__ == '__main__':
    sys.exit(main(sys.argv))
