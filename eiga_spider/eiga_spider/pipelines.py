# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import io
import json
import pymongo
from scrapy.exceptions import DropItem
from pprint import pprint

class MongoPipeline(object):
    collection_name = 'eiga_movies'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'movies')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        movie_collection = self.db[self.collection_name]

        movie = movie_collection.find_one({'eiga_movie_id': item['eiga_movie_id']})

        if movie is None:
            movie_collection.insert(dict(item))
        else:
            print('movie exists, add gallery')
            print(item)
            movie_collection.update_one({'eiga_movie_id': item['eiga_movie_id']},
                                        {
                                            '$set': {
                                                'gallery': item['gallery']
                                            },
                                            '$currentDate': {
                                                'lastModified': True
                                            }
                                        })
        return item

class UpdateOnSchedulePipeline(MongoPipeline):
    def process_item(self, item, spider):
        eiga_collection = self.db['eiga_movies']

        movie = eiga_collection.find_one({'eiga_movie_id': item['eiga_movie_id']})

        if movie is None:
            print('add new movie in db: %s (%s)' % (item['title_jp'], item['eiga_movie_id']))
            eiga_collection.insert_one(dict(item))
        else:
            print('scheduled movie %s (%s) already exists, skip' % (item['eiga_movie_id'], item['title_jp']))
        return item

class MongoDBPipeline(object):
    def __init__(self, client, db):
        self.client = client
        self.db = db

    @classmethod
    def from_crawler(cls, crawler):
        mongo_uri = crawler.settings.get('MONGO_URI'),
        mongo_db = crawler.settings.get('MONGO_DATABASE', 'movies')
        client = pymongo.MongoClient(mongo_uri)
        db = client[mongo_db]
        return cls(client, db)

    def close_spider(self, spider):
        self.client.close()

class MovieUpdatesPipeline(MongoDBPipeline):
    old_in_theater_movie_ids = set()
    old_out_theater_movie_ids = set()
    new_in_theater_movie_ids = set()
    new_scheduled_movie_ids = set()

    def open_spider(self, spider):
        self.movie_col = self.db['eiga_movies']
        self.update_col = self.db['updates']

        self.old_in_theater_movie_ids = self._get_in_theater_movie_ids()
        self.old_out_theater_movie_ids = self._get_out_theater_movie_ids()

    def _get_in_theater_movie_ids(self):
        movies = self.movie_col.find({'in_theater': True}, {'eiga_movie_id': 1})
        return {m['eiga_movie_id'] for m in movies}

    def _get_out_theater_movie_ids(self):
        movies = self.movie_col.find({'in_theater': False}, {'eiga_movie_id': 1})
        return {m['eiga_movie_id'] for m in movies}

    def process_item(self, item, spider):
        if item['in_theater'] == True:
            self.new_in_theater_movie_ids.add(item['id'])
        elif item['in_theater'] is None:
            self.new_scheduled_movie_ids.add(item['id'])
        else:
            print('invalid item processed (%s), in theather should not set to False' % item['id'])
        return item

    def close_spider(self, spider):

        updates = self._calculate_updates()
        self.insert_updates(updates)

        super(self.__class__, self).close_spider(spider)

    def _calculate_updates(self):
        old_all_movie_ids = self.old_in_theater_movie_ids | self.old_out_theater_movie_ids
        new_out_theater_movie_ids = self.new_scheduled_movie_ids - self.new_in_theater_movie_ids

        opened_movie_ids = self.new_in_theater_movie_ids & self.old_out_theater_movie_ids
        closed_movie_ids = self.old_in_theater_movie_ids - self.new_in_theater_movie_ids
        new_movies_in_theater = self.new_in_theater_movie_ids - old_all_movie_ids
        new_movies_out_theater = new_out_theater_movie_ids - old_all_movie_ids

        updates = {
            'opened': list(opened_movie_ids),
            'closed': list(closed_movie_ids),
            'new': {
                'in_theater': list(new_movies_in_theater),
                'out_theater': list(new_movies_out_theater)
            },
            'updated': False
        }

        print('calculated all updates:')
        pprint(updates)
        return updates

    def insert_updates(self, updates):
        print('insert updates to db')
        result = self.update_col.insert_one(updates)
        if result is not None:
            print('insert success with id %s' % result.inserted_id)
            print('add inserted date')
            up_doc = self.update_col.find_one_and_update({'_id': result.inserted_id}, {
                '$currentDate': {
                    'createdAt': True
                }
            }, return_document=pymongo.collection.ReturnDocument.AFTER)
            if up_doc.get('createdAt') is not None:
                print('date is: %s' % up_doc['createdAt'])
            else:
                print('Error: failed to add inserted date')
        else:
            print('Error: failed to insert new updates to db')

    def _find_movie_in_db(self, movie_id):
        return self.movie_col.find_one({'eiga_movie_id': movie_id})

    def _update_movie(self, oid, update):
        self.movie_col.update_one({'_id': oid}, {'$set': update}, upsert=True)


class UpdateGalleryPipeline(MongoDBPipeline):
    def open_spider(self, spider):
        self.movie_col = self.db['eiga_movies']
        self.update_col = self.db['updates']

    def process_item(self, item, spider):

        movie = self.movie_col.find_one({'eiga_movie_id': item['eiga_movie_id']})

        if movie is None:
            raise DropItem('movie (%s) not found in DB, skip updating gallery' % item['eiga_movie_id'])
        else:
            print('movie exists, add gallery')
            self.movie_col.update_one({'eiga_movie_id': item['eiga_movie_id']},
                                        {
                                            '$set': {
                                                'gallery': item['gallery']
                                            },
                                            '$currentDate': {
                                                'lastModified': True
                                            }
                                        })
        return item
