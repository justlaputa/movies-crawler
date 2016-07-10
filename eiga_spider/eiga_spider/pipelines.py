# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import io
import json
import pymongo

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
            print 'movie exists, add gallery'
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

class UpdateGalleryPipeline(MongoPipeline):
    def process_item(self, item, spider):
        movie_collection = self.db[self.collection_name]

        movie = movie_collection.find_one({'eiga_movie_id': item['eiga_movie_id']})

        if movie is None:
            movie_collection.insert_one(dict(item))
        else:
            print 'movie exists, add gallery'
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

class MongoDBPipeline(object):
    def __init__(self, mongo_uri, mongo_db):
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[mongo_db]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'movies')
        )

    def close_spider(self, spider):
        self.client.close()

class NewMoviesPipeline(MongoDBPipeline):
    in_theater_movie_ids = []
    updates = {
        'new_open': [],
        'closed': [],
        'new_movie': []
    }

    def open_spider(self, spider):
        self.movie_col = self.db['eiga_movies']
        self.update_col = self.db['updates']

        movies = self.movie_col.find({'in_theater': True}, {'eiga_movie_id': 1})
        self.in_theater_movie_ids = [m['eiga_movie_id'] for m in movies]

    def process_item(self, item, spider):
        exist_movie = self._find_movie_in_db(item['id'])

        if exist_movie:
            if item['in_theater'] and exist_movie['in_theater']:
                print('open movie already in theater (%s)' % item['id'])
                self.in_theater_movie_ids.remove(item['id'])
            elif item['in_theater'] and not exist_movie['in_theater']:
                print('found newly opened movie (%s)' % item['id'])
                self.updates['new_open'].append(item['id'])
            elif not item['in_theater'] and exist_movie['in_theater']:
                print('found closed movie (%s)' % item['id'])
                self.updates['closed'].append(item['id'])
                self.in_theater_movie_ids.remove(item['id'])
        else:
            self.updates['new_movie'].append(item['id'])
        return item

    def close_spider(self, spider):
        for id in self.in_theater_movie_ids:
            self.updates['closed'].append(id)

        print('all updates:')
        print(self.updates)
        self.update_col.insert_one(self.updates)

        super(self.__class__, self).close_spider(spider)

    def _find_movie_in_db(self, movie_id):
        return self.movie_col.find_one({'eiga_movie_id': movie_id})

    def _update_movie(self, oid, update):
        self.movie_col.update_one({'_id': oid}, {'$set': update}, upsert=True)

class JsonExportPipeline(object):
    def __init__(self):
        self.file = io.open('japan_movies.json', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(line)
        return item

    def spider_closed(self, spider):
        self.file.close()
