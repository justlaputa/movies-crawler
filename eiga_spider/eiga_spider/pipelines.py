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


class JsonExportPipeline(object):
    def __init__(self):
        self.file = io.open('japan_movies.json', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(line)
        return item

    def spider_closed(self, spider):
        self.file.close()
