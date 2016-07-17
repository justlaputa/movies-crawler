import unittest
import pymongo
from eiga_spider.pipelines import NewMoviesPipeline

class TestNewMovieSpider(unittest.TestCase):

    def setUp(self):
        newmovie_pipeline = NewMoviesPipeline({}, {})
        newmovie_pipeline.old_in_theater_movie_ids = set(['84410', '83805', '83708', '83700', '83316'])
        newmovie_pipeline.old_out_theater_movie_ids = set(['84204', '84418', '84165', '82788', '81060'])

        self.newmovie_pipeline = newmovie_pipeline

    def test_calculate_updates_new_opened(self):
        self.newmovie_pipeline.new_in_theater_movie_ids = set(['84204', '84418', '83708', '85161'])
        self.newmovie_pipeline.new_scheduled_movie_ids = set(['84204', '87161'])

        updates = self.newmovie_pipeline._calculate_updates()
        self.assertItemsEqual(updates['opened'], ['84204', '84418'])

    def test_calculate_updates_new_closed(self):
        self.newmovie_pipeline.new_in_theater_movie_ids = set(['84410', '83805', '85160'])
        self.newmovie_pipeline.new_scheduled_movie_ids = set(['84204', '83708', '87710'])

        updates = self.newmovie_pipeline._calculate_updates()
        self.assertItemsEqual(updates['closed'], ['83708', '83700', '83316'])

    def test_calculate_updates_new_in_theater(self):
        self.newmovie_pipeline.new_in_theater_movie_ids = set(['84410', '81060', '85160', '85161'])
        self.newmovie_pipeline.new_scheduled_movie_ids = set(['84204', '83708', '85160'])

        updates = self.newmovie_pipeline._calculate_updates()
        self.assertItemsEqual(updates['new']['in_theater'], ['85160', '85161'])

    def test_calculate_updates_new_out_theater(self):
        self.newmovie_pipeline.new_in_theater_movie_ids = set(['84410', '81060'])
        self.newmovie_pipeline.new_scheduled_movie_ids = set(['84204', '83805', '81060', '86100'])

        updates = self.newmovie_pipeline._calculate_updates()
        self.assertItemsEqual(updates['new']['out_theater'], ['86100'])


if __name__ == '__main__':
    unittest.main()
