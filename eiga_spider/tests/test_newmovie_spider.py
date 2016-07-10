import unittest
import pymongo
from eiga_spider.spiders.newMovieSpider import UpdateMovieSpider
from responses import fake_response_from_file

class TestNewMovieSpider(unittest.TestCase):

    def setUp(self):
        self.spider = UpdateMovieSpider()

    def test_start_requests(self):
        requests = self.spider.start_requests()
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0].url, 'http://eiga.com/now/')
        self.assertEqual(requests[1].url, 'http://eiga.com/coming/')

    def test_parse_one_playing_page(self):
        response = fake_response_from_file('now.html', 'http://eiga.com/now/')
        movie_ids = {"82416", "83989", "84660", "84613", "83940", "83401", "82200", "81957", "83984", "85051", "82859", "84894", "85040", "85204", "85065", "85130", "85029", "81531", "79719", "82707"}
        results = self.spider.parse_one_playing_page(response)

        for m in results:
            self.assertTrue(m['in_theater'])

        parsed_ids = [m['id'] for m in results]
        self.assertEqual(len(parsed_ids), 20)
        self.assertEqual(set(parsed_ids), movie_ids)

    def test_get_next_playing_page_info(self):
        response = fake_response_from_file('now.html', 'http://eiga.com/now/')
        (next_page_no, next_page_url) = self.spider.get_next_playing_page_info(response)

        self.assertEqual(next_page_no, 2)
        self.assertEqual(next_page_url, 'http://eiga.com/now/all/release/2/')

    def test_get_next_playing_page_info_on_last_page(self):
        response = fake_response_from_file('now_18.html', 'http://eiga.com/now/')
        (next_page_no, next_page_url) = self.spider.get_next_playing_page_info(response)

        self.assertEqual(next_page_no, -1)
        self.assertEqual(next_page_url, None)

    def test_parse_one_scheduled_page(self):
        response = fake_response_from_file('coming.html', 'http://eiga.com/coming/')
        movie_ids = {"79353","82706","83875","84631","84477","85056","84451","84614","84834","84892","84842","85140","84500","85205","46697","84600","85066","81783","84978","85160","83801","85097","83738","83222","84186","84847","85178","84841","84844","84836","84848","84846","82996","82020","84851","84858","84843","84855","83980","81388","84837","83938","84802","85114","82812","84298","85212","84824","8002","9134","84796","84719","84971","83015","84953","83951","30164","84831","84774","84954","84912","84838","84832","84830","84854","81507","84852","84850","79978","82152","84480","84827","83238","83642","83151","83981","80040","83904","83158","82363","84839","84632","83941","81042","82938","84849","84856"}
        results = self.spider.parse_one_scheduled_page(response)

        for m in results:
            self.assertFalse(m['in_theater'])

        parsed_ids = [m['id'] for m in results]
        self.assertEqual(len(parsed_ids), len(movie_ids))
        self.assertEqual(set(parsed_ids), movie_ids)

if __name__ == '__main__':
    unittest.main()
