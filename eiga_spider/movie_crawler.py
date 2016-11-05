from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import sys

class MovieCrawler():
    """
    Run scrapy crawler "get_movies" to get all movies metadata from eiga.com
    include downloading gallery images
    """

    def __init__(self, settings):
        self.scrapy_settings = settings

    def run(self, in_theater_ids, out_theater_ids):
        process = CrawlerProcess(self.scrapy_settings)
        process.crawl('get_movies', in_theater_ids, out_theater_ids)
        process.start()
        process.stop()

def main(args):
    in_ids = ['83278']
    out_ids = ['84410']
    if len(args) > 1:
        in_ids = args[1].split(',')
    if len(args) > 2:
        out_ids = args[2].split(',')

    crawler = MovieCrawler(get_project_settings())
    crawler.run(in_ids, out_ids)

if __name__ == '__main__':
    sys.exit(main(sys.argv))
