import unittest
import json
from movie_convert import MovieConverter

class TestMovieConverter(unittest.TestCase):
    def setUp(self):
        with open('fixtures/eiga_movie.json') as file:
            eiga_movie = json.load(file)
        self.eiga_movie = eiga_movie
        self.movie_converter = MovieConverter()

    def testConverter(self):
        web_movie = self.movie_converter.get_web_movie(self.eiga_movie)


if __name__ == '__main__':
    unittest.main()
