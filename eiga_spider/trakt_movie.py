import trakt
from trakt.movies import Movie

import sys

trakt.init('laputa',
           client_id='cfe296e5568274da416c68b42577c7719c1236e66fa766bd4a9228c871f851c5',
           client_secret='68285158542c967a9158118e5dd8accdb45187b61849a1eb6f7ff0ed471241a0')

class TraktMovie:

    @classmethod
    def get_movie(self, title, year=None):
        try:
            result = Movie.search(title, year)
            if len(result) > 0:
                return result[0]
            else:
                return None
        except TypeError as e:
            print('the movie title only contains non-ascii character, PyTrakt can not handle this: %s' % title)
            return None

def main(args):
    from pprint import pprint
    trakt_movie = TraktMovie()
    movie = trakt_movie.get_movie(args[1])
    pprint(movie.ids)
    pprint(movie.ids['ids']['trakt'])

if __name__ == '__main__':
    sys.exit(main(sys.argv))