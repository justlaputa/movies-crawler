import trakt
from trakt.movies import Movie

import sys

trakt.init('laputa',
           client_id='cfe296e5568274da416c68b42577c7719c1236e66fa766bd4a9228c871f851c5',
           client_secret='68285158542c967a9158118e5dd8accdb45187b61849a1eb6f7ff0ed471241a0')

class MovieTrakt:
    """
    Get movie information from trakt api
    """
    def get_trakt_info(self, eiga_movie):
        logging.debug('search trakt for movie %s', eiga_movie['title_jp'])
        original_title = eiga_movie['movie_data'].get('原題'.decode('utf8'), None)
        year = eiga_movie['movie_data'].get('製作年'.decode('utf8'), None)
        logging.debug('search trakt for movie [%s](%s)', original_title, year)
        if original_title is None:
            logging.warning('original title does not exist, skip get trakt info')
            return None
        else:
            if year is not None:
                year = year[:4]
        try:
            result = Movie.search(original_title, year)
            if len(result) > 0:
                return result[0]
            else:
                return None
        except TypeError as e:
            logging.error('the movie title only contains non-ascii character, '
                          'PyTrakt can not handle this: %s' % title)
            return None

def main(args):
    from pprint import pprint
    trakt_movie = MovieTrakt()
    movie = trakt_movie.get_movie(args[1])
    pprint(movie.ids)
    pprint(movie.ids['ids']['trakt'])

if __name__ == '__main__':
    sys.exit(main(sys.argv))
