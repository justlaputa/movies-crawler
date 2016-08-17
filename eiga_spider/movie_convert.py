# -*- coding: utf-8 -*-

import logging

class MovieConverter():
    logger = logging.getLevelName(__name__)

    def __init__(self):
        pass

    def get_web_movie(self, eiga_movie):
        self.logger.info('converting from eiga movie: [%s]', eiga_movie['title_jp'])

        movie = {}

        self._fill_in_from_eiga_movie(eiga_movie, movie)

        if (self._is_foreign_movie(eiga_movie)):
            print('\tforeign movie: [%s](%s)' % (movie['original_title'], movie['production_year']))

            trakt_movie = self.trakt.get_movie(movie['original_title'], movie['production_year'])

            if trakt_movie is None:
                print('\tcould not found trakt movie')
            else:
                print('\tfound trakt movie: %d: %s' % (trakt_movie.ids['ids']['trakt'], trakt_movie.ids['ids']['slug']))

            self._fill_foreign_movie_by_trakt(eiga_movie, movie, trakt_movie)

        print('finish\n')

        return movie

    def _is_foreign_movie(self, eiga_movie):
        return '原題' in eiga_movie['movie_data'] and eiga_movie['movie_data']['原題'] != ''

    def _fill_in_from_eiga_movie(self, eiga_movie, movie):
        movie_data = eiga_movie['movie_data']

        movie['original_title'] = movie_data.get('原題', None) or eiga_movie['title_jp']
        movie['country_titles'] = [
            {
                'iso_3166_1': 'JP',
                'title': eiga_movie['title_jp']
            }
        ]
        movie['production_year'] = self._get_production_year(movie_data)
        movie['homepage'] = movie_data.get('official_site', '')
        movie['external_ids'] = {'eiga': self._get_eiga_id(eiga_movie)}
        movie['release_info'] = [
            {
                "iso_3166_1": "JP",
                "content_rating": self._parse_content_rating(movie_data),
                "release_date": self._parse_release_date(eiga_movie['release_date_jp']),
                "runtime": self._parse_release_runtime(movie_data)
            }
        ]
        movie['production_countries'] = self._get_production_countries(movie_data)
        movie['images'] = {
            'posters': [],
            'fanarts': []
        }

        if eiga_movie.get('poster_url') is not None:
            movie['images']['posters'].append({
                'iso_3166_1': 'JP',
                'full': eiga_movie.get('poster_url')
            })
        if eiga_movie.get('gallery') is not None:
            movie['images']['fanarts'].extend([{'full': url} for url in eiga_movie['gallery']])

    def _get_production_countries(self, movie_data):
        return self.country_parser.parse(movie_data['製作国'])

    def _get_production_year(self, movie_data):
        return movie_data['製作年'][:4]

    def _get_eiga_id(self, eiga_movie):
        try:
            return int(eiga_movie['eiga_movie_id'])
        except ValueError:
            return None

    def _fill_foreign_movie_by_trakt(self, eiga_movie, movie, trakt_movie):

        if trakt_movie is not None:
            movie['images']['posters'].insert(0, {
                'iso_3166_1': 'US',
                'full': trakt_movie.images['poster']['full'],
                'thumb': trakt_movie.images['poster']['thumb']
            })
            movie['images']['fanarts'].insert(0, {
                'full': trakt_movie.images['fanart']['full'],
                'thumb': trakt_movie.images['fanart']['thumb']
            })
            movie['country_titles'].append({
                'iso_3166_1': 'US',
                'title': trakt_movie.title
            })
            movie['production_year'] = trakt_movie.year
            movie['external_ids'].update(trakt_movie.ids['ids'])

        return

    def _parse_content_rating(self, movie_data):
        return movie_data.get('映倫区分', None)

    def _parse_release_runtime(self, movie_data):
        if '上映時間' in movie_data:
            try:
                return int(movie_data['上映時間'][:-1])
            except ValueError:
                return None
        return None

    def _parse_release_date(self, date_str):
        if date_str is None:
            return None
        try:
            return dateparser.parse(date_str, languages=['ja'])
        except ValueError:
            return None
