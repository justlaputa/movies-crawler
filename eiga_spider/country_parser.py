import json

with open('country_dict.json') as country_data:
    country_dict = json.load(country_data)

class CountryParser:

    def __init__(self):
        self.country_dict = self._build_country_dict()

    def _build_country_dict(self):
        with open('iso-3166-1_ja.json') as data:
            country_data = json.load(data)
        country_dict = {}

        for country in country_data:
            ja_names = [country['name']['ja']]
            ja_names.extend(country['name'].get('ja_alt', []))
            for ja_name in ja_names:
                country_dict[ja_name] = {
                    "iso_3166_1": country['iso_3166_2'],
                    'en_name': country['name']['en']
                }
        return country_dict

    def parse(self, country_text):
        if country_text.find('合作') >= 0:
            country_text = country_text[:country_text.find('合作')]

        if '・' in country_text:
            country_text = country_text.split('・')
        else:
            country_text = [country_text]

        for country in country_text:
            if not country in self.country_dict:
                print('not found country: %s' % country)
                self.country_dict[country] = None

        return [self.country_dict[c] for c in country_text if c in self.country_dict]