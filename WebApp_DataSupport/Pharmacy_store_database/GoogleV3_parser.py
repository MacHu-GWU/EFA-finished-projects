##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-12

class googleV3_location(object):
    def __init__(self, raw):
        self.street_number = self.search_address_components('street_number', raw)
        self.route = self.search_address_components('route', raw)
        self.neighborhood = self.search_address_components('neighborhood', raw)
        self.locality = self.search_address_components('locality', raw)
        self.administrative_area_level_3 = self.search_address_components('administrative_area_level_3', raw)
        self.administrative_area_level_2 = self.search_address_components('administrative_area_level_2', raw)
        self.administrative_area_level_1 = self.search_address_components('administrative_area_level_1', raw)
        self.country = self.search_address_components('country', raw)
        self.postal_code = self.search_address_components('postal_code', raw)
        self.formatted_address = raw.get('formatted_address', None)
        self.latitude = raw['geometry']['location']['lat']
        self.longitude = raw['geometry']['location']['lng']
        self.raw = raw
        
    def search_address_components(self, types, raw):
        for component in raw['address_components']:
            if types in component['types']:
                return component['long_name']
        return None

if __name__ == '__main__':
    from HSH.Data.jt import *
    raw = load_jt(r'api_example_json\GoogleV3_example.json')
    prt_jt(raw)
    loc = googleV3_location(raw)
    print loc.street_number
    print loc.route
    print loc.neighborhood
    print loc.locality
    print loc.administrative_area_level_3
    print loc.administrative_area_level_2
    print loc.administrative_area_level_1
    print loc.country
    print loc.postal_code
    print loc.formatted_address
    print loc.latitude
    print loc.longitude 