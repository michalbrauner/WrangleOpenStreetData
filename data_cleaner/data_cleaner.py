import re
import unicodedata
from dateutil.parser import parse

BASIC_FIELDS_TO_GET = ['changeset', 'id', 'timestamp', 'uid', 'user', 'version']
NODE_FIELDS_TO_GET = BASIC_FIELDS_TO_GET + ['lat', 'lon']
WAY_FIELDS_TO_GET = BASIC_FIELDS_TO_GET
RELATION_FIELDS_TO_GET = BASIC_FIELDS_TO_GET

FLOAT_REG = '^[0-9]+([.]{1}[0-9]+){0,1}$'
INT_REG = '^[0-9]+$'
VALID_TAG_NAME = re.compile(r'^(([A-Za-z_0-9\-]+){1}([:]{1}([A-Za-z_0-9.\-]+))*)$')
VALID_HOUSE_NUMBER = '([0-9]+[0-9a-zA-Z]*){1}([/]{1}[0-9a-zA-Z\-]+){0,1}'
VALID_STREET_NAME = '[^;]+'
VALID_POSTCODE = '[0-9]{5}'


def strip_diacritic(value):
    normalized = unicodedata.normalize("NFKD", value)

    return "".join(c for c in normalized if unicodedata.category(c) != "Mn")


def clean_tag_name(tag_name):
    return tag_name.lower()


def is_address_tag(tag_name):
    return tag_name.lower().startswith('addr:')


def check_and_clean_street_name(street):

    street_more_parts = re.match(r'^({});({})$'.format(VALID_STREET_NAME, VALID_STREET_NAME), street)
    if street_more_parts:
        street = street_more_parts.group(1)

    if not re.match(r'^{}$'.format(VALID_STREET_NAME), street):
        raise ValueError('Street \'{}\' has an invalid name'.format(street))

    return street


def check_and_clean_country(country):
    country = country.upper()

    if country != 'CZ':
        raise ValueError('Country \'{}\' is invalid'.format(country))

    return country


def check_and_clean_postcode(postcode):

    postcode = re.sub(r'[\s]', '', postcode)

    if postcode.startswith('CZ-'):
        postcode = postcode.replace('CZ-', '')

    if postcode.startswith('CZ'):
        postcode = postcode.replace('CZ', '')

    postcode = re.sub(r'Praha[0-9]{1,2}$', '', postcode)

    postcode_more_parts = re.match(r'^({});({})$'.format(VALID_POSTCODE, VALID_POSTCODE), postcode)
    if postcode_more_parts:
        postcode = postcode_more_parts.group(1)

    if not re.match(r'^{}$'.format(VALID_POSTCODE), postcode):
        raise ValueError('Postcode \'{}\' is invalid'.format(postcode))

    return postcode


def check_and_clean_housenumber(housenumber):

    housenumber = re.sub(r'^ev[.]{1}', '', housenumber)

    if housenumber.startswith('?/'):
        housenumber = housenumber.replace('?/', '')

    if re.match(r'^/[0-9]+$', housenumber):
        housenumber = housenumber.replace('/', '')

    housenumber = re.sub(r'[\s]', '', housenumber)
    housenumber = re.sub(r'[,;][\w]+[.][\w]+', '', housenumber)

    housenumber_more_parts = re.match(r'^({});({})$'.format(VALID_HOUSE_NUMBER, VALID_HOUSE_NUMBER), housenumber)
    if housenumber_more_parts:
        housenumber = housenumber_more_parts.group(1)

    if not re.match(r'^{}$'.format(VALID_HOUSE_NUMBER), housenumber):
        raise ValueError('Housenumber \'{}\' is invalid'.format(housenumber))

    return housenumber


def get_tags(node):
    tags = []
    address = {'street': None, 'country': None, 'postcode': None, 'housenumber': None}

    fixme_count = 0

    for tag in node.iter("tag"):
        tag_name = strip_diacritic(tag.get('k'))

        # @see http://wiki.openstreetmap.org/wiki/Key:uir_adr:ADRESA_KOD
        if not re.match(VALID_TAG_NAME, tag_name):
            raise ValueError('Invalid tag name \'{}\' for id={}'.format(tag_name, node.get('id')))

        clear_tag_name = clean_tag_name(tag_name)

        tag_value = tag.get('v')

        if clear_tag_name == 'fixme':
            fixme_count = fixme_count + 1
        elif clear_tag_name == 'addr:street':
            address['street'] = check_and_clean_street_name(tag_value)
        elif clear_tag_name == 'addr:country':
            address['country'] = check_and_clean_country(tag_value)
        elif clear_tag_name == 'addr:postcode':
            address['postcode'] = check_and_clean_postcode(tag_value)
        elif clear_tag_name == 'addr:housenumber':
            address['housenumber'] = check_and_clean_housenumber(tag_value)

        if not is_address_tag(tag_name):
            tags.append({'key': clear_tag_name, 'value': tag.get('v')})

    # In case we have some address and not filled country, we use default value 'CZ'
    if address['country'] is None:
        for field_name in address:
            if address[field_name] is not None:
                address['country'] = 'CZ'

    # If there is only empty street but everything is set, we assume we are able to identify the place by postcode
    # and also by ref:ruian:addr tag
    if address['country'] is not None and address['street'] is None:
        for tag in tags:
            if tag['key'] in ['ref:ruian:addr', 'ref:ruian']:
                address['street'] = ''
                break

    return {'tags': tags, 'address': address}, fixme_count


def parse_float(coord_as_string):
    if not re.match(FLOAT_REG, coord_as_string):
        raise ValueError('Value \'{}\' is not valid float number', coord_as_string)

    return float(coord_as_string)


def parse_number(number_as_string):
    if not re.match(INT_REG, number_as_string):
        raise ValueError('Value \'{}\' is not valid int number', number_as_string)

    return int(number_as_string)


def parse_datetime(datetime_as_string):
    return parse(timestr=datetime_as_string)


def clean_fields(node, fields):
    final_node = {}

    for field in fields:
        field_value = node.get(field)
        field_id = node.get('id')

        if field_value is None:
            raise ValueError('Field \'{}\' should be defined for id={}'.format(field, field_id))

        if field in ['changeset', 'id', 'uid', 'version']:
            final_node[field] = parse_number(node.get(field))
        elif field in ['lat', 'lon']:
            final_node[field] = parse_float(node.get(field))
        elif field in ['timestamp']:
            final_node[field] = parse_datetime(node.get(field))
        else:
            final_node[field] = node.get(field).strip()

    tags, fixme_count = get_tags(node)

    final_node['tags'] = tags['tags']
    final_node['address'] = tags['address']
    final_node['type'] = node.tag

    return final_node, fixme_count


def clean_node(node):
    final_data = clean_fields(node, NODE_FIELDS_TO_GET)

    return final_data


def clean_way(way):
    final_data = clean_fields(way, WAY_FIELDS_TO_GET)

    return final_data


def clean_relation(relation):
    final_data = clean_fields(relation, RELATION_FIELDS_TO_GET)

    return final_data
