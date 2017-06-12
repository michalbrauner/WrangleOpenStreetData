import re
from dateutil.parser import parse

BASIC_FIELDS_TO_GET = ['changeset', 'id', 'timestamp', 'uid', 'user', 'version']
NODE_FIELDS_TO_GET = BASIC_FIELDS_TO_GET + ['lat', 'lon']
WAY_FIELDS_TO_GET = BASIC_FIELDS_TO_GET
RELATION_FIELDS_TO_GET = BASIC_FIELDS_TO_GET

FLOAT_REG = '^[0-9]+([.]{1}[0-9]+){0,1}$'
INT_REG = '^[0-9]+$'
VALID_TAG_NAME = re.compile(r'^([a-z_]+)+([:]{1}([a-z_0-9]+))*$')


def clean_tag_name(tag_name):
    if tag_name.lower() == 'fixme':
        tag_name = tag_name.lower()

    return tag_name


def get_tags(node):
    tags = []

    for tag in node.iter("tag"):
        tag_name = tag.get('k')

        # @see http://wiki.openstreetmap.org/wiki/Key:uir_adr:ADRESA_KOD
        if not re.match(VALID_TAG_NAME, tag_name) and tag_name not in ['uir_adr:ADRESA_KOD', 'FIXME']:
            raise ValueError('Invalid tag name \'{}\' for id={}'.format(tag_name, node.get('id')))

        tags.append({'key': clean_tag_name(tag_name), 'value': tag.get('v')})

    return {'tags': tags}


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

    final_node['tags'] = get_tags(node)

    return final_node


def clean_node(node):
    final_data = clean_fields(node, NODE_FIELDS_TO_GET)

    return final_data


def clean_way(way):
    final_data = clean_fields(way, WAY_FIELDS_TO_GET)

    return final_data


def clean_relation(relation):
    final_data = clean_fields(relation, RELATION_FIELDS_TO_GET)

    return final_data
