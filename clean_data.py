import configuration
import xml.etree.cElementTree as ET
import pprint
import data_cleaner.data_cleaner as dc
import io, json, os, sys
import pymongo


INVALID_ITEMS_FILE = 'invalid_items.json'
SAVE_TO_DATABASE_EACH_N_ELEMENTS = 10000


def insert_elements_into_db(client, elements):
    db = client.openstreetmap

    if len(elements) == 0:
        return []

    result = db.elements.insert_many(elements)

    return result.inserted_ids


def address_is_empty(address):
    none_values = 0
    total_values = len(address)

    for field_name in address:
        if address[field_name] is None:
            none_values = none_values + 1

    return none_values == total_values


def address_is_not_complete(address):

    for field_name in address:
        if address[field_name] is None:
            return True

    return False


def main():
    osm_file_path = configuration.OSM_FILE

    osm_file_size = os.path.getsize(osm_file_path)
    osm_file = open(osm_file_path, 'r', encoding='utf8')

    elements = []
    elements_count = {'node': 0, 'way': 0, 'relation': 0, 'skipped': 0, 'total': 0, 'fixme_tag_count': 0}
    non_existing_node_types = set([])

    elements_structure = []

    address_not_empty_count = 0
    address_not_empty_and_not_complete_count = 0

    invalid_items_stream = io.open(INVALID_ITEMS_FILE, 'w', encoding='utf-8')
    invalid_items_stream.write('[')

    osm_file.seek(0)

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    client.openstreetmap.elements.drop()
    pymongo.write_concern.WriteConcern(w=1, j=False)

    print('Phase 1 - data cleaning and saving into database')
    print('----------------------------')
    sys.stdout.flush()

    for event, elem in ET.iterparse(osm_file, events=['start', 'end']):

        fixme_count_in_element = 0

        current_position = osm_file.tell()
        completed = round(100 * (current_position / osm_file_size))

        print(' --> {}% complete                           '.format(completed), end="\r")
        sys.stdout.flush()

        try:
            if event == 'start':
                if elem.tag == 'node':
                    element, fixme_count_in_element = dc.clean_node(elem)

                    elements_count[elem.tag] = elements_count[elem.tag] + 1
                    elements_count['total'] = elements_count['total'] + 1

                    if fixme_count_in_element > 0:
                        elements_count['skipped'] = elements_count['skipped'] + 1
                    elif not address_is_empty(element['address']) \
                            and address_is_not_complete(element['address']):

                        write_to_invalid_stream_file(address_not_empty_and_not_complete_count, element,
                                                     invalid_items_stream)

                        address_not_empty_and_not_complete_count = address_not_empty_and_not_complete_count + 1
                        address_not_empty_count = address_not_empty_count + 1

                        elements_count['skipped'] = elements_count['skipped'] + 1
                    else:
                        if not address_is_empty(element['address']):
                            address_not_empty_count = address_not_empty_count + 1

                        elements.append(element)

                elif elem.tag == 'way':
                    element, fixme_count_in_element = dc.clean_way(elem)

                    elements_count[elem.tag] = elements_count[elem.tag] + 1
                    elements_count['total'] = elements_count['total'] + 1

                    if fixme_count_in_element == 0:
                        elements.append(element)
                    else:
                        elements_count['skipped'] = elements_count['skipped'] + 1

                elif elem.tag == 'relation':
                    element, fixme_count_in_element = dc.clean_relation(elem)

                    elements_count[elem.tag] = elements_count[elem.tag] + 1
                    elements_count['total'] = elements_count['total'] + 1

                    if fixme_count_in_element == 0:
                        elements.append(element)
                    else:
                        elements_count['skipped'] = elements_count['skipped'] + 1

                elif elem.tag != 'osm' and len(elements_structure) == 1:
                    non_existing_node_types.add(elem.tag)

                elements_count['fixme_tag_count'] = elements_count['fixme_tag_count'] + fixme_count_in_element
        except ValueError as e:
            element = dict(error=e.__str__(), tag=elem.tag, tag_id=elem.get('id'))

            write_to_invalid_stream_file(address_not_empty_and_not_complete_count, element, invalid_items_stream)

        if event == 'start':
            elements_structure.append(elem)

        if event == 'end':
            parent = elements_structure.pop()
            parent.clear()

        elem.clear()

        if len(elements) >= SAVE_TO_DATABASE_EACH_N_ELEMENTS:

            print(' --> {}% complete (writing to database)'.format(completed), end="\r")
            sys.stdout.flush()

            insert_elements_into_db(client, elements)

            del elements[:]
            del elements
            elements = []

    insert_elements_into_db(client, elements)

    invalid_items_stream.write(']')
    invalid_items_stream.close()

    print('\n\nPhase 2 - stats')
    print('----------------------------\n')

    pprint.pprint('Unknown element types:')
    pprint.pprint(non_existing_node_types)
    pprint.pprint('---')
    pprint.pprint('Element counts:')
    pprint.pprint(elements_count)
    pprint.pprint('---')
    pprint.pprint('Not complete addresses / total addresses:')
    pprint.pprint('{} / {}'.format(address_not_empty_and_not_complete_count, address_not_empty_count))


def write_to_invalid_stream_file(address_not_empty_and_not_complete_count, element, invalid_items_stream):
    if address_not_empty_and_not_complete_count > 0:
        invalid_items_stream.write(',')

    invalid_items_stream.write(json.dumps(convert_element_to_serializable(element)))


def convert_element_to_serializable(element):
    element_to_save = element

    if 'timestamp' in element_to_save:
        element_to_save['timestamp'] = element_to_save['timestamp'].timestamp()

    return element_to_save


if __name__ == "__main__":
    main()
