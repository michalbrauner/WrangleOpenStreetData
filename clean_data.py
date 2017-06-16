import configuration
import xml.etree.cElementTree as ET
import pprint
import data_cleaner.data_cleaner as dc
import io, json

INVALID_ADDRESS_FILE = 'invalid_address.json'

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
    osm_file = open(configuration.SAMPLE_FILE, 'r', encoding='utf8')
    elements = []
    elements_count = {'node': 0, 'way': 0, 'relation': 0, 'fixme_tag_count': 0}
    non_existing_node_types = set([])

    elements_structure = []

    address_not_empty_count = 0
    address_not_empty_and_not_complete_count = 0

    invalid_addressess_stream = io.open(INVALID_ADDRESS_FILE, 'w', encoding='utf-8')
    invalid_addressess_stream.write('[')

    for event, elem in ET.iterparse(osm_file, events=['start', 'end']):
        element = {}
        fixme_count_in_element = 0

        if event == 'start':
            if elem.tag == 'node':
                element['type'] = 'node'
                element['data'], fixme_count_in_element = dc.clean_node(elem)
                elements_count[elem.tag] = elements_count[elem.tag] + 1
                elements.append(element)

                if not address_is_empty(element['data']['address']):
                    address_not_empty_count = address_not_empty_count + 1
                    if address_is_not_complete(element['data']['address']):
                        if address_not_empty_and_not_complete_count > 0:
                            invalid_addressess_stream.write(',')

                        element_to_save = element['data']
                        element_to_save['timestamp'] = element_to_save['timestamp'].timestamp()
                        invalid_addressess_stream.write(json.dumps(element_to_save))

                        address_not_empty_and_not_complete_count = address_not_empty_and_not_complete_count + 1


            elif elem.tag == 'way':
                element['type'] = 'way'
                element['data'], fixme_count_in_element = dc.clean_way(elem)
                elements_count[elem.tag] = elements_count[elem.tag] + 1
                elements.append(element)

            elif elem.tag == 'relation':
                element['type'] = 'relation'
                element['data'], fixme_count_in_element = dc.clean_relation(elem)
                elements_count[elem.tag] = elements_count[elem.tag] + 1
                elements.append(element)

            elif elem.tag != 'osm' and len(elements_structure) == 1:
                non_existing_node_types.add(elem.tag)

            elements_count['fixme_tag_count'] = elements_count['fixme_tag_count'] + fixme_count_in_element

        if event == 'start':
            elements_structure.append(elem.tag)

        if event == 'end':
            elements_structure.pop()

    invalid_addressess_stream.write(']');
    invalid_addressess_stream.close()

    pprint.pprint('Unknown element types:')
    pprint.pprint(non_existing_node_types)
    pprint.pprint('---')
    pprint.pprint('Element counts:')
    pprint.pprint(elements_count)
    pprint.pprint('---')
    pprint.pprint('Not complete addresses / total addresses:')
    pprint.pprint('{} / {}'.format(address_not_empty_and_not_complete_count, address_not_empty_count))

if __name__ == "__main__":
    main()
