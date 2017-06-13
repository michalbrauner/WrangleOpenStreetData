import configuration
import xml.etree.cElementTree as ET
import pprint
import data_cleaner.data_cleaner as dc


def main():
    osm_file = open(configuration.SAMPLE_FILE, 'r', encoding='utf8')
    elements = []
    elements_count = {'node': 0, 'way': 0, 'relation': 0, 'fixme_tag_count': 0}
    non_existing_node_types = set([])

    elements_structure = []

    for event, elem in ET.iterparse(osm_file, events=['start', 'end']):
        element = {}
        fixme_count_in_element = 0

        if event == 'start':
            if elem.tag == 'node':
                element['type'] = 'node'
                element['data'], fixme_count_in_element = dc.clean_node(elem)
                elements_count[elem.tag] = elements_count[elem.tag] + 1
                elements.append(element)

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

    pprint.pprint('Unknown element types:')
    pprint.pprint(non_existing_node_types)
    pprint.pprint('---')
    pprint.pprint('Element counts:')
    pprint.pprint(elements_count)

if __name__ == "__main__":
    main()
