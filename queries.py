import pymongo


def main():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client.openstreetmap

    print('Number of valid items: {}'.format(db.elements.count()))
    print('Number of valid nodes: {}'.format(number_of_elements(db, 'node')))
    print('Number of valid ways: {}'.format(number_of_elements(db, 'way')))
    print('Number of valid relations: {}'.format(number_of_elements(db, 'relation')))
    print('Number of unique authors: {}'.format(number_of_unique_items(db, 'uid')))

    get_and_print_user_with_biggest_contribution(db)
    get_and_print_three_most_frequent_shops(db)

    print('Number of unique changesets: {}'.format(number_of_unique_items(db, 'changeset')))
    print('Number of items that have a source "survey": {}'.format(
        get_number_of_elements_having_a_tag(db, 'source', 'survey')))

    print('Number of restaurants: {}'.format(get_number_of_elements_having_a_tag(db, 'amenity', 'restaurant')))
    print('Number of bakeries: {}'.format(get_number_of_elements_having_a_tag(db, 'shop', 'bakery')))


def get_number_of_elements_having_a_tag(db, tag_name, tag_value):
    elements_with_source_survey = db.elements.aggregate([
        {'$unwind': "$tags"},
        {'$match': {
            '$and': [
                {'tags.key': tag_name},
                {'tags.value': tag_value}
            ]
        }},
        {'$group': {'_id': 'id', 'count': {'$sum': 1}}}
    ])

    return list(elements_with_source_survey)[0]['count']


def get_and_print_three_most_frequent_shops(db):
    three_most_frequent_types_of_shop = db.elements.aggregate([
        {'$unwind': "$tags"},
        {'$match': {'tags.key': 'shop'}},
        {'$group': {'_id': '$tags.value', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 3}
    ])
    print(list(three_most_frequent_types_of_shop))


def number_of_elements(db, node_type):
    count_of_type = db.elements.aggregate([
        {'$match': {'type': node_type}},
        {'$group': {'_id': None, 'count': {'$sum': 1}}}
    ])

    return list(count_of_type)[0]['count']


def number_of_unique_items(db, by_field):
    count_of_items = db.elements.aggregate([
        {'$group': {'_id': '${}'.format(by_field), 'count': {'$sum': 1}}},
        {'$group': {'_id': None, 'count': {'$sum': 1}}}
    ])

    return list(count_of_items)[0]['count']


def get_and_print_user_with_biggest_contribution(db):
    author_with_biggest_number_of_elements = db.elements.aggregate([
        {'$group': {'_id': '$uid', 'name': {'$first': '$user'}, 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 1}
    ])
    stats = list(author_with_biggest_number_of_elements)[0]
    contribution_rate = round((stats['count'] / db.elements.count()) * 100)
    print(
        'The biggest contribution has the user \'{}\' with id \'{}\' - he created \'{}\' elements ({}% contribution rate)'.format(
            stats['name'], stats['_id'], stats['count'], contribution_rate))


if __name__ == "__main__":
    main()
