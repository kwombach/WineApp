import sqlite3
import csv
import pandas as pd
import string
from collections import Counter
import ast


def word_clouds(categorization, column_name):
    con = sqlite3.connect('wineapp.db')
    # cur = con.cursor()

    # selecting all the information from the appropriate word cloud table
    sql = '''SELECT *
               FROM top_words_from_reviews_by_''' + categorization

    price_cloud_df = pd.read_sql(sql, con)

    list_of_word_dictionaries = []
    for category in price_cloud_df[column_name].values:
        word_cloud_list_as_df = price_cloud_df.loc[price_cloud_df[column_name] == category]
        word_cloud_list_as_string = word_cloud_list_as_df['word_cloud'].values[0]
        word_cloud_list_as_list = ast.literal_eval(word_cloud_list_as_string)
        word_cloud_list_as_dict = dict(word_cloud_list_as_list)
        list_of_word_dictionaries.append([category, word_cloud_list_as_dict])

    # return format:
    # [['0-15', {'dark': 8803, 'cherry': 8694, ... }],['15-20', {'dark': 8803, 'cherry': 8694, ... }], ...]

    return list_of_word_dictionaries


def price_clouds_unique_dictonary():
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()

    # selecting all the information from the appropriate word cloud table
    sql = '''SELECT *
               FROM top_words_from_reviews_by_price'''

    price_cloud_df = pd.read_sql(sql, con)

    list_of_word_dictionaries = []
    for price_range in price_cloud_df['wine_price_range'].values:
        word_cloud_list_as_df = price_cloud_df.loc[price_cloud_df['wine_price_range'] == price_range]
        word_cloud_list_as_string = word_cloud_list_as_df['word_cloud'].values[0]
        word_cloud_list_as_list = ast.literal_eval(word_cloud_list_as_string)
        word_cloud_list_as_dict = dict(word_cloud_list_as_list)
        list_of_word_dictionaries.append(word_cloud_list_as_dict)

    # print(list_of_word_dictionaries[0])

    # got lazy - working with sets:
    master_set = set()
    master_set.update(list_of_word_dictionaries[1].keys())
    master_set.update(list_of_word_dictionaries[2].keys())
    master_set.update(list_of_word_dictionaries[3].keys())
    master_set.update(list_of_word_dictionaries[4].keys())
    master_set.update(list_of_word_dictionaries[5].keys())
    master_set.update(list_of_word_dictionaries[6].keys())

    _0to15_words = set(list_of_word_dictionaries[0].keys()) - master_set
    print('0to15', _0to15_words)

    master_set_1 = set()
    master_set_1.update(list_of_word_dictionaries[0].keys())
    master_set_1.update(list_of_word_dictionaries[2].keys())
    master_set_1.update(list_of_word_dictionaries[3].keys())
    master_set_1.update(list_of_word_dictionaries[4].keys())
    master_set_1.update(list_of_word_dictionaries[5].keys())
    master_set_1.update(list_of_word_dictionaries[6].keys())

    _15to20_words = set(list_of_word_dictionaries[1].keys()) - master_set_1
    print('0to15', _15to20_words)

    master_set_2 = set()
    master_set_2.update(list_of_word_dictionaries[0].keys())
    master_set_2.update(list_of_word_dictionaries[1].keys())
    master_set_2.update(list_of_word_dictionaries[3].keys())
    master_set_2.update(list_of_word_dictionaries[4].keys())
    master_set_2.update(list_of_word_dictionaries[5].keys())
    master_set_2.update(list_of_word_dictionaries[6].keys())

    _20to30_words = set(list_of_word_dictionaries[2].keys()) - master_set_2
    print('20to30', _20to30_words)

    master_set_3 = set()
    master_set_3.update(list_of_word_dictionaries[0].keys())
    master_set_3.update(list_of_word_dictionaries[1].keys())
    master_set_3.update(list_of_word_dictionaries[2].keys())
    master_set_3.update(list_of_word_dictionaries[4].keys())
    master_set_3.update(list_of_word_dictionaries[5].keys())
    master_set_3.update(list_of_word_dictionaries[6].keys())

    _30to40_words = set(list_of_word_dictionaries[3].keys()) - master_set_3
    print('30to40', _30to40_words)

    master_set_4 = set()
    master_set_4.update(list_of_word_dictionaries[0].keys())
    master_set_4.update(list_of_word_dictionaries[1].keys())
    master_set_4.update(list_of_word_dictionaries[2].keys())
    master_set_4.update(list_of_word_dictionaries[3].keys())
    master_set_4.update(list_of_word_dictionaries[5].keys())
    master_set_4.update(list_of_word_dictionaries[6].keys())

    _40to70_words = set(list_of_word_dictionaries[4].keys()) - master_set_4
    print('40to70', _40to70_words)

    master_set_5 = set()
    master_set_5.update(list_of_word_dictionaries[0].keys())
    master_set_5.update(list_of_word_dictionaries[1].keys())
    master_set_5.update(list_of_word_dictionaries[2].keys())
    master_set_5.update(list_of_word_dictionaries[3].keys())
    master_set_5.update(list_of_word_dictionaries[4].keys())
    master_set_5.update(list_of_word_dictionaries[6].keys())

    _70to100_words = set(list_of_word_dictionaries[5].keys()) - master_set_5
    print('70to100', _70to100_words)

    master_set_6 = set()
    master_set_6.update(list_of_word_dictionaries[0].keys())
    master_set_6.update(list_of_word_dictionaries[1].keys())
    master_set_6.update(list_of_word_dictionaries[2].keys())
    master_set_6.update(list_of_word_dictionaries[3].keys())
    master_set_6.update(list_of_word_dictionaries[4].keys())
    master_set_6.update(list_of_word_dictionaries[5].keys())

    _100toPlus_words = set(list_of_word_dictionaries[6].keys()) - master_set_6
    print('100Plus', _100toPlus_words)


if __name__ == '__main__':

    price_clouds_unique_dictonary()

    word_clouds('price', 'wine_price_range')
    word_clouds('variant', 'wine_variant')
    word_clouds('wine', 'review_wineId_int')
