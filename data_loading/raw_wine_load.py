import sqlite3
import csv
import pandas as pd
import string
# import pyenchant
import unicodedata
import html


def find_ngrams(input_list, n):
  return zip(*[input_list[i:] for i in range(n)])


def main(filename):
    con_wdb = sqlite3.connect('wineapp.db')

    c = con_wdb.cursor()

    # Create a CSV reader
    reader = csv.reader(open(filename))

    # skip header
    next(reader, None)

    for row in reader:
        # Convert row to lower case
        # row = [item.lower() for item in row]
        try:
            wine_name_search = unicodedata.normalize('NFKD', html.unescape(row[6])) \
                .encode('ascii', 'ignore') \
                .decode("utf-8") \
                .replace(' ', '+') \
                .replace('&+', '') \
                .replace('.', '') \
                .lower()
            wine_name_fancy = html.unescape(row[6])
            wine_name_plain = unicodedata.normalize('NFKD', html.unescape(row[6]))\
                .encode('ascii', 'ignore')\
                .decode("utf-8")
            c.execute('''
                INSERT INTO wine_data_raw (chunk_id,review_points,review_text,review_time,review_userId,
                review_userName,wine_name,wine_variant,wine_wineId,wine_year
                ,wine_name_fancy
                ,wine_name_plain
                ,wine_name_search) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''', (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                      wine_name_fancy,wine_name_plain,wine_name_search))

        except sqlite3.Error as e:
            print("Wine, insert error:", e.args[0])
    con_wdb.commit()

def populate_wines():
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS wines')
    cur.execute(
        'CREATE TABLE wines (wine_wineId, wine_name, wine_variant, '
        'wine_year, qty_reviews,wine_name_fancy,wine_name_plain,wine_name_search);')
    con.commit()
    cur.execute('''INSERT INTO wines 
                   SELECT wine_wineId, wine_name, wine_variant, wine_year, count(*) as qty_reviews
                   ,wine_name_fancy
                   ,wine_name_plain
                   ,wine_name_search
                     FROM wine_data_raw
                    GROUP BY wine_wineId, wine_name, wine_variant, wine_year
                    ORDER BY qty_reviews DESC''')
    con.commit()

#chunk_id,review_points,review_text,review_time,review_userId, review_userName,wine_name,wine_variant,wine_wineId,

def populate_users():
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS users')
    cur.execute(
        'CREATE TABLE users (review_userId, review_userName, qty_reviews);')
    con.commit()
    cur.execute('''INSERT INTO users 
                   SELECT review_userId, review_userName, count(*) as qty_reviews
                     FROM wine_data_raw
                    GROUP BY review_userId, review_userName
                    ORDER BY qty_reviews DESC''')
    con.commit()

def convert_reviews_to_word_freq_dicts():
    # d = pyenchant.Dict("en_US")
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    sql = '''SELECT chunk_id,review_points,review_text,review_time,review_userId,
                    review_userName,wine_name,wine_variant,wine_wineId
               FROM wine_data_raw'''
    df = pd.read_sql(sql, con)
    con.commit()

    for review in df.values:
        review_holder_dict = {}
        processed_string_list = review[2].translate(str.maketrans('', '', string.punctuation)).lower().split()
        #####################################################
        # REMOVE COMMON WORDS AND MAYBE CHECK SPELLING HERE #
        #####################################################
        processed_string_list += list(find_ngrams(processed_string_list, 2))

        for word in processed_string_list:
            if word in review_holder_dict:
                review_holder_dict[word] += 1
            else:
                review_holder_dict[word] = 1

        review[2] = str(review_holder_dict)

    cur.execute('DROP TABLE IF EXISTS wine_raw_processed_reviews')
    df.to_sql('wine_raw_processed_reviews', con)
    con.commit()


def scraped_wine_raw():
    con_wdb = sqlite3.connect('wineapp.db')

    c = con_wdb.cursor()

    c.execute('DROP TABLE IF EXISTS scraped_wine_data')
    c.execute(
        'CREATE TABLE scraped_wine_data (scraped_wine_id,scraped_wine_search_term,scraped_wine_match'
        ',scraped_wine_price, scraped_wine_location);')
    con_wdb.commit()

    # Create a CSV reader
    filestream = open("scraped_wine_prices_locations_clean.txt", "r")
    next(filestream, None)
    for line in filestream:
        if 'FAIL' not in line:
            row = line.split(',')
            #############################################
            # Add OPTIONAL drop of main raw table for size reduction.
            # FIX wine prices >999.99
            # FIX commas in wine search name (manually)
            # FIX make notes for future reference in scraper for wrapping outputs in for ease of processing.
            # Never make this mistake again.............
            #############################################
            # print(row[0], row[1], row[2], row[3], row[4])
            try:
                c.execute('''
                    INSERT INTO scraped_wine_data (scraped_wine_id,scraped_wine_search_term,scraped_wine_match
                    ,scraped_wine_price, scraped_wine_location) VALUES (?,?,?,?,?)''',
                    (row[0], row[1], row[2], row[3], row[4]))
            except sqlite3.Error as e:
                print("Raw Wine, insert error:", e.args[0])

    con_wdb.commit()


def raw_wine_data_with_scraped(drop_table=False):

    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS raw_wine_data_with_scraped')
    cur.execute(
        'CREATE TABLE raw_wine_data_with_scraped (chunk_id,review_points,review_text,review_time,review_userId,'
        'review_userName,wine_name,wine_variant,wine_wineId,wine_year,wine_name_fancy,'
        'wine_name_plain,wine_name_search,scraped_wine_id,scraped_wine_search_term,scraped_wine_match'
        ',scraped_wine_price, scraped_wine_location);')
    con.commit()
    cur.execute('''INSERT INTO raw_wine_data_with_scraped
                   SELECT * FROM wine_data_raw r
                   LEFT JOIN scraped_wine_data s
                   ON s.scraped_wine_id = r.wine_wineId''')

    if drop_table:
        cur.execute('DROP TABLE IF EXISTS wine_data_raw')

    con.commit()


if __name__ == '__main__':
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS wine_data_raw')
    cur.execute(
        'CREATE TABLE wine_data_raw (chunk_id,review_points,review_text,review_time,review_userId,'
        'review_userName,wine_name,wine_variant,wine_wineId,wine_year,wine_name_fancy,'
        'wine_name_plain,wine_name_search);')
    con.commit()

    main('cellartracker-clean1.csv')
    main('cellartracker-clean2.csv')
    populate_wines()
    populate_users()
    convert_reviews_to_word_freq_dicts()
    scraped_wine_raw()
    raw_wine_data_with_scraped(drop_table=True)


