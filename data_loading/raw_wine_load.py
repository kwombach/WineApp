import sqlite3
import csv
import pandas as pd
import string
import unicodedata
import html


def find_ngrams(input_list, n):
  return zip(*[input_list[i:] for i in range(n)])


def main(filename):
    con_wdb = sqlite3.connect('wineapp.db')

    c = con_wdb.cursor()

    # Create a CSV reader
    reader = csv.reader(open(filename))

    # Skip header
    next(reader, None)

    for row in reader:
        try:
            # getting rid of HTML characters and whatnot for URL searches.
            wine_name_search = unicodedata.normalize('NFKD', html.unescape(row[6])) \
                .encode('ascii', 'ignore') \
                .decode("utf-8") \
                .replace(' ', '+') \
                .replace('&+', '') \
                .replace('.', '') \
                .lower()
            # adding back some terms for potential future use.
            wine_name_fancy = html.unescape(row[6])
            wine_name_plain = unicodedata.normalize('NFKD', html.unescape(row[6]))\
                .encode('ascii', 'ignore')\
                .decode("utf-8")
            c.execute('''
                INSERT INTO wine_data_raw (
                chunk_id,
                review_points,
                review_text,
                review_time,
                review_userId,
                review_userName,
                wine_name,
                wine_variant,
                wine_wineId,
                wine_year,
                wine_name_fancy,
                wine_name_plain,
                wine_name_search) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''', (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                      wine_name_fancy,wine_name_plain,wine_name_search))

        # Catch errors
        except sqlite3.Error as e:
            print("Main, insert error:", e.args[0])
    con_wdb.commit()


def scraped_wine_raw():
    con_wdb = sqlite3.connect('wineapp.db')

    c = con_wdb.cursor()

    c.execute('DROP TABLE IF EXISTS scraped_wine_data')
    c.execute('''
                CREATE TABLE scraped_wine_data (
                scraped_wine_id,
                scraped_wine_search_term,
                scraped_wine_match,
                scraped_wine_price,
                scraped_wine_location);''')
    con_wdb.commit()

    # Create a CSV reader
    filestream = open("scraped_wine_prices_locations_clean.txt", "r")
    next(filestream, None)
    for line in filestream:
        # FAIL lines were what I used when searches failed.
        # PROBLEM: "Next time" dump these to a separate file so stuff like this isn't necessary.
        if 'FAIL' not in line:
            row = line.split(',')
            #############################################
            # Scraper has some serious issues (which have been fixed this go around)
            # but the should be revised in the future.
            # PROBLEMS:
            # 1. Most of these can be fixed by using a better delimiter like '^' instead of the sad ',' or
            # writing this information out to csv better instead of a txt file. But I'm not sure about that.
            # 2. Wine prices >999.99 have commas which shouldve been stripped out.
            # 3. Commas should be been escaped or removed from wine names.
            # 4. All sorts of quotations marks should have been escaped or removed
            #############################################
            try:
                c.execute('''
                    INSERT INTO scraped_wine_data (
                    scraped_wine_id,
                    scraped_wine_search_term,
                    scraped_wine_match,
                    scraped_wine_price, 
                    scraped_wine_location) VALUES (?,?,?,?,?)''',
                    (row[0], row[1], row[2], row[3], row[4]))
            except sqlite3.Error as e:
                print("scraped_wine, insert error:", e.args[0])

    con_wdb.commit()


def raw_wine_data_with_scraped(drop_table=False):

    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS raw_wine_data_with_scraped')
    cur.execute('''CREATE TABLE raw_wine_data_with_scraped (
                    chunk_id,
                    review_points,
                    review_text,
                    review_time,
                    review_userId,
                    review_userName,
                    wine_name,
                    wine_variant,
                    wine_wineId,
                    wine_year,
                    wine_name_fancy,
                    wine_name_plain,
                    wine_name_search,
                    scraped_wine_id,
                    scraped_wine_search_term,
                    scraped_wine_match,
                    scraped_wine_price,
                    scraped_wine_location);''')
    con.commit()
    cur.execute('''INSERT INTO raw_wine_data_with_scraped
                   SELECT * FROM wine_data_raw r
                   LEFT JOIN scraped_wine_data s
                   ON s.scraped_wine_id = r.wine_wineId''')

    #############################################
    # This is left over for a previous iteration. This may not be the best place
    # to drop this table as it's used for other routines later.
    # But I'm using an extra table generation (of A LOT of records)
    # It's not efficient and should be revisited.

    # PROBLEM: wines table is generated partially from raw_wine_data_with_scraped table (made in this routine)
    # this table is generated from wine_data_raw and scraped_wine_data ... this table doesn't need to exist.
    # you should be able to create the wines table by simply joining in scraped_wine_data table and skipping this
    # table entirely.  For some reason that wasn't working immediately and I moved onto more important things besides
    # this one issue that will only cause a 30 second delay each time you load the tables.  WINE_ID's are NOT UNIQUE
    # and must be addressed which is why the query for wines is so janky and why the reviews table cannot have wine
    # name and other details beside wine_id
    #############################################

    if drop_table:
        cur.execute('DROP TABLE IF EXISTS wine_data_raw')

    con.commit()

def populate_wines_new():
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS wines')
    cur.execute('''
                    CREATE TABLE wines (
                    wine_wineId_int PRIMARY KEY,
                    wine_name, 
                    wine_variant, 
                    wine_year,
                    wine_name_fancy,
                    wine_name_plain,
                    wine_name_search,
                    scraped_wine_id,
                    scraped_wine_search_term,
                    scraped_wine_match,
                    scraped_wine_price,
                    scraped_wine_location,
                    wine_qty_reviews );
                ''')
    con.commit()
    cur.execute('''INSERT INTO wines 
                   SELECT lev1.wine_wineId_int,
                   lev1.wine_name, 
                   lev1.wine_variant, 
                   lev1.wine_year,
                   lev1.wine_name_fancy,
                   lev1.wine_name_plain,
                   lev1.wine_name_search,
                   lev1.scraped_wine_id,
                   lev1.scraped_wine_search_term,
                   lev1.scraped_wine_match,
                   lev1.scraped_wine_price,
                   lev1.scraped_wine_location,
                   lev2.wine_qty_reviews 
              FROM (
                    SELECT CAST(wine_wineId as int) as wine_wineId_int,
                           wine_name, 
                           wine_variant, 
                           wine_year,
                           wine_name_fancy,
                           wine_name_plain,
                           wine_name_search,
                           scraped_wine_id,
                           scraped_wine_search_term,
                           scraped_wine_match,
                           scraped_wine_price,
                           scraped_wine_location,
                           MAX(qty_reviews) Most_Reviews
                      FROM 
                            (
                            SELECT rs.wine_wineId,
                                   wine_name, 
                                   wine_variant, 
                                   wine_year,
                                   count(*) as qty_reviews,
                                   wine_year,
                                   wine_name_fancy,
                                   wine_name_plain,
                                   wine_name_search,
                                   scraped_wine_id,
                                   scraped_wine_search_term,
                                   scraped_wine_match,
                                   scraped_wine_price,
                                   scraped_wine_location
                              FROM raw_wine_data_with_scraped rs
                             GROUP BY rs.wine_wineId,
                                   wine_name, 
                                   wine_variant, 
                                   wine_year,
                                   wine_name_fancy,
                                   wine_name_plain,
                                   wine_name_search,
                                   scraped_wine_id,
                                   scraped_wine_search_term,
                                   scraped_wine_match,
                                   scraped_wine_price,
                                   scraped_wine_location
                             ORDER BY qty_reviews ASC
                            )
                      GROUP BY wine_wineId_int
                      ORDER BY wine_wineId_int ASC
                    ) lev1
              LEFT JOIN (
                        SELECT COUNT(wine_wineId) as wine_qty_reviews, CAST(wine_wineId as int) as wine_wineId_int
                        FROM wine_data_raw
                        GROUP BY wine_wineId
                        ) lev2
                ON lev1.wine_wineId_int = lev2.wine_wineId_int
             ORDER BY lev1.wine_wineId_int ASC
             ''')
    con.commit()


def populate_users():
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS users')
    cur.execute('''CREATE TABLE users (
                                users_userId PRIMARY KEY,
                                users_userName,
                                users_qty_reviews);''')
    con.commit()
    cur.execute('''INSERT INTO users 
                   SELECT review_userId, review_userName, count(*) as qty_reviews
                     FROM wine_data_raw
                    GROUP BY review_userId, review_userName
                    ORDER BY qty_reviews DESC''')
    con.commit()


def populate_reviews(drop_table=False):
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS reviews')
    cur.execute('''CREATE TABLE reviews (
                    review_points,
                    review_text,
                    review_time,
                    review_userId,
                    review_wineId_int,
                    FOREIGN KEY (review_userId) REFERENCES users(users_userId),
                    FOREIGN KEY (review_wineId_int) REFERENCES wines(wine_wineId_int));''')
    con.commit()
    cur.execute('''INSERT INTO reviews
                   SELECT review_points,
                          review_text,
                          review_time,
                          review_userId,
                          CAST(wine_wineId as int) as review_wineId_int
                     FROM wine_data_raw r''')

    #############################################
    # This should be reviewed when making changes to skip the creation of "scraped_wine_data"
    #############################################
    if drop_table:
        cur.execute('DROP TABLE IF EXISTS wine_data_raw')
        cur.execute('DROP TABLE IF EXISTS scraped_wine_data')

    con.commit()


def convert_reviews_to_word_freq_dicts():
    # d = pyenchant.Dict("en_US")
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    sql = '''SELECT chunk_id,
                    review_points,
                    review_text,
                    review_time,
                    review_userId,
                    review_userName,
                    wine_name,
                    wine_variant,
                    wine_wineId
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


def populate_wines_old():
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS wines')
    cur.execute(
        'CREATE TABLE wines ('
        'wine_wineId,'
        'wine_name,'
        'wine_variant, '
        'wine_year,'
        'qty_reviews,'
        'wine_name_fancy,'
        'wine_name_plain,'
        'wine_name_search);')
    con.commit()
    cur.execute('''INSERT INTO wines 
                   SELECT wine_wineId, wine_name, wine_variant, wine_year, count(*) as qty_reviews
                   ,wine_name_fancy
                   ,wine_name_plain
                   ,wine_name_search
                     FROM raw_wine_data_with_scraped
                    GROUP BY wine_wineId, wine_name, wine_variant, wine_year
                    ORDER BY qty_reviews DESC''')
    con.commit()


if __name__ == '__main__':
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS wine_data_raw')
    cur.execute('''
        CREATE TABLE wine_data_raw (
                    chunk_id,
                    review_points,
                    review_text,
                    review_time,
                    review_userId,
                    review_userName,
                    wine_name,
                    wine_variant,
                    wine_wineId,
                    wine_year,
                    wine_name_fancy,
                    wine_name_plain,
                    wine_name_search);''')
    con.commit()

    main('cellartracker-clean1.csv')
    main('cellartracker-clean2.csv')
    scraped_wine_raw()

    raw_wine_data_with_scraped()
    populate_wines_new()
    populate_users()

    #############################################
    # DO THIS LAST: This should drop some tables. If you're tempted to NOT drop the tables. Review data
    # very closely. Again, non-unique wine_id's really threw a wrench into this - hence all these weirdisms.
    #############################################

    populate_reviews(drop_table=True)

    #############################################
    # Older Routines or Unused
    #############################################
    # populate_wines()
    # convert_reviews_to_word_freq_dicts()





