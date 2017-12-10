import sqlite3
import csv
import pandas as pd
import string
import unicodedata
import html
from collections import Counter
import text_processor
from nltk.corpus import stopwords
from datetime import datetime

def find_ngrams(input_list, n):
    return zip(*[input_list[i:] for i in range(n)])


def main(filename):

    stop_words = stopwords.words('english')

    for line in open('low_weight_words.txt', 'r'):
        stop_words.append(line.strip())

    con_wdb = sqlite3.connect('wineapp.db')

    c = con_wdb.cursor()

    # Create a CSV reader
    reader = csv.reader(open(filename))

    # Skip header
    next(reader, None)
    progress_counter = 0
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
            wine_name_plain = unicodedata.normalize('NFKD', html.unescape(row[6])) \
                .encode('ascii', 'ignore') \
                .decode("utf-8")
            review_text_plain = unicodedata.normalize('NFKD', html.unescape(row[2])) \
                .encode('ascii', 'ignore') \
                .decode("utf-8")
            wine_variant_plain = unicodedata.normalize('NFKD', html.unescape(row[7])) \
                .encode('ascii', 'ignore') \
                .decode("utf-8")
            review_text_lem_no_stop = text_processor.remove_stop_then_lem(review_text_plain.lower(), stop_words)

            if len(review_text_lem_no_stop) < 3:
                progress_counter += 1
                if progress_counter % 20000 == 0:
                    print('Raw Wine Load Records Processed (skip):', progress_counter)
                    con_wdb.commit()
                continue

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
                wine_name_search,
                review_text_lem_no_stop) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''', (row[0], row[1], review_text_plain, row[3], row[4], row[5], row[6], wine_variant_plain, row[8], row[9],
                      wine_name_fancy, wine_name_plain, wine_name_search, review_text_lem_no_stop))

            progress_counter += 1
            if progress_counter % 20000 == 0:
                print('Raw Wine Load Records Processed:', progress_counter)
                con_wdb.commit()

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
                    review_text_lem_no_stop,
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
                    SELECT CAST(wine_wineId AS INT) AS wine_wineId_int,
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
                                   count(*) AS qty_reviews,
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
                        SELECT COUNT(wine_wineId) AS wine_qty_reviews, CAST(wine_wineId AS INT) AS wine_wineId_int
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
                   SELECT review_userId, review_userName, count(*) AS qty_reviews
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
                    review_text_lem_no_stop,
                    FOREIGN KEY (review_userId) REFERENCES users(users_userId),
                    FOREIGN KEY (review_wineId_int) REFERENCES wines(wine_wineId_int));''')
    con.commit()
    cur.execute('''INSERT INTO reviews
                   SELECT review_points,
                          review_text,
                          review_time,
                          review_userId,
                          CAST(wine_wineId AS INT) AS review_wineId_int,
                          review_text_lem_no_stop
                     FROM wine_data_raw r''')

    #############################################
    # This should be reviewed when making changes to skip the creation of "scraped_wine_data"
    #############################################
    if drop_table:
        cur.execute('DROP TABLE IF EXISTS wine_data_raw')
        cur.execute('DROP TABLE IF EXISTS scraped_wine_data')
        cur.execute('DROP TABLE IF EXISTS raw_wine_data_with_scraped')

    con.commit()


def top_words_from_reviews_by_wine():
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS top_words_from_reviews_by_wine')
    cur.execute('''CREATE TABLE top_words_from_reviews_by_wine (
                        review_wineId_int,
                        word_cloud);''')
    con.commit()

    sql = '''SELECT r.review_wineId_int, r.review_text_lem_no_stop, w.wine_name_plain, 
                    w.scraped_wine_price, w.wine_qty_reviews
               FROM reviews r
               LEFT JOIN wines w
                 ON r.review_wineId_int = w.wine_wineId_int
              WHERE r.review_wineId_int IN 
                                            (
                                            SELECT wine_wineId_int
                                              FROM wines
                                             WHERE wine_qty_reviews > 14
                                             ORDER BY wine_qty_reviews DESC
                                            )
           ORDER BY r.review_wineId_int DESC'''

    df = pd.read_sql(sql, con)

    group_sql = '''SELECT wine_wineId_int
                      FROM wines
                     WHERE wine_qty_reviews > 14
                     ORDER BY wine_qty_reviews DESC
                    '''

    grouper_df = pd.read_sql(group_sql, con)

    # Ignore this:
    # Side note for text wrangler for regex search to select all numbers \d{1,7}

    for group in grouper_df.values:
        group_review_df = df.loc[df['review_wineId_int'] == group[0]]
        group_corpus_list = [i for i in group_review_df['review_text_lem_no_stop']]

        counts = Counter()

        for text in group_corpus_list:
            # counts.update(word for word in text.split() + list(find_ngrams(text.split(), 2)))
            counts.update(word for word in text.split())

        joined_counts = [(' '.join(word[0]), word[1]) if type(word[0]) is tuple
                         else (word[0], word[1]) for word in counts.most_common(100)]
        # print(counts)
        # print(counts.most_common(100))
        # exit()
        # counts = low_weight_words(counts)
        try:
            cur.execute('''
                INSERT INTO top_words_from_reviews_by_wine (
                        review_wineId_int,
                        word_cloud) VALUES (?,?)''',
                        (int(group[0]), str(joined_counts)))
            counts.clear()
            counts += Counter()
        except sqlite3.Error as e:
            print("Top words by wine, insert error:", e.args[0])
            counts.clear()
            counts += Counter()

    con.commit()


def top_words_from_reviews_by_variant():
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS top_words_from_reviews_by_variant')
    cur.execute('''CREATE TABLE top_words_from_reviews_by_variant (
                        wine_variant,
                        word_cloud);''')
    con.commit()

    sql = '''SELECT r.review_wineId_int, r.review_text_lem_no_stop, w.wine_name_plain, w.wine_variant,
                    w.scraped_wine_price, w.wine_qty_reviews
               FROM reviews r
               LEFT JOIN wines w
                 ON r.review_wineId_int = w.wine_wineId_int
              WHERE r.review_wineId_int IN 
                                            (
                                            SELECT wine_wineId_int
                                              FROM wines
                                             WHERE wine_qty_reviews > 14
                                             ORDER BY wine_qty_reviews DESC
                                            )
           ORDER BY r.review_wineId_int DESC'''

    df = pd.read_sql(sql, con)

    group_sql = '''SELECT wine_variant
                   FROM (
                        SELECT wine_variant
                          FROM wines
                         WHERE wine_qty_reviews > 14
                         ORDER BY wine_qty_reviews DESC
                        )
                   GROUP BY wine_variant
                    '''

    grouper_df = pd.read_sql(group_sql, con)

    # Ignore this:
    # Side note for text wrangler for regex search to select all numbers \d{1,7}

    for group in grouper_df.values:
        group_review_df = df.loc[df['wine_variant'] == group[0]]
        group_corpus_list = [i for i in group_review_df['review_text_lem_no_stop']]

        counts = Counter()

        for text in group_corpus_list:
            # counts.update(word for word in text.split() + list(find_ngrams(text.split(), 2)))
            counts.update(word for word in text.split())

        joined_counts = [(' '.join(word[0]), word[1]) if type(word[0]) is tuple
                         else (word[0], word[1]) for word in counts.most_common(100)]

        # print(counts)
        # print(counts.most_common(100))
        # exit()
        # counts = low_weight_words(counts)
        try:
            cur.execute('''
                INSERT INTO top_words_from_reviews_by_variant (
                        wine_variant,
                        word_cloud) VALUES (?,?)''',
                        (str(group[0]), str(joined_counts)))
            counts.clear()
            counts += Counter()
        except sqlite3.Error as e:
            print("Top words by variant, insert error:", e.args[0])
            counts.clear()
            counts += Counter()

    con.commit()

def top_words_from_reviews_by_price():
    con = sqlite3.connect('wineapp.db')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS top_words_from_reviews_by_price')
    cur.execute('''CREATE TABLE top_words_from_reviews_by_price (
                        wine_price_range,
                        word_cloud);''')
    con.commit()

    sql = '''SELECT r.review_wineId_int, r.review_text_lem_no_stop,
            CASE WHEN CAST(w.scraped_wine_price AS float) > 0 AND CAST(w.scraped_wine_price AS float) <= 15 THEN '0-15'
                     WHEN CAST(w.scraped_wine_price AS float) > 15 AND CAST(w.scraped_wine_price AS float) <= 25 THEN '15-20'
                     WHEN CAST(w.scraped_wine_price AS float) > 25 AND CAST(w.scraped_wine_price AS float) <= 40 THEN '20-30'
                     WHEN CAST(w.scraped_wine_price AS float) > 40 AND CAST(w.scraped_wine_price AS float) <= 60 THEN '30-40'
                     WHEN CAST(w.scraped_wine_price AS float) > 60 AND CAST(w.scraped_wine_price AS float) <= 90 THEN '40-70'
                     WHEN CAST(w.scraped_wine_price AS float) > 90 AND CAST(w.scraped_wine_price AS float) <= 150 THEN '70-100'
                     WHEN CAST(w.scraped_wine_price AS float) > 150 AND CAST(w.scraped_wine_price AS float) <= 10000 THEN '100+'
              END price_cat
               FROM reviews r
               LEFT JOIN wines w
                 ON r.review_wineId_int = w.wine_wineId_int
              WHERE r.review_wineId_int IN 
                                            (
                                            SELECT wine_wineId_int
                                              FROM wines
                                             WHERE wine_qty_reviews > 14
                                               AND CAST(scraped_wine_price AS float)  IS NOT NULL
                                               AND CAST(scraped_wine_price AS float)  > 0
                                             ORDER BY wine_qty_reviews DESC
                                            )
           ORDER BY r.review_wineId_int DESC'''

    df = pd.read_sql(sql, con)

    grouper_lst = ['0-15', '15-20', '20-30', '30-40', '40-70', '70-100', '100+']

    # Ignore this:
    # Side note for text wrangler for regex search to select all numbers \d{1,7}

    for group in grouper_lst:
        group_review_df = df.loc[df['price_cat'] == group]
        group_corpus_list = [i for i in group_review_df['review_text_lem_no_stop']]

        counts = Counter()

        for text in group_corpus_list:
            # counts.update(word for word in text.split() + list(find_ngrams(text.split(), 2)))
            counts.update(word for word in text.split())

        joined_counts = [(' '.join(word[0]), word[1]) if type(word[0]) is tuple
                         else (word[0], word[1]) for word in counts.most_common(100)]

        # print(counts)
        # print(counts.most_common(100))
        # exit()
        # counts = low_weight_words(counts)
        try:
            cur.execute('''
                INSERT INTO top_words_from_reviews_by_price (
                        wine_price_range,
                        word_cloud) VALUES (?,?)''',
                        (str(group), str(joined_counts)))
            counts.clear()
            counts += Counter()
        except sqlite3.Error as e:
            print("Top words by price, insert error:", e.args[0])
            counts.clear()
            counts += Counter()

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
                   SELECT wine_wineId, wine_name, wine_variant, wine_year, count(*) AS qty_reviews
                   ,wine_name_fancy
                   ,wine_name_plain
                   ,wine_name_search
                     FROM raw_wine_data_with_scraped
                    GROUP BY wine_wineId, wine_name, wine_variant, wine_year
                    ORDER BY qty_reviews DESC''')
    con.commit()


def low_weight_words(counter):
    low_weight = Counter(grand=100000,
                         weight=100000,
                         acid=100000,
                         tones=100000,
                         mouthfeel=100000,
                         hint=100000,
                         nose=100000,
                         finish=100000,
                         bottle=100000,
                         palate=100000,
                         acidity=100000,
                         long=100000,
                         color=100000,
                         little=100000,
                         aromas=100000,
                         delicious=100000,
                         age=100000,
                         the=100000,
                         a=100000,
                         of=100000,
                         this=100000,
                         to=100000,
                         i=100000,
                         on=100000,
                         it=100000,
                         very=100000,
                         but=100000,
                         was=100000,
                         that=100000,
                         more=100000,
                         notes=100000,
                         good=100000,
                         great=100000,
                         some=100000,
                         wine=100000,
                         at=100000,
                         had=100000,
                         nice=100000,
                         have=100000,
                         an=100000,
                         years=100000,
                         fine=100000,
                         just=100000,
                         well=100000,
                         my=100000,
                         really=100000,
                         bit=100000,
                         its=100000,
                         flavors=100000,
                         so=100000,
                         label=100000,
                         one=100000,
                         time=100000,
                         than=100000,
                         full=100000,
                         much=100000,
                         still=100000,
                         always=100000,
                         all=100000,
                         by=100000,
                         nv=100000,
                         better=100000,
                         up=100000,
                         lovely=100000,
                         be=100000,
                         mouth=100000,
                         beautiful=100000,
                         glass=100000,
                         like=100000,
                         there=100000,
                         quite=100000,
                         new=100000,
                         balance=100000,
                         no=100000,
                         me=100000,
                         has=100000,
                         taste=100000,
                         drink=100000,
                         excellent=100000,
                         you=100000,
                         last=100000,
                         best=100000,
                         bottles=100000,
                         almost=100000,
                         out=100000,
                         tasted=100000,
                         what=100000,
                         showing=100000,
                         white=100000,
                         wonderful=100000,
                         over=100000,
                         opened=100000,
                         too=100000,
                         drank=100000,
                         been=100000,
                         first=100000,
                         after=100000,
                         we=100000,
                         would=100000,
                         served=100000,
                         when=100000,
                         slightly=100000,
                         touch=100000,
                         though=100000,
                         could=100000,
                         which=100000,
                         again=100000,
                         price=100000,
                         get=100000,
                         purchased=100000,
                         ive=100000,
                         about=100000,
                         lots=100000,
                         way=100000,
                         are=100000,
                         year=100000,
                         off=100000,
                         will=100000,
                         note=100000,
                         same=100000,
                         colour=100000,
                         right=100000,
                         were=100000,
                         maybe=100000,
                         even=100000,
                         back=100000,
                         another=100000,
                         previous=100000,
                         think=100000,
                         now=100000,
                         flavours=100000,
                         ago=100000,
                         tasting=100000,
                         stuff=100000,
                         other=100000,
                         cellar=100000,
                         sure=100000,
                         can=100000,
                         many=100000,
                         half=100000,
                         few=100000,
                         ever=100000,
                         power=100000,
                         showed=100000,
                         two=100000,
                         dinner=100000,
                         love=100000,
                         yet=100000,
                         character=100000,
                         fantastic=100000,
                         ml=100000,
                         drinking=100000,
                         en=100000,
                         experience=100000,
                         amazing=100000,
                         tasty=100000,
                         least=100000,
                         hue=100000,
                         immediately=100000,
                         probably=100000,
                         recent=100000,
                         length=100000,
                         de=100000,
                         less=100000,
                         most=100000,
                         hints=100000,
                         wines=100000,
                         focused=100000,
                         food=100000,
                         throughout=100000,
                         im=100000,
                         lot=100000,
                         perfect=100000,
                         how=100000,
                         through=100000,
                         air=100000,
                         nicely=100000,
                         should=100000,
                         every=100000,
                         wow=100000,
                         only=100000,
                         although=100000,
                         start=100000,
                         magnum=100000,
                         flavor=100000,
                         seemed=100000,
                         day=100000,
                         before=100000,
                         different=100000,
                         end=100000,
                         subtle=100000,
                         celebrate=100000,
                         expect=100000,
                         rather=100000,
                         impressive=100000)
    return counter-low_weight


if __name__ == '__main__':
    begin_time = datetime.now()
    print(begin_time)

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
                    wine_name_search,
                    review_text_lem_no_stop);''')
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

    # WORD CLOUDS #############################################
    top_words_from_reviews_by_wine()
    top_words_from_reviews_by_variant()
    top_words_from_reviews_by_price()

    end_time = datetime.now()
    print(end_time-begin_time)
