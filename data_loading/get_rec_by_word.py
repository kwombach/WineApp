import text_processor as tp
from nltk.corpus import stopwords
import sqlite3
import pandas as pd
import ast
import pickle

def main(input_text):

    wine_list = pickle.load(open('word_freqs.p', 'rb'))
    return recommend_by_word(clean_user_input(input_text), wine_list)


def word_cloud_gather(categorization, column_name):
    con = sqlite3.connect('wineapp.db')
    # cur = con.cursor()

    # selecting all the information from the appropriate word cloud table
    sql = '''SELECT *
               FROM top_words_from_reviews_by_''' + categorization

    price_cloud_df = pd.read_sql(sql, con)

    list_of_word_clouds = []
    for category in price_cloud_df[column_name].values:
        word_cloud_list_as_df = price_cloud_df.loc[price_cloud_df[column_name] == category]
        word_cloud_list_as_string = word_cloud_list_as_df['word_cloud'].values[0]
        word_cloud_list_as_list = ast.literal_eval(word_cloud_list_as_string)
        list_of_word_clouds.append([category, word_cloud_list_as_list])

    return list_of_word_clouds


def clean_user_input(input_text):

    stop_words = stopwords.words('english')

    for line in open('low_weight_words.txt', 'r'):
        stop_words.append(line.strip())

        user_input_cleaned = tp.remove_stop_then_lem(input_text.lower(), stop_words)

    return user_input_cleaned.split()


def recommend_by_word(user_input_cleaned, wines):

    final_match_list = ['No Match', 'No Match', 'No Match', 'No Match', 'No Match']

    wine_match_no = 0
    for wine in wines:
        matched_words = 0
        for word in user_input_cleaned:
            if word in wine[1]:
                matched_words += 1

        wines[wine_match_no] = [wine[0], wine[1], matched_words]
        wine_match_no += 1

    wines.sort(key=lambda x: x[2], reverse=True)
    if wines[0][2] == 0:
        return(final_match_list)

    check_for_win = []
    incrementer = 0
    matches = wines[0][2]
    while matches != 0:
        matches = wines[incrementer][2]
        check_for_win.append(wines[incrementer])
        incrementer += 1

    match_qty = list(set([i[2] for i in check_for_win]))
    match_qty.sort(reverse=True)

    top_matches_list = []

    for match_no in match_qty:
        for wine in check_for_win:
            if wine[2] == match_no:
                similarity_score = 0
                word_cloud_dictionary = {k: v+1 for v, k in enumerate(wine[1])}
                for word_preference in user_input_cleaned:
                    if word_cloud_dictionary.get(word_preference):
                        similarity_score += word_cloud_dictionary.get(word_preference)
                top_matches_list.append([wine[0], wine[2], similarity_score])

    wine_match_sim_score = pd.DataFrame(top_matches_list,
                                        columns=['wineID','number_matches', 'similarity_score'])

    wine_information = pickle.load(open('wine_table.p', 'rb'))

    polished_wine_list = wine_match_sim_score.merge(wine_information, left_on='wineID',
                                                    right_on='wine_wineId_int', how='inner')

    reduced_wine_list = polished_wine_list[['wine_name_plain', 'number_matches',
                                            'similarity_score', 'wine_qty_reviews']]

    sorted_polished_wine_list = reduced_wine_list.sort_values(['number_matches', 'similarity_score',
                                                              'wine_qty_reviews', 'wine_name_plain'],
                                                              ascending=[False, True, False, True])

    sorted_polished_wine_list = \
        sorted_polished_wine_list.drop(sorted_polished_wine_list.index[len(sorted_polished_wine_list) - 1])

    if len(sorted_polished_wine_list) > 5:
        final_match_qty = 5
    else:
        final_match_qty = len(sorted_polished_wine_list)

    for t in range(final_match_qty):
        final_match_list[t] = sorted_polished_wine_list['wine_name_plain'].values[t]

    return final_match_list


if __name__ == '__main__':

    main('xyz')