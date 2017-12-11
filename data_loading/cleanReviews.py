import sqlite3
import pandas as pd
import numpy as np
import csv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


import nltk
import string
from nltk.stem.porter import PorterStemmer

stemmer = PorterStemmer()

def getReviews():
	con = sqlite3.connect('wineapp.db')
	cur = con.cursor()
	sql = '''SELECT wine_wineId_int FROM wines WHERE wine_qty_reviews > 9 ORDER by wine_qty_reviews DESC'''
	wine_ids = pd.read_sql(sql, con)

	n = wine_ids.shape[0]
	print("Number of Wines: {}".format(n))
	lst = [""]*n
	i=0
	sql2 = 'SELECT review_text_lem_no_stop, review_wineId_int FROM reviews WHERE review_wineId_int'
	winedf = pd.read_sql(sql2, con)

	for x in wine_ids.values:
		group_review_df = winedf.loc[winedf['review_wineId_int'] == x[0]]
        # group_corpus_list = [i for i in group_review_df['review_text_lem_no_stop']]
		# print(reviews)
		for review in group_review_df['review_text_lem_no_stop']:
		    lst[i] += str(review + " ")
		i+=1
		print("Combining Reviews for Wine #: {}".format(i))
	return lst

# def stem_tokens(tokens, stemmer):
#     stemmed = []
#     for item in tokens:
#         stemmed.append(stemmer.stem(item))
#     return stemmed

def tokenize(text):
    tokens = nltk.word_tokenize(text)
    # stems = stem_tokens(tokens, stemmer)
    return tokens


def vectorize(list_of_strings):
	# Takes in a list of strings
	# Strings Correspond to all reviews for each wine
	# Returns matrix of all wines and all words 
	# (not binary -- weighted based on TF-IDF Parameters)

	tfidf_vectorizor = TfidfVectorizer(tokenizer = tokenize, analyzer = 'word')
	tfidf_matrix = tfidf_vectorizor.fit_transform(list_of_strings)
	print(tfidf_matrix.shape)
	return tfidf_matrix

def getSimilarity(vectorized_matrix):
	print('making sim_matrix!')
	n = vectorized_matrix.shape[0]
	sim_matrix = cosine_similarity(vectorized_matrix[0:n], vectorized_matrix)
	print('sim_matrix made!')
	return sim_matrix

def run():
	list_of_reviews = getReviews()
	matrix = vectorize(list_of_reviews)
	similarity_matrix = getSimilarity(matrix)
	similarity_matrix.tofile('sim_mat.csv', sep = ',')
	# a.tofile('foo.csv',sep=','
	# with open('sim_mat.csv', 'w') as sm:
	# 	for row in similarity_matrix:
	# 		writer = csv.writer(sm)
	# 		writer.writerows(row)

	# df = pd.DataFrame(similarity_matrix)
	# df.to_csv('similarity_matrix.csv')
	print('.csv file made!')
	return similarity_matrix

run()

# list_of_reviews = getReviews()
# matrix = vectorize(list_of_reviews)
# num_processes = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=num_processes)
# result1 = pool.map(getSimilarity, matrix)


	