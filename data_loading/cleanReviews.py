import sqlite3
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity 
from sklearn.metrics.pairwise import pairwise_distances


import nltk
import string
from nltk.stem.porter import PorterStemmer

stemmer = PorterStemmer()

def getReviews():
	con = sqlite3.connect('wineapp.db')
	cur = con.cursor()
	sql = '''SELECT wine_wineId_int FROM wines ORDER by wine_qty_reviews DESC LIMIT 41911'''
	wine_ids = pd.read_sql(sql, con)

	n = wine_ids.shape[0]
	print("Number of Wines: {}".format(n))
	lst = [""]*n
	i=0

	for x in wine_ids.values:
		sql = 'SELECT review_text, review_wineId_int FROM reviews WHERE review_wineId_int = '+str(x[0])
		wine_df = pd.read_sql(sql, con)
		#print(wine_df)
		reviews = wine_df.iloc[:,0]
		# print(reviews)
		for review in reviews:
		    lst[i] += str(review + " ")
		i+=1
		print("Combining Reviews for Wine #: {}".format(i))

	return lst

def stem_tokens(tokens, stemmer):
    stemmed = []
    for item in tokens:
        stemmed.append(stemmer.stem(item))
    return stemmed

def tokenize(text):
    tokens = nltk.word_tokenize(text)
    stems = stem_tokens(tokens, stemmer)
    return stems


def vectorize(list_of_strings):
	# Takes in a list of strings
	# Strings Correspond to all reviews for each wine
	# Returns matrix of all wines and all words 
	# (not binary -- weighted based on TF-IDF Parameters)

	tfidf_vectorizor = TfidfVectorizer(tokenizer = tokenize, analyzer = 'word', stop_words = 'english', ngram_range=(1,2))
	tfidf_matrix = tfidf_vectorizor.fit_transform(list_of_strings)
	print(tfidf_matrix.shape)
	return tfidf_matrix

def getSimilarity(vectorized_matrix):
	n = vectorized_matrix.shape[0]
	# dists = pairwise_distances(vectorized_matrix, metric = 'cosine', n_jobs = -1)
	sim_matrix = cosine_similarity(vectorized_matrix[0:n], vectorized_matrix)
	return sim_matrix

def run():
	list_of_reviews = getReviews()
	matrix = vectorize(list_of_reviews)
	similarity_matrix = getSimilarity(matrix)
	df = pd.DataFrame(similarity_matrix)
	df.to_csv('similarity_matrix.csv')
	print('.csv file made!')
	return similarity_matrix

run()

# list_of_reviews = getReviews()
# matrix = vectorize(list_of_reviews)
# num_processes = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=num_processes)
# result1 = pool.map(getSimilarity, matrix)


	