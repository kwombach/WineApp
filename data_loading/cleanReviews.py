import sqlite3
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity 

def getReviews():
	con = sqlite3.connect('wineapp.db')
	cur = con.cursor()
	sql = '''SELECT wine_wineId_int FROM wines ORDER by wine_qty_reviews DESC LIMIT 150'''
	wine_ids = pd.read_sql(sql, con)

	n = wine_ids.shape[0]
	print(n)
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
		print(i)

	return lst

def vectorize(list_of_strings):
	# Takes in a list of strings
	# Strings Correspond to all reviews for each wine
	# Returns matrix of all wines and all words 
	# (not binary -- weighted based on TF-IDF Parameters)

	tfidf_vectorizor = TfidfVectorizer(analyzer = 'word', stop_words = 'english', ngram_range=(1,2))
	tfidf_matrix = tfidf_vectorizor.fit_transform(list_of_strings)
	print(tfidf_matrix.shape)
	return tfidf_matrix

def getSimilarity(vectorized_matrix):
	n = vectorized_matrix.shape[0]
	sim_matrix = cosine_similarity(vectorized_matrix[0:n], vectorized_matrix)
	return sim_matrix

def run():
	list_of_reviews = getReviews()
	matrix = vectorize(list_of_reviews)
	similarity_matrix = getSimilarity(matrix)
	df = pd.DataFrame(similarity_matrix)
	df.to_csv('similarity_matrix.csv')
	return similarity_matrix

run()


