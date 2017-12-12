import sys
import sqlite3
import pandas as pd
from fuzzywuzzy import process
from fuzzywuzzy import fuzz


def selectWine(wine_string):
	con = sqlite3.connect('wineapp.db')
	cur = con.cursor()
	sql = '''SELECT wine_wineId_int, wine_name_plain FROM wines WHERE wine_qty_reviews > 9 ORDER by wine_qty_reviews'''
	wines = pd.read_sql(sql, con)
	wines = wines.set_index(['wine_wineId_int'])
	choices = wines['wine_name_plain']
	wine_match, score, wine_match_id = process.extract(wine_string, choices, scorer = fuzz.partial_ratio, limit=1)[0]
	# print(wines)
	# print(wine_match_id, wine_match)
	return wine_match_id, score

def recommed(wine_id):
	con = sqlite3.connect('wineapp.db')
	cur = con.cursor()
	sql = '''SELECT  w1.wine_name_plain as query,
			 w2.wine_name_plain as similar_wine_1,
			 w3.wine_name_plain as similar_wine_2,
			 w4.wine_name_plain as similar_wine_3,
			 w5.wine_name_plain as similar_wine_4,
			 w6.wine_name_plain as similar_wine_5,
			 w7.wine_name_plain as similar_wine_6,
			 w8.wine_name_plain as similar_wine_7,
			 w9.wine_name_plain as similar_wine_8,
			 w10.wine_name_plain as similar_wine_9,
			 w11.wine_name_plain as similar_wine_10
		   FROM similar_wines sw 
			LEFT JOIN wines w1
			  ON sw.similar_wines_wineId = w1.wine_wineId_int
			LEFT JOIN wines w2
			  ON sw.wineId_match_1 = w2.wine_wineId_int
			LEFT JOIN wines w3
			  ON sw.wineId_match_2 = w3.wine_wineId_int
			LEFT JOIN wines w4
			  ON sw.wineId_match_3 = w4.wine_wineId_int
			LEFT JOIN wines w5
			  ON sw.wineId_match_4 = w5.wine_wineId_int
			LEFT JOIN wines w6
			  ON sw.wineId_match_5 = w6.wine_wineId_int
			LEFT JOIN wines w7
			  ON sw.wineId_match_6 = w7.wine_wineId_int
			LEFT JOIN wines w8
			  ON sw.wineId_match_7 = w8.wine_wineId_int
			LEFT JOIN wines w9
			  ON sw.wineId_match_8 = w9.wine_wineId_int  
			LEFT JOIN wines w10
			  ON sw.wineId_match_9 = w10.wine_wineId_int
			LEFT JOIN wines w11
			  ON sw.wineId_match_10 = w11.wine_wineId_int
			WHERE similar_wines_wineId = ''' + str(wine_id)

	sim_wines = pd.read_sql(sql, con)
	return sim_wines

def getRecs(wine_query):
	arg = str(wine_query)
	wine_id, score = selectWine(arg)
	if score > 50:
		recs = recommed(wine_id)
		return recs
	else:
		print('No Match')








