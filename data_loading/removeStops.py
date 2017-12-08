import string
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


def remove(text):
	text = "".join(l for l in text if l not in string.punctuation)

	stop_words = stopwords.words('english')

	for line in open('low_weight_words.txt', 'r'):
		stop_words.append(line.strip())
	print(stop_words)
	 
	word_tokens = word_tokenize(text)
	 
	filtered_sentence = [w for w in word_tokens if not w in stop_words]
	sentence = ' '.join(filtered_sentence)
	return sentence