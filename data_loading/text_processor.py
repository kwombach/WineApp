import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet

import string
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

##########################
# REQUIRED NLTK PACKAGES #
##########################
'''
nltk.download('wordnet')
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('stop words')
'''
##########################


def remove_stop_then_lem(text, stop_words):
    return tokenize(remove(text, stop_words))


def remove(text, stop_words):
    text = "".join(l for l in text if l not in string.punctuation)

    # stop_words = stopwords.words('english')
    #
    # for line in open('low_weight_words.txt', 'r'):
    #     stop_words.append(line.strip())
    # print(stop_words)

    word_tokens = word_tokenize(text)

    filtered_sentence = [w for w in word_tokens if not w in stop_words]
    sentence = ' '.join(filtered_sentence)
    return sentence


def tokenize(text):
    tokens = nltk.word_tokenize(text)
    stems = lemmatize_tokens(tokens)
    return stems


def lemmatize_tokens(tokens):

    wl = WordNetLemmatizer()

    lemmatized = []
    for item in tokens:
        lemmatized.append(wl.lemmatize(wl.lemmatize(item, 'a')))
    string = ' '.join(lemmatized)
    return string

