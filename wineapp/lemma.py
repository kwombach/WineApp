import nltk
import string
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet



wl = WordNetLemmatizer()

def tokenize(text):
    tokens = nltk.word_tokenize(text)
    stems = lemmatize_tokens(tokens)
    return stems

def lemmatize_tokens(tokens):
    lemmatized = []
    for item in tokens:
        lemmatized.append(wl.lemmatize(wl.lemmatize(item, 'a')))
    string = ' '.join(lemmatized)
    return string

