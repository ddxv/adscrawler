import re
from collections import Counter

import nltk
import spacy
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from rake_nltk import Rake

# Load spaCy model
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Downloading spaCy model...")
    import os

    os.system("python -m spacy download en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

# Ensure necessary NLTK resources are downloaded
nltk.download("punkt")
nltk.download("punkt_tab")
nltk.download("stopwords")
nltk.download("wordnet")

# Custom stopwords to remove personal pronouns & other irrelevant words
CUSTOM_STOPWORDS = {"your", "our", "my", "their", "his", "her", "its", "what", "which"}

STOPWORDS = set(stopwords.words("english")).union(CUSTOM_STOPWORDS)


def clean_text(text: str) -> str:
    """Lowercases text and removes non-alphabetic characters except spaces."""
    return re.sub(r"[^a-zA-Z\s]", "", text.lower())


def extract_keywords_spacy(text: str, top_n: int = 10) -> list[str]:
    """Extracts noun phrase keywords using spaCy."""
    doc = nlp(text)
    keywords = [chunk.text for chunk in doc.noun_chunks if chunk.root.text.isalpha()]
    keyword_freq = Counter(keywords)
    return [kw for kw, _ in keyword_freq.most_common(top_n)]


def extract_keywords_nltk(text: str, top_n: int = 10) -> list[str]:
    """Extracts lemmatized keywords using NLTK."""
    words = word_tokenize(text)
    lemmatizer = WordNetLemmatizer()
    keywords = {
        lemmatizer.lemmatize(word)
        for word in words
        if word.isalpha() and word not in STOPWORDS
    }
    return list(keywords)[:top_n]


def extract_keywords_rake(text: str, top_n: int = 10) -> list[str]:
    """Extracts keywords using RAKE (Rapid Automatic Keyword Extraction)."""
    r = Rake()
    r.extract_keywords_from_text(text)
    return r.get_ranked_phrases()[:top_n]


def extract_keywords(text: str, top_n: int = 10) -> list[str]:
    """Extracts keywords using spaCy, NLTK, and RAKE, then returns a unique set."""
    text = clean_text(text)

    keywords = (
        extract_keywords_spacy(text, top_n)
        + extract_keywords_nltk(text, top_n)
        + extract_keywords_rake(text, top_n)
    )
    keywords = [x.lower() for x in keywords if len(x) <= 255]

    return list(sorted(set(keywords)))


if __name__ == "__main__":
    # Example usage
    description = """Example description of a store app and it's keywords in the really exciting game and marketing description."""

    keywords = extract_keywords(description)
    print(keywords)
