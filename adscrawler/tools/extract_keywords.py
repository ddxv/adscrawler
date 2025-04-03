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
nltk.download("punkt", quiet=True)
nltk.download("stopwords", quiet=True)
nltk.download("wordnet", quiet=True)

# Custom stopwords to remove personal pronouns & other irrelevant words
CUSTOM_STOPWORDS = {
    "your",
    "our",
    "my",
    "their",
    "his",
    "her",
    "its",
    "what",
    "which",
    "you",
    "it",
    "that",
    "app",
    "we",
    "the app",
    "application",
    "one",
    "them",
    "use",
    "need",
    "get",
    "who",
}
STOPWORDS = set(stopwords.words("english")).union(CUSTOM_STOPWORDS)


def clean_text(text: str) -> str:
    """Lowercases text and removes non-alphabetic characters except spaces."""
    text = text.replace("\r", ". ").replace("\n", ". ").replace("\xa0", ". ")
    text = re.sub(r"\bhttp\S*", "", text)
    return re.sub(r"[^a-zA-Z\s]", ". ", text.lower())


def count_tokens(phrase: str) -> int:
    """Count the number of tokens in a phrase."""
    return len(word_tokenize(phrase))


def extract_keywords_spacy(
    text: str, top_n: int = 10, max_tokens: int = 3
) -> list[str]:
    """Extracts noun phrase keywords using spaCy with token limit."""
    doc = nlp(text)
    keywords = []

    for chunk in doc.noun_chunks:
        if chunk.root.text.isalpha():
            # Check token count
            if count_tokens(chunk.text) <= max_tokens:
                keywords.append(chunk.text)

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


def extract_keywords_rake(text: str, top_n: int = 10, max_tokens: int = 3) -> list[str]:
    """Extracts keywords using RAKE with token limit."""
    r = Rake()
    r.extract_keywords_from_text(text)

    # Filter phrases by token count
    filtered_phrases = []
    for phrase in r.get_ranked_phrases():
        if count_tokens(phrase) <= max_tokens:
            filtered_phrases.append(phrase)

    return filtered_phrases[:top_n]


def extract_keywords(text: str, top_n: int = 10, max_tokens: int = 3) -> list[str]:
    """Extracts keywords using spaCy, NLTK, and RAKE, then returns a unique set."""
    text = clean_text(text)
    keywords = (
        extract_keywords_spacy(text, top_n, max_tokens)
        + extract_keywords_nltk(text, top_n)
        + extract_keywords_rake(text, top_n, max_tokens)
    )

    # Additional check to filter by token count
    filtered_keywords = []
    for kw in keywords:
        kw = kw.lower()
        if count_tokens(kw) <= max_tokens:
            filtered_keywords.append(kw)

    return list(sorted(set(filtered_keywords)))


if __name__ == "__main__":
    # Example usage
    description = """Example description of a store app and it's keywords in the really exciting game and marketing description."""
    keywords = extract_keywords(description)
    print(keywords)
