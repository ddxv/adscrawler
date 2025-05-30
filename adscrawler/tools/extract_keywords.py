import re
from collections import Counter

import nltk
import spacy
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from rake_nltk import Rake
from sklearn.feature_extraction.text import TfidfVectorizer

from adscrawler.connection import PostgresCon
from adscrawler.queries import (
    query_all_store_app_descriptions,
    query_keywords_base,
)

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
nltk.download("averaged_perceptron_tagger_eng", quiet=True)

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
    "this app",
    "application",
    "this application",
    "one",
    "them",
    "use",
    "need",
    "get",
    "who",
    "i",
    "also",
    "our app",
    "the game",
    "youll",
    "youre",
    "whos",
    "whatsway",
    "lets",
    "let",
    "set",
    "com",
}
STOPWORDS = set(stopwords.words("english")).union(CUSTOM_STOPWORDS)


def clean_text(text: str) -> str:
    """Lowercases text and removes non-alphabetic characters except spaces."""
    text = (
        text.replace("\r", ". ")
        .replace("\n", ". ")
        .replace("\t", ". ")
        .replace("\xa0", ". ")
        .replace("•", ". ")
        .replace("'", "")
        .replace("’", "")
        .replace("-", " ")
    )
    text = re.sub(r"\bhttp\S*", "", text)
    text = re.sub(r"\bwww\S*", "", text)
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
                if not any(token.is_stop or token in STOPWORDS for token in chunk):
                    keywords.append(chunk.text)

    keyword_freq = Counter(keywords)
    return [kw for kw, _ in keyword_freq.most_common(top_n)]


def extract_keywords_nltk(text: str, top_n: int = 10) -> list[str]:
    """Extracts lemmatized keywords using NLTK with frequency ranking."""
    words = word_tokenize(text)
    pos_tags = nltk.pos_tag(words)
    lemmatizer = WordNetLemmatizer()
    processed_words = []
    for word, tag in pos_tags:
        # Only process alphabetic words that aren't stopwords
        if word.isalpha() and word.lower() not in STOPWORDS:
            # Convert POS tag to WordNet format for better lemmatization
            tag_first_char = tag[0].lower()
            wordnet_pos = {
                "n": wordnet.NOUN,
                "v": wordnet.VERB,
                "a": wordnet.ADJ,
                "r": wordnet.ADV,
            }.get(tag_first_char, wordnet.NOUN)
            lemma = lemmatizer.lemmatize(word.lower(), wordnet_pos)
            if len(lemma) > 2:
                processed_words.append(lemma)
    word_freq = Counter(processed_words)
    return [word for word, freq in word_freq.most_common(top_n)]


def extract_keywords_rake(text: str, top_n: int = 10, max_tokens: int = 3) -> list[str]:
    """Extracts keywords using RAKE with token limit."""
    r = Rake()
    r.extract_keywords_from_text(text)

    # Filter phrases by token count
    filtered_phrases = []
    for phrase in r.get_ranked_phrases():
        if count_tokens(phrase) <= max_tokens:
            filtered_phrases.append(phrase)
    filtered_phrases = [
        phrase for phrase in filtered_phrases if phrase not in STOPWORDS
    ]
    return filtered_phrases[:top_n]


def extract_keywords(
    text: str,
    database_connection: PostgresCon,
    top_n: int = 5,
    max_tokens: int = 2,
) -> list[str]:
    """Extracts keywords using spaCy, NLTK, and RAKE, then returns a unique set."""
    text = clean_text(text)
    words_spacy = extract_keywords_spacy(text, top_n, max_tokens)
    words_nltk = extract_keywords_nltk(text, top_n)
    # words_rake = extract_keywords_rake(text, top_n, max_tokens)
    keywords = words_spacy + words_nltk

    # Additional check to filter by token count
    filtered_keywords = []
    for kw in keywords:
        kw = kw.lower()
        if count_tokens(kw) <= max_tokens:
            filtered_keywords.append(kw)

    # Remove stopwords from filtered keywords
    filtered_keywords = [kw for kw in filtered_keywords if kw not in STOPWORDS]

    keywords_base = query_keywords_base(database_connection)

    matched_base_keywords = keywords_base[
        keywords_base["keyword_text"].apply(lambda x: x in text)
    ]
    matched_base_keywords = matched_base_keywords["keyword_text"].str.strip().tolist()
    combined_keywords = list(sorted(set(filtered_keywords + matched_base_keywords)))

    return combined_keywords


def get_global_keywords(database_connection: PostgresCon) -> list[str]:
    """Get the global keywords from the database.
    NOTE: This takes about ~5-8GB of RAM for 50k keywords and 200k descriptions. For now run manually.
    """
    df = query_all_store_app_descriptions(database_connection=database_connection)
    cleaned_texts = [
        clean_text(description) for description in df["description"].tolist()
    ]

    vectorizer = TfidfVectorizer(
        ngram_range=(1, 3),  # Include 1-grams, 2-grams, and 3-grams
        stop_words=list(STOPWORDS),
        max_df=0.80,  # Ignore terms in >80% of docs (too common)
        min_df=30,  # Ignore terms in <10 docs (too rare)
        max_features=50000,
    )

    tfidf_matrix = vectorizer.fit_transform(cleaned_texts)

    feature_names = vectorizer.get_feature_names_out()
    global_scores = tfidf_matrix.sum(axis=0).A1  # Sum scores per term

    keyword_scores = list(zip(feature_names, global_scores, strict=False))
    keyword_scores.sort(key=lambda x: x[1], reverse=True)

    global_keywords = [kw for kw, score in keyword_scores if kw not in STOPWORDS]

    return global_keywords
