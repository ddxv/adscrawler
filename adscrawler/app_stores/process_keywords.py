# noqa: PLC0415
import datetime
import os
import re
from collections import Counter

import pandas as pd

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import (
    delete_and_insert,
    query_all_store_app_descriptions,
    query_apps_to_process_keywords,
    query_keywords_base,
    upsert_df,
)

logger = get_logger(__name__)

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
    "application",
    "one",
    "ones",
    "dont",
    "us",
    "takes",
    "take",
    "them",
    "use",
    "uses",
    "need",
    "get",
    "who",
    "i",
    "also",
    "youll",
    "youre",
    "whos",
    "whats",
    "lets",
    "let",
    "set",
    "com",
    "game",
}


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


def clean_df_text(df: pd.DataFrame, column: str) -> pd.DataFrame:
    # Note these are same as clean_text function
    df[column] = (
        df[column]
        .str.replace("\r", ". ")
        .replace("\n", ". ")
        .replace("\t", ". ")
        .replace("\xa0", ". ")
        .replace("•", ". ")
        .replace("'", "")
        .replace("’", "")
        .replace("-", " ")
        .replace(r"\bhttp\S*", "", regex=True)
        .replace(r"\bwww\S*", "", regex=True)
        .replace(r"[^a-zA-Z\s]", ". ", regex=True)
        .str.lower()
    )
    return df


def clean_df_text(df: pd.DataFrame, column: str) -> pd.DataFrame:
    # 1. Extract the Series to work on it efficiently
    s = df[column].astype(str)

    # 2. Convert all structural separators and whitespace-like noise to periods
    # This handles \r, \n, \t, \xa0, and bullets in one pass
    s = s.str.replace(r"[\r\n\t\xa0•]+", ". ", regex=True)

    # 3. Handle Apostrophes and Hyphens specifically (don't turn them into periods)
    s = s.str.replace(r"['’]", "", regex=True)
    s = s.str.replace(r"-", " ", regex=True)

    # 4. Remove URLs
    s = s.str.replace(r"\b(?:http|www)\S*", "", regex=True, flags=re.IGNORECASE)

    # 5. Replace everything else that isn't a letter or space with a period
    # We remove \s from the exclusion so newlines/tabs are finally nuked if any remain
    s = s.str.replace(r"[^a-zA-Z ]", ". ", regex=True)

    # 6. Final Cleanup: lowercase and collapse multiple spaces/periods
    # "Design. . . . Community" -> "design. community"
    df[column] = (
        s.str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"\.+", ".", regex=True)
    )

    return df


def count_tokens(phrase: str) -> int:
    """Count the number of tokens in a phrase."""
    from nltk.tokenize import word_tokenize

    return len(word_tokenize(phrase))


def extract_keywords_spacy(
    text: str, top_n: int = 10, max_tokens: int = 3
) -> list[str]:
    """Extracts noun phrase keywords using spaCy with token limit."""
    # Load spaCy model
    import spacy  # noqa: PLC0415
    from nltk.corpus import stopwords

    mystopwords = set(stopwords.words("english")).union(CUSTOM_STOPWORDS)

    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError:
        print("Downloading spaCy model...")

        os.system("python -m spacy download en_core_web_sm")
        nlp = spacy.load("en_core_web_sm")

    doc = nlp(text)
    keywords = []

    for chunk in doc.noun_chunks:
        if chunk.root.text.isalpha():
            # Check token count
            if count_tokens(chunk.text) <= max_tokens:
                if not any(token.is_stop or token in mystopwords for token in chunk):
                    keywords.append(chunk.text)

    keyword_freq = Counter(keywords)
    return [kw for kw, _ in keyword_freq.most_common(top_n)]


def extract_keywords_nltk(text: str, top_n: int = 10) -> list[str]:
    """Extracts lemmatized keywords using NLTK with frequency ranking."""
    from nltk.tokenize import word_tokenize

    words = word_tokenize(text)
    # Ensure necessary NLTK resources are downloaded
    import nltk
    from nltk.corpus import stopwords, wordnet
    from nltk.stem import WordNetLemmatizer

    mystopwords = set(stopwords.words("english")).union(CUSTOM_STOPWORDS)

    nltk.download("punkt", quiet=True)
    nltk.download("stopwords", quiet=True)
    nltk.download("wordnet", quiet=True)
    nltk.download("averaged_perceptron_tagger_eng", quiet=True)

    pos_tags = nltk.pos_tag(words)
    lemmatizer = WordNetLemmatizer()
    processed_words = []
    for word, tag in pos_tags:
        # Only process alphabetic words that aren't stopwords
        if word.isalpha() and word.lower() not in mystopwords:
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


def get_stopwords() -> set[str]:
    """Get the stopwords from NLTK and spaCy."""
    import spacy
    from nltk.corpus import stopwords

    nlp = spacy.load("en_core_web_sm")
    spacy_stopwords = nlp.Defaults.stop_words
    mystopwords = (
        set(stopwords.words("english")).union(CUSTOM_STOPWORDS).union(spacy_stopwords)
    )
    return list(mystopwords)


def extract_keywords_rake(text: str, top_n: int = 10, max_tokens: int = 3) -> list[str]:
    """Extracts keywords using RAKE with token limit."""
    from rake_nltk import Rake

    mystopwords = get_stopwords()

    r = Rake()
    r.extract_keywords_from_text(text)

    # Filter phrases by token count
    filtered_phrases = []
    for phrase in r.get_ranked_phrases():
        if count_tokens(phrase) <= max_tokens:
            filtered_phrases.append(phrase)
    filtered_phrases = [
        phrase for phrase in filtered_phrases if phrase not in mystopwords
    ]
    return filtered_phrases[:top_n]


def extract_unique_app_keywords_from_text(
    text: str,
    top_n: int = 2,
    max_tokens: int = 1,
) -> list[str]:
    """Extracts keywords using spaCy, NLTK, and RAKE, then returns a unique set."""

    mystopwords = get_stopwords()

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
    filtered_keywords = [kw for kw in filtered_keywords if kw not in mystopwords]

    # keywords_base = query_keywords_base(database_connection)
    # matched_base_keywords = keywords_base[
    #         keywords_base["keyword_text"].apply(lambda x: x in description_text)
    #     ]

    # matched_base_keywords = matched_base_keywords["keyword_text"].str.strip().tolist()
    # combined_keywords = list(sorted(set(filtered_keywords + matched_base_keywords)))
    combined_keywords = list(sorted(set(filtered_keywords)))

    return combined_keywords


# def pos_filter_descriptions(texts: pd.Series) -> list[str]:
#     """Batch processes text to keep only NOUN, PROPN, and ADJ."""
#     import spacy
#     #spacy.cli.download("en_core_web_sm")
#     # Load model once, disable what we don't need for speed
#     nlp = spacy.load("en_core_web_sm", disable=["parser", "ner", "lemmatizer"])
#     # Note: We keep the lemmatizer if you want 'games' -> 'game',
#     # but for raw speed, you can disable it too.

#     processed_texts = []
#     # nlp.pipe processes in batches and is much faster than .apply()
#     for doc in nlp.pipe(texts, batch_size=1000, n_process=-1): # -1 uses all cores
#         tokens = [
#             token.text.lower()
#             for token in doc
#             if token.pos_ in {"NOUN", "PROPN", "ADJ"} and not token.is_stop
#         ]
#         processed_texts.append(" ".join(tokens))
#     return processed_texts

# 2000/4 = 15000
# 1000/8 = 26000
# 2000/8 = oom
# 200/14 = 30000
# 1000/14 = oom
# 600/14 = 21000
# 800/12 = 24000


def pos_filter_descriptions(texts: pd.Series, batch_size=1000) -> list[str]:
    # Load light model; disable everything but the tagger (for POS) and lemmatizer
    import spacy
    import tqdm

    nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
    processed_texts = []
    # for doc in nlp.pipe(texts, batch_size=batch_size, n_process=4):
    for doc in tqdm.tqdm(
        nlp.pipe(texts, batch_size=batch_size, n_process=12), total=len(texts)
    ):
        # Keep only Nouns, Proper Nouns, and Adjectives
        tokens = [
            token.lemma_.lower()  # Use lemma to group 'games' and 'game'
            for token in doc
            if token.pos_ in {"NOUN", "PROPN", "ADJ"} and not token.is_stop
        ]
        processed_texts.append(" ".join(tokens))
    return processed_texts


def get_global_keywords(database_connection: PostgresCon) -> list[str]:
    """Get the global keywords from the database.
    NOTE: This takes about ~5-8GB of RAM for 50k keywords and 200k descriptions. For now run manually.
    """

    from sklearn.feature_extraction.text import TfidfVectorizer  # noqa: PLC0415

    mystopwords = get_stopwords()

    df = query_all_store_app_descriptions(
        language_slug="en", database_connection=database_connection
    )

    df = pd.read_pickle("descriptions_df.pkl")
    df = clean_df_text(df, "description")
    # df.to_pickle("descriptions_df_cleaned.pkl")
    df = pd.read_pickle("descriptions_df_cleaned.pkl")

    df["description"] = pos_filter_descriptions(df["description"])

    vectorizer = TfidfVectorizer(
        ngram_range=(1, 3),  # Include 1-grams, 2-grams, 3-grams
        stop_words=list(mystopwords),
        max_df=0.20,  # Ignore terms in at least % of docs (too common)
        min_df=100,  # Ignore terms in <x docs (too rare)
        max_features=20000,
    )

    # Main slow/memory-intensive step
    tfidf_matrix = vectorizer.fit_transform(df["description"])

    feature_names = vectorizer.get_feature_names_out()
    global_scores = tfidf_matrix.sum(axis=0).A1  # Sum scores per term
    keyword_scores = list(zip(feature_names, global_scores, strict=False))
    keyword_scores.sort(key=lambda x: x[1], reverse=True)
    global_keywords = [kw for kw, score in keyword_scores if kw not in mystopwords]
    return global_keywords


def insert_global_keywords(database_connection: PostgresCon) -> None:
    """Insert global keywords into the database.
    NOTE: This takes about ~15-20GB of RAM for 50k keywords and 3.5m descriptions. For now run manually.
    """

    global_keywords = get_global_keywords(database_connection)
    global_keywords_df = pd.DataFrame(global_keywords, columns=["keyword_text"])
    table_name = "keywords"
    insert_columns = ["keyword_text"]
    key_columns = ["keyword_text"]
    keywords_df = upsert_df(
        table_name=table_name,
        df=global_keywords_df,
        insert_columns=insert_columns,
        key_columns=key_columns,
        database_connection=database_connection,
        return_rows=True,
    )
    keywords_df = keywords_df.rename(columns={"id": "keyword_id"})
    keywords_df = keywords_df[["keyword_id"]]
    table_name = "keywords_base"
    insert_columns = ["keyword_id"]
    key_columns = ["keyword_id"]
    keywords_df.to_sql(
        name=table_name,
        con=database_connection.engine,
        if_exists="replace",
        index=False,
        schema="public",
    )


def process_app_keywords(database_connection: PostgresCon, limit: int) -> None:
    """Process app keywords.

    While Python might be less efficient than SQL it's more flexible for
    the query input limiting which apps and when to run.
    This way apps can be processed in batches and only when really needed.
    """
    logger.info(f"Extracting app keywords for {limit} apps")
    extract_app_keywords_from_descriptions(database_connection, limit)
    logger.info("Extracted app keywords finished")


def extract_app_keywords_from_descriptions(
    database_connection: PostgresCon, limit: int
) -> None:
    """Process keywords for app descriptions."""
    description_df = query_apps_to_process_keywords(database_connection, limit=limit)
    keywords_base = query_keywords_base(database_connection)
    keywords_base["keyword_text"] = (
        " " + keywords_base["keyword_text"].str.lower() + " "
    )
    description_df["description_text"] = (
        " "
        + description_df["description_short"]
        + " "
        + description_df["description"]
        + " "
    ).str.lower()
    description_df = clean_df_text(description_df, "description_text")
    all_keywords_dfs = []
    logger.info(f"Processing {len(description_df)} app descriptions")
    for _i, row in description_df.iterrows():
        logger.debug(f"Processing app description: {_i}/{len(description_df)}")
        description_id = row["description_id"]
        store_app = row["store_app"]
        description_text = row["description_text"]
        matched_base_keywords = keywords_base[
            keywords_base["keyword_text"].apply(
                lambda x, text=description_text: x in text
            )
        ]
        keywords_df = pd.DataFrame(
            matched_base_keywords, columns=["keyword_text", "keyword_id"]
        )
        keywords_df["description_id"] = description_id
        keywords_df["store_app"] = store_app
        all_keywords_dfs.append(keywords_df)
    main_keywords_df = pd.concat(all_keywords_dfs)
    main_keywords_df = main_keywords_df[["store_app", "description_id", "keyword_id"]]
    main_keywords_df["extracted_at"] = datetime.datetime.now(tz=datetime.UTC)
    table_name = "app_keywords_extracted"
    insert_columns = ["store_app", "description_id", "keyword_id", "extracted_at"]
    logger.info(f"Delete and insert {len(main_keywords_df)} app keywords")
    delete_and_insert(
        df=main_keywords_df,
        table_name=table_name,
        schema="public",
        database_connection=database_connection,
        insert_columns=insert_columns,
        delete_by_keys=["store_app"],
        delete_keys_have_duplicates=True,
    )
