# Top News Article Recommendation System

This project aims to recommend the top ten news articles relevant to each query by evaluating articles based on relevance scores and filtering out similar titles. The program uses Spark for efficient data processing, applying techniques like flat mapping, broadcasting, and accumulators for performance optimization on large datasets.

## Program Overview

The program processes a large corpus of news articles, selecting only those with a "paragraph" subtype and ranking them based on relevance scores for each query. A set of custom structures and functions handles the filtering, processing, and scoring of articles, returning a list of the ten most relevant articles per query.

### Key Features

- **Textual Filtering**: Only articles with a "paragraph" subtype are processed.
- **Relevance Scoring**: DPH (Divergence from Randomness) scoring is used to rank articles based on their relevance to each query.
- **Title Similarity Check**: Ensures that similar-titled articles are filtered out to improve result diversity.
- **Efficient Data Processing**: Accumulators and broadcasting minimize redundant calculations, and data repartitioning enables parallel processing.

## Custom Structures

### 1. FilteredArticle
   - Contains essential fields for each article: content as a string, a list of tokenized content words, and term counts.
   - Includes methods to retrieve the original article and content details.

### 2. DocumentRanking
   - Holds query-based ranked results in a list structure, containing the top ten most relevant articles for each query.

## Core Functions

### 1. `ArticleProcessorMap`
   - Converts raw articles into `FilteredArticle` objects, selecting only relevant content and handling null values.
   - Uses accumulators to compute values needed for scoring, like average document length and term collections.

### 2. `QueryTermCount`
   - Returns a tuple with query terms and their frequency in the corpus, leveraging broadcast variables to reduce redundant calculations.

### 3. `DocsByDPHScore`
   - Scores each article for a given query using the DPH scoring utility, returning ranked results that feed into the top-ten selection for each query.

### 4. `DocsToQuery`
   - Maps each `DocumentRanking` to a specific query, enabling key-based grouping of results.

### 5. `FinalDocsGroupedByQuery`
   - Groups articles by query and selects the top ten results, checking for similar titles to enhance diversity in recommendations.

## Efficiency Techniques

- **Accumulators**: Used for computing aggregate statistics like corpus length and average document length.
- **Broadcast Variables**: Minimize redundant data transfer, especially with large datasets.
- **Parallel Processing**: Data is repartitioned to process articles and queries in parallel.
  
## Challenges and Solutions

1. **Large Dataset Handling**: Efficiently computed required values with accumulators and broadcast variables to avoid redundant data processing.
2. **Tracking Original Articles**: Created structures to retain references to the original news articles.
3. **Efficient Grouping by Query**: Used query-based keys for document grouping, enabling parallel processing and applying `flatMap` to grouped results.

## How to Run

1. Load the news articles dataset.
2. Run the program on a Spark-enabled environment.
3. Results will include the top ten most relevant articles for each query, filtered for similar titles and ranked by relevance score.
