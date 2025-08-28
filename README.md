# Russian Protests Research

This project explores protest activity in Russia since 2022 using data from **VKontakte**.  
It powered an investigation published in **Novaya Gazeta Europe**: [Picking Your Battles (June 2025)](https://novayagazeta.eu/articles/2025/06/27/picking-your-battles-en)  

Journalists analyzed nearly **40,000 protests across Russia over the last three years**, based on posts on VKontakte.  

---

## Project Overview

The repository contains three main programs:  

1. **Data Collection**  
   - Downloads posts from VKontakte groups and newsfeed using the platform API.  

2. **Data Vectorization & Cleaning**  
   - Cleans posts via keyword filtering.  
   - Converts texts into embeddings (using OpenAI API).  
   - Clusters posts and removes duplicates or semantically similar entries.  

3. **Entity Extraction with LLM**  
   - Performs final semantic cleaning.  
   - Extracts named entities (locations, organizations, protest types) using Gemini API queries.

---

## Prerequisites

Before running any programs, you'll need to obtain API tokens:

### Required API Tokens

1. **VKontakte Token(s)** 
   - One or more VK access tokens for API requests
   - Multiple tokens recommended for multithreading
   - Obtain from: [VK API Documentation](https://dev.vk.com/api/access-token/getting-started)

2. **OpenAI Token**
   - Required for text vectorization (converting posts to embeddings)
   - Obtain from: [OpenAI API Keys](https://platform.openai.com/api-keys)

3. **Google Gemini Token(s)**
   - One or more Gemini API tokens for LLM entity extraction
   - Multiple tokens recommended for processing large datasets
   - Obtain from: [Google AI Studio](https://makersuite.google.com/app/apikey)

---

## Usage Instructions

### 1. Data Collection (`download_posts_vk.sh`)

This script downloads posts from VKontakte using various scraping methods.

```bash
# Make the script executable
chmod +x src/scraping/download_posts_vk.sh

# Run the script
cd src/scraping && ./download_posts_vk.sh
```

**What it does:**
- Prompts you to enter VK API tokens (press Enter after each token, empty input to finish)
- Runs multiple Python scrapers:
  - `vk_search_cities.py` - searches city identifiers using Russian population data
  - `vk_search_groups.py` - searches for groups in specific cities by keywords
  - `vk_scraper_posts_groups.py` - scrapes posts from target groups
  - `vk_scraper_posts_newsfeed.py` - scrapes from news feeds
  - `clean_posts_by_keywords.py` - cleans posts by keywords
  - `vk_scraper_groups_info.py` - collects group metadata
  - `vk_scraper_users_info.py` - collects user information
  - `merge_groups_users_info.py` - merges collected metadata

**Input:** VK API tokens (entered interactively), Russian population data  
**Output:** Raw post data in JSONL/CSV format

### 2. Data Processing (`drop_duplicates.sh`)

This script processes the collected data by vectorizing texts and removing duplicates.

```bash
# Make the script executable
chmod +x src/preprocessing/drop_duplicates.sh

# Run the script
cd src/preprocessing && ./drop_duplicates.sh
```

**What it does:**
- Prompts for OpenAI API token
- Converts posts to embeddings using `posts_to_embeddings.py`
- Clusters similar posts within 10 day periods `cluster_embeddings_by_time.py`
- Removes duplicate/similar content using `remove_similar_posts.py`

**Input:** OpenAI API token, raw post data  
**Output:** Deduplicated dataset

### 3. LLM Processing (`process_posts_ai.sh`)

This script uses Gemini LLM to extract entities and perform final semantic analysis.

```bash
# Make the script executable
chmod +x src/llm/process_posts_ai.sh

# Run the script
cd src/llm && ./process_posts_ai.sh
```

**What it does:**
- Prompts for Gemini API token(s)
- Uses `query_gemini.py` to perform final semantic cleaning of the dataset
- Uses `query_gemini.py` to extract:
  - Geographic locations
  - Organization names
  - Protest types and categories
  - Other named entities

**Input:** Gemini API token(s), processed post data  
**Output:** Final dataset with extracted entities

---

## Setup

1. Clone the repository
2. Obtain all required API tokens (see Prerequisites section)
3. Run the scripts in order: collection → processing → LLM analysis

---

## Token Management Tips

- **VK Tokens**: Use multiple tokens to avoid rate limits. The scripts will rotate through available tokens automatically.
- **Gemini Tokens**: Multiple tokens significantly speed up large-scale entity extraction due to parallel processing.
- **OpenAI Token**: One token is typically sufficient for embeddings generation, but ensure adequate quota for your dataset size.