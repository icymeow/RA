# JJWXC Novel N-gram Analysis

This project crawls novels from JJWXC and analyzes tag and genre trends using N-gram models from 2016 to 2025.

## Pipeline
1. Crawl novel metadata (author, tags, genre, publication date)
2. Data cleaning and normalization
3. N-gram frequency analysis
4. Visualization using Streamlit

## Project Structure

RA_jjwxc/
│
├── Ngram/ # N-gram analysis scripts
├── topic-modelling/ # Topic modeling experiments
├── jj_web_multi_year.py
├── requirements.txt
├── README.md
└── .gitignore


## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
