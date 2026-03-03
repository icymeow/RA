import pandas as pd
from collections import Counter
from itertools import zip_longest
# Ngram_genre_analysis.py
# ================= CONFIG =================
INPUT_CSV = "jjwxc_10yrs_withtags_by_year.csv"
OUTPUT_XLSX = "jjwxc_genre_ngram_analysis.xlsx"

MAX_N = 2   # 1=unigram, 2=bigram, 3=trigram (建议2)

# ================= FUNCTIONS =================
def parse_type_tokens(type_str):
    if pd.isna(type_str):
        return []
    return [t.strip() for t in str(type_str).split("-") if t.strip()]

def generate_ngrams(tokens, n):
    if len(tokens) < n:
        return []
    return list(zip(*[tokens[i:] for i in range(n)]))

# ================= MAIN =================
def main():
    df = pd.read_csv(INPUT_CSV, encoding="utf-8-sig", on_bad_lines="skip", low_memory=False)

    # Year
    if "发表年份" not in df.columns:
        df["发表年份"] = pd.to_datetime(df["发表时间"], errors="coerce").dt.year
        df["发表年份"] = df["发表年份"].fillna(df["sheet"])

    df = df.dropna(subset=["发表年份"])
    df["发表年份"] = df["发表年份"].astype(int)

    # Parse 类型 tokens
    df["type_tokens"] = df["类型"].apply(parse_type_tokens)

    records = []

    for year, group in df.groupby("发表年份"):
        all_ngrams = []

        for tokens in group["type_tokens"]:
            for n in range(1, MAX_N + 1):
                all_ngrams.extend([(n, g) for g in generate_ngrams(tokens, n)])

        counter = Counter(all_ngrams)

        for (n, gram), freq in counter.items():
            records.append({
                "year": year,
                "N": n,
                "Ngram": " → ".join(gram),
                "freq": freq
            })

    out = pd.DataFrame(records)
    out.sort_values(["year", "N", "freq"], ascending=[True, True, False], inplace=True)

    out.to_excel(OUTPUT_XLSX, index=False)
    print("Saved:", OUTPUT_XLSX)

if __name__ == "__main__":
    main()
