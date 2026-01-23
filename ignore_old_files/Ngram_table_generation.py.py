# 步骤四，N-gram 分析，按年份统计各 N-gram 频次，输出 XLSX 文件

import ast
import pandas as pd
from collections import Counter
from itertools import chain

# ================== 配置 ==================
INPUT_XLSX = "jjwxc_10yrs_withtags_by_year.xlsx"   # 步骤三生成的 XLSX 文件
OUTPUT_XLSX = "jjwxc_ngram_analysis.xlsx"

MAX_N = 2   # 1 = unigram, 2 = bigram

# ================== 工具函数 ==================
def parse_list_cell(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            v = ast.literal_eval(x)
            if isinstance(v, list):
                return v
        except Exception:
            return [i for i in x.split() if i]
    return []

def generate_ngrams(tokens, n):
    return list(zip(*[tokens[i:] for i in range(n)]))

# ================== 主流程 ==================
def main():
    df = pd.read_excel(INPUT_XLSX)

    # 年份
    if "发表年份" not in df.columns:
        df["发表年份"] = pd.to_datetime(df["发表时间"], errors="coerce").dt.year

    # 标签 list
    df["内容标签_list"] = df["内容标签_list"].apply(parse_list_cell)

    records = []

    for year, group in df.groupby("发表年份"):
        all_ngrams = []

        for tags in group["内容标签_list"]:
            if not tags:
                continue
            for n in range(1, MAX_N + 1):
                all_ngrams.extend(
                    [(n, g) for g in generate_ngrams(tags, n)]
                )

        counter = Counter(all_ngrams)

        for (n, gram), freq in counter.items():
            records.append({
                "发表年份": int(year),
                "N": n,
                "N-gram": " ".join(gram),
                "频次": int(freq)
            })

    out_df = pd.DataFrame(records)
    out_df.sort_values(["N", "发表年份", "频次"], ascending=[True, True, False], inplace=True)

    out_df.to_excel(OUTPUT_XLSX, index=False)
    print(f"✅ N-gram 分析完成，已输出：{OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
