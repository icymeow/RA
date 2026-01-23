# 步骤四：从包含标签的 CSV 里做 N-gram 分析，输出 Excel 报告
# ngram_analysis_from_csv.py

import ast
import pandas as pd
from collections import Counter

# ================== 配置 ==================
INPUT_CSV = "jjwxc_10yrs_withtags.csv"
OUTPUT_XLSX = "jjwxc_tag_ngram_analysis.xlsx"

MAX_N = 2   # 1 = unigram, 2 = bigram


# ================== 工具函数 ==================
def parse_list_cell(x):
    """把单元格内容转成 list[str]"""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        # 尝试解析 "['甜文', '爽文']"
        try:
            v = ast.literal_eval(s)
            if isinstance(v, list):
                return [str(i).strip() for i in v if str(i).strip()]
        except Exception:
            pass
        # 兜底：按空格切
        return [i for i in s.split() if i]
    return []


def generate_ngrams(tokens, n):
    if len(tokens) < n:
        return []
    return list(zip(*[tokens[i:] for i in range(n)]))


def get_year_column(df: pd.DataFrame) -> pd.Series:
    """
    优先用 发表时间 解析年份；
    解析不到的用 sheet（通常就是年份）兜底；
    """
    year = pd.Series([pd.NA] * len(df), index=df.index, dtype="Int64")

    if "发表时间" in df.columns:
        dt = pd.to_datetime(df["发表时间"], errors="coerce")
        year = dt.dt.year.astype("Int64")

    # 兜底：用 sheet 当年份（你的数据一般 sheet=2014/2015/...）
    if "sheet" in df.columns:
        mask = year.isna()
        # 提取4位数字年份
        sheet_year = (
            df.loc[mask, "sheet"]
            .astype(str)
            .str.extract(r"(\d{4})", expand=False)
        )
        year.loc[mask] = pd.to_numeric(sheet_year, errors="coerce").astype("Int64")

    return year


# ================== 主流程 ==================
def main():
    print(f"[INFO] Reading CSV: {INPUT_CSV}")

    # 你的 CSV 可能有坏行（之前报过 Expected fields...），这里直接跳过坏行
    df = pd.read_csv(
        INPUT_CSV,
        encoding="utf-8-sig",
        on_bad_lines="skip",
        low_memory=False
    )

    print(f"[INFO] Loaded rows: {len(df)}")

    # 年份列
    df["发表年份"] = get_year_column(df)

    # 标签 list
    if "内容标签_list" not in df.columns:
        raise RuntimeError("CSV里没有列：内容标签_list")

    df["内容标签_list"] = df["内容标签_list"].apply(parse_list_cell)

    # 去掉没有年份的行（避免 groupby 出现 nan）
    df = df.dropna(subset=["发表年份"])
    df["发表年份"] = df["发表年份"].astype(int)

    records = []

    # 按年份统计
    for year, group in df.groupby("发表年份"):
        all_ngrams = []

        for tags in group["内容标签_list"]:
            if not tags:
                continue
            for n in range(1, MAX_N + 1):
                all_ngrams.extend([(n, g) for g in generate_ngrams(tags, n)])

        counter = Counter(all_ngrams)

        for (n, gram), freq in counter.items():
            records.append({
                "发表年份": int(year),
                "N": int(n),
                "N-gram": " ".join(gram),
                "频次": int(freq)
            })

    out_df = pd.DataFrame(records)
    out_df.sort_values(["N", "发表年份", "频次"], ascending=[True, True, False], inplace=True)

    out_df.to_excel(OUTPUT_XLSX, index=False)
    print(f"✅ N-gram 分析完成，已输出：{OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
