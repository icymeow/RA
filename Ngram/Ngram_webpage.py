# Ngram_webpage.py
# 这个代码是生成 Streamlit 网页应用，用于可视化和分析来自 JJWXC 小说网站的 N-gram 数据，
# 包括内容标签（Tag）和小说类型（Genre）两个维度的 N-gram 趋势、生命周期以及各维度年度占比。

# 跑的时候在terminal里输入
# pip install streamlit plotly pandas openpyxl （如果按照requirements.txt弄好了就不用了）
# cd Ngram (移动到Ngram folder)
# streamlit run Ngram_webpage.py


import pandas as pd
import streamlit as st
import plotly.express as px

# ===================== 默认路径（本地用时改成自己的路径） =====================
DEFAULT_TAG_NG_XLSX = "/Users/xin/Desktop/RA/jjwxc_tag_ngram_analysis.xlsx"
DEFAULT_GENRE_NG_XLSX = "/Users/xin/Desktop/RA/jjwxc_genre_ngram_analysis.xlsx"

APP_TITLE = "JJWXC Ngram Viewer (Tags + Genre)"

# ===================== Genre 维度词表（你可自行增删） =====================
GENRE_DIMENSIONS = {
    "言情系统": ["言情", "纯爱", "百合", "无cp", "多元"],
    "时代": ["近代现代", "古色古香", "架空历史", "幻想未来"],
    "类型": [
        "爱情", "武侠", "奇幻", "仙侠", "游戏", "传奇", "科幻",
        "童话", "惊悚", "悬疑", "剧情", "轻小说",
        "古典衍生", "东方衍生", "西方衍生", "其他衍生",
        "儿歌", "散文", "童谣", "寓言", "儿童小说",
    ],
}

# ===================== 页面基础设置 =====================
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.caption("支持：Tag N-gram / Genre N-gram / 生命周期 / 各维度年度占比。")

# ===================== 工具：标准化列名 =====================
def normalize_ngram_df(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    """
    统一成列：Year, N, Ngram, Freq
    kind: 'tag' or 'genre'
    """
    df = df.copy()

    # 可能的列名映射
    rename_map = {
        "发表年份": "Year",
        "year": "Year",
        "年份": "Year",
        "N-gram": "Ngram",
        "Ngram": "Ngram",
        "ngram": "Ngram",
        "频次": "Freq",
        "freq": "Freq",
        "次数": "Freq",
        "N": "N",
        "n": "N",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    required = {"Year", "N", "Ngram", "Freq"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{kind} ngram 表缺少列：{missing}；当前列：{list(df.columns)}")

    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["N"] = pd.to_numeric(df["N"], errors="coerce")
    df["Freq"] = pd.to_numeric(df["Freq"], errors="coerce")
    df["Ngram"] = df["Ngram"].astype(str)

    df = df.dropna(subset=["Year", "N", "Ngram", "Freq"]).copy()
    df["Year"] = df["Year"].astype(int)
    df["N"] = df["N"].astype(int)
    df["Freq"] = df["Freq"].astype(int)

    # 去掉空字符串 ngram
    df["Ngram"] = df["Ngram"].str.strip()
    df = df[df["Ngram"] != ""].copy()

    return df


@st.cache_data
def load_xlsx(path: str, kind: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    return normalize_ngram_df(df, kind=kind)


# ===================== Sidebar：文件路径设置 =====================
with st.sidebar:
    st.header("数据源")
    tag_path = st.text_input("Tag Ngram XLSX 路径", value=DEFAULT_TAG_NG_XLSX)
    genre_path = st.text_input("Genre Ngram XLSX 路径", value=DEFAULT_GENRE_NG_XLSX)
    st.divider()

    st.header("通用过滤")
    min_total = st.slider("过滤：全时期总频次 ≥", 1, 200, 5)
    show_topk = st.checkbox("显示 Top-K（按年份）", value=True)
    topk = st.slider("Top-K", 3, 50, 15) if show_topk else 15


# ===================== 读取数据 =====================
try:
    tag_df = load_xlsx(tag_path, kind="tag")
except Exception as e:
    st.error(f"读取 Tag Ngram 表失败：{repr(e)}")
    st.stop()

try:
    genre_df = load_xlsx(genre_path, kind="genre")
except Exception as e:
    st.error(f"读取 Genre Ngram 表失败：{repr(e)}")
    st.stop()


# ===================== 辅助：构建可选项 =====================
def build_ngram_options(df: pd.DataFrame, n_choice: int, min_total: int):
    totals = df[df["N"] == n_choice].groupby("Ngram")["Freq"].sum().sort_values(ascending=False)
    opts = totals[totals >= min_total].index.tolist()
    return opts, totals


def plot_trend(df: pd.DataFrame, selected: list[str], n_choice: int, title: str):
    sub = df[df["N"] == n_choice].copy()
    if selected:
        sub = sub[sub["Ngram"].isin(selected)].copy()
    if sub.empty:
        st.warning("当前选择在过滤后没有数据（或未选择任何项）。")
        return

    sub = sub.sort_values(["Year", "Ngram"])
    fig = px.line(sub, x="Year", y="Freq", color="Ngram", markers=True, title=title)
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(
        "⬇️ 下载当前曲线数据 CSV",
        data=sub.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"{title.replace(' ', '_')}_N{n_choice}.csv",
        mime="text/csv",
    )


def plot_topk(df: pd.DataFrame, year_pick: int, n_choice: int, topk: int, title: str):
    one_year = df[(df["Year"] == year_pick) & (df["N"] == n_choice)].copy()
    if one_year.empty:
        st.info("该年份该 N 没有数据。")
        return
    one_year = one_year.sort_values("Freq", ascending=False).head(topk)
    fig = px.bar(one_year[::-1], x="Freq", y="Ngram", orientation="h", title=f"{year_pick} Top-{topk}（N={n_choice}）")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(one_year.reset_index(drop=True), use_container_width=True)


# ===================== Tabs =====================
tab1, tab2, tab3, tab4 = st.tabs(
    ["① 内容标签 Ngram 趋势", "② 标签：每年标签 + 生命周期", "③ Genre Ngram 趋势", "④ Genre 年度频次/百分比"]
)

# ===================== Tab1：Tag Ngram Trend =====================
with tab1:
    st.subheader("① 内容标签 N-gram 趋势")

    years = sorted(tag_df["Year"].unique().tolist())
    colA, colB, colC = st.columns([1, 1, 1])

    with colA:
        n_choice = st.radio("选择 N", [1, 2], horizontal=True, index=0, key="tag_n_choice")

    options, totals = build_ngram_options(tag_df, n_choice=n_choice, min_total=min_total)

    with colB:
        if n_choice == 1:
            default_sel = [x for x in ["甜文", "正剧", "轻松"] if x in options]
        else:
            # bigram 默认给空，避免太大；你可改成常见组合
            default_sel = options[:3] if len(options) >= 3 else options
        selected = st.multiselect("选择要画的 Tag N-gram（已按阈值过滤）", options=options, default=default_sel)

    with colC:
        year_pick = st.selectbox("Top-K 年份", years, index=len(years) - 1, key="tag_topk_year")

    plot_trend(tag_df, selected=selected, n_choice=n_choice, title="Tag Ngram Trend")

    if show_topk:
        st.divider()
        plot_topk(tag_df, year_pick=year_pick, n_choice=n_choice, topk=topk, title="Tag TopK")

    with st.expander("查看阈值过滤后的候选列表（按总频次排序）", expanded=False):
        st.write(f"候选数量：{len(options)}（总 Ngram 数：{len(totals)}）")
        st.dataframe(
            totals.reset_index().rename(columns={"index": "Ngram", "Freq": "Total"}).head(200),
            use_container_width=True
        )

# ===================== Tab2：Tag Year Set + Lifecycle =====================
with tab2:
    st.subheader("② 标签：每年有哪些标签 + 生命周期（首次/最后出现）")
    st.caption("这一页直接用 Tag Ngram 表构建：不需要 raw 表。建议基于 N=1（单标签）分析。")

    tag_uni = tag_df[tag_df["N"] == 1].copy()

    # 每年有哪些标签：取当年出现过（Freq>0）的 tag 集合
    year_tags = (
        tag_uni[tag_uni["Freq"] > 0]
        .groupby("Year")["Ngram"]
        .apply(lambda s: sorted(set(s.tolist())))
        .reset_index(name="标签列表")
        .sort_values("Year")
        .reset_index(drop=True)
    )
    year_tags["标签数量"] = year_tags["标签列表"].apply(len)

    st.subheader("每一年标签数量变化")
    fig_cnt = px.line(year_tags, x="Year", y="标签数量", markers=True, title="每年出现过的单标签数量（N=1）")
    fig_cnt.update_layout(hovermode="x unified")
    st.plotly_chart(fig_cnt, use_container_width=True)

    y_pick = st.selectbox("选择年份查看标签列表", year_tags["Year"].tolist(), index=len(year_tags) - 1, key="tag_year_list")
    tags_that_year = year_tags.loc[year_tags["Year"] == y_pick, "标签列表"].iloc[0]
    st.write(f"**{y_pick} 年**出现过 {len(tags_that_year)} 个单标签：")
    st.write("，".join(tags_that_year))

    st.divider()

    # 生命周期（N=1）
    life = (
        tag_uni[tag_uni["Freq"] > 0]
        .groupby("Ngram")["Year"]
        .agg(首次出现年份="min", 最后出现年份="max", 出现年数="nunique")
        .reset_index()
        .rename(columns={"Ngram": "Tag"})
    )
    life["覆盖跨度"] = life["最后出现年份"] - life["首次出现年份"] + 1
    life = life.sort_values(["首次出现年份", "最后出现年份", "Tag"]).reset_index(drop=True)

    st.subheader("标签生命周期查询")
    tag_pick = st.selectbox("选择一个标签", life["Tag"].tolist(), index=0, key="tag_life_pick")
    r = life[life["Tag"] == tag_pick].iloc[0]
    st.write(
        f"**{tag_pick}**：首次出现 **{int(r['首次出现年份'])}**，最后出现 **{int(r['最后出现年份'])}**，"
        f"出现过 **{int(r['出现年数'])}** 个年份（跨度 **{int(r['覆盖跨度'])}** 年）"
    )

    col1, col2 = st.columns(2)

    with col1:
        first_counts = (
            life.groupby("首次出现年份")["Tag"].nunique()
            .reset_index(name="新出现标签数")
            .rename(columns={"首次出现年份": "Year"})
            .sort_values("Year")
        )
        fig_new = px.line(first_counts, x="Year", y="新出现标签数", markers=True, title="每年新出现标签数量")
        fig_new.update_layout(hovermode="x unified")
        st.plotly_chart(fig_new, use_container_width=True)

    with col2:
        last_counts = (
            life.groupby("最后出现年份")["Tag"].nunique()
            .reset_index(name="最后出现标签数")
            .rename(columns={"最后出现年份": "Year"})
            .sort_values("Year")
        )
        fig_end = px.line(last_counts, x="Year", y="最后出现标签数", markers=True, title="每年最后出现标签数量")
        fig_end.update_layout(hovermode="x unified")
        st.plotly_chart(fig_end, use_container_width=True)

    st.subheader("某一年：新出现 / 消失 的标签列表")
    y_focus = st.selectbox("选择年份", sorted(year_tags["Year"].tolist()), index=len(year_tags) - 1, key="tag_new_end_year")
    new_tags = life[life["首次出现年份"] == y_focus]["Tag"].tolist()
    end_tags = life[life["最后出现年份"] == y_focus]["Tag"].tolist()

    cA, cB = st.columns(2)
    with cA:
        st.write(f"🆕 {y_focus} 首次出现（{len(new_tags)}）")
        st.write("，".join(new_tags) if new_tags else "无")
    with cB:
        st.write(f"🥀 {y_focus} 最后出现（{len(end_tags)}）")
        st.write("，".join(end_tags) if end_tags else "无")

    with st.expander("下载：year_tags / lifecycle", expanded=False):
        st.download_button(
            "⬇️ 下载 year_tags.csv",
            data=year_tags.to_csv(index=False).encode("utf-8-sig"),
            file_name="tag_year_tags.csv",
            mime="text/csv",
        )
        st.download_button(
            "⬇️ 下载 tag_lifecycle.csv",
            data=life.to_csv(index=False).encode("utf-8-sig"),
            file_name="tag_lifecycle.csv",
            mime="text/csv",
        )
        st.dataframe(life, use_container_width=True)

# ===================== Tab3：Genre Ngram Trend =====================
with tab3:
    st.subheader("③ Genre N-gram 趋势（genre 字段拆词后 N-gram）")
    st.caption("这里的 Ngram 来自 genre 拆分后的 token，例如：原创 / 纯爱 / 近代现代 / 爱情 / 女主 / 双视角 ...")

    years_g = sorted(genre_df["Year"].unique().tolist())
    colA, colB, colC = st.columns([1, 1, 1])

    with colA:
        n_choice_g = st.radio("选择 N", [1, 2], horizontal=True, index=0, key="genre_n_choice")

    options_g, totals_g = build_ngram_options(genre_df, n_choice=n_choice_g, min_total=min_total)

    with colB:
        if n_choice_g == 1:
            default_sel_g = [x for x in ["言情", "纯爱", "近代现代", "爱情"] if x in options_g]
        else:
            default_sel_g = options_g[:5] if len(options_g) >= 5 else options_g
        selected_g = st.multiselect("选择要画的 Genre N-gram（已按阈值过滤）", options=options_g, default=default_sel_g)

    with colC:
        year_pick_g = st.selectbox("Top-K 年份", years_g, index=len(years_g) - 1, key="genre_topk_year")

    plot_trend(genre_df, selected=selected_g, n_choice=n_choice_g, title="Genre Ngram Trend")

    if show_topk:
        st.divider()
        plot_topk(genre_df, year_pick=year_pick_g, n_choice=n_choice_g, topk=topk, title="Genre TopK")

    with st.expander("查看阈值过滤后的候选列表（按总频次排序）", expanded=False):
        st.write(f"候选数量：{len(options_g)}（总 Ngram 数：{len(totals_g)}）")
        st.dataframe(
            totals_g.reset_index().rename(columns={"index": "Ngram", "Freq": "Total"}).head(200),
            use_container_width=True
        )

# ===================== Tab4：Genre 年度频次/百分比 =====================
with tab4:
    st.subheader("④ 不同 genre 维度在不同年份的频次 / 百分比")
    st.caption("基于 Genre unigram（N=1）统计：每本书在“言情系统/时代/类型”各只出现一次，因此频次≈当年该类别书的数量。")

    g1 = genre_df[genre_df["N"] == 1].copy()

    dim_name = st.selectbox("选择要看的维度", list(GENRE_DIMENSIONS.keys()), index=0)
    dim_tokens = GENRE_DIMENSIONS[dim_name]

    # 只保留该维度的 token
    dim_df = g1[g1["Ngram"].isin(dim_tokens)].copy()
    if dim_df.empty:
        st.warning("该维度在你的 genre unigram 数据里没有匹配到 token。请检查 GENRE_DIMENSIONS 词表是否拼写一致。")
        st.stop()

    # pivot：Year x token -> freq
    pivot = (
        dim_df.pivot_table(index="Year", columns="Ngram", values="Freq", aggfunc="sum", fill_value=0)
        .reindex(sorted(dim_df["Year"].unique()))
    )
    # 确保列顺序按词表顺序
    pivot = pivot.reindex(columns=[t for t in dim_tokens if t in pivot.columns])

    freq_long = pivot.reset_index().melt(id_vars="Year", var_name="类别", value_name="频次")
    freq_long = freq_long.dropna(subset=["类别"])

    # 百分比：按年归一化
    row_sum = pivot.sum(axis=1).replace(0, pd.NA)
    pct = pivot.div(row_sum, axis=0) * 100
    pct_long = pct.reset_index().melt(id_vars="Year", var_name="类别", value_name="百分比")
    pct_long = pct_long.dropna(subset=["类别"])

    st.subheader(f"{dim_name}：年度频次（count）")
    fig_f = px.area(freq_long, x="Year", y="频次", color="类别", title=f"{dim_name} 年度频次（堆叠面积）")
    fig_f.update_layout(hovermode="x unified")
    st.plotly_chart(fig_f, use_container_width=True)

    st.subheader(f"{dim_name}：年度百分比（%）")
    fig_p = px.line(pct_long, x="Year", y="百分比", color="类别", markers=True, title=f"{dim_name} 年度占比（%）")
    fig_p.update_layout(hovermode="x unified")
    st.plotly_chart(fig_p, use_container_width=True)

    with st.expander("查看数据表（可下载）", expanded=False):
        st.write("频次表（Year x 类别）：")
        st.dataframe(pivot.reset_index(), use_container_width=True)

        st.download_button(
            "⬇️ 下载频次表 CSV",
            data=pivot.reset_index().to_csv(index=False).encode("utf-8-sig"),
            file_name=f"genre_{dim_name}_freq.csv",
            mime="text/csv",
        )

        st.download_button(
            "⬇️ 下载百分比表 CSV",
            data=pct.reset_index().to_csv(index=False).encode("utf-8-sig"),
            file_name=f"genre_{dim_name}_pct.csv",
            mime="text/csv",
        )

st.caption("说明：Tag 生命周期/年度标签基于 Tag Ngram(N=1) 直接推断；Genre 占比基于 Genre unigram(N=1) 统计。")
