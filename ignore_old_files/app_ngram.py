import pandas as pd
import streamlit as st
import plotly.express as px
import ast

# ===================== 配置 =====================
NG_XLSX = "/Users/xin/Desktop/RA/jjwxc_ngram_analysis.xlsx"
RAW_XLSX = "/Users/xin/Desktop/RA/jjwxc_top100_data_cleaned.xlsx"
APP_TITLE = "JJWXC Content-Tag Ngram Viewer"

# ===================== Genre 词表 =====================
GENRE_MAIN = ["言情", "纯爱", "百合", "无cp", "多元"]
GENRE_ERA = ["近代现代", "古色古香", "架空历史", "幻想未来"]
GENRE_TYPE = [
    "爱情","武侠","奇幻","仙侠","游戏","传奇","科幻","童话","惊悚","悬疑","剧情","轻小说",
    "古典衍生","东方衍生","西方衍生","其他衍生","儿歌","散文","童谣","寓言","儿童小说"
]

# ===================== 你的标签词表（VALID_TAGS）=====================
VALID_TAGS = {
    "甜文","情有独钟","爽文","穿越时空","天作之合","强强","穿书","天之骄子","系统","成长","豪门世家","都市","日常","宫廷侯爵",
    "种田文","重生","仙侠修真","年代文","业界精英","破镜重圆","先婚后爱","升级流","娱乐圈","万人迷","快穿","灵异神怪","无限流",
    "幻想空间","校园","励志","基建","救赎","群像","沙雕","ABO","美食","欢喜冤家","年下","治愈","现代架空","末世","综漫",
    "追爱火葬场","星际","HE","团宠","青梅竹马","逆袭","异能","直播","暗恋","因缘邂逅","生子","萌宠","悬疑推理","美强惨",
    "高岭之花","囤货","相爱相杀","正剧","市井生活","少年漫","朝堂","西方罗曼","轻松","咒回","经营","打脸","历史衍生","柯南",
    "英美衍生","女强","荒野求生","惊悚","狗血","未来架空","女配","体育竞技","克苏鲁","抽奖抽卡","文野","三教九流","钓系","玄学",
    "近水楼台","迪化流","婚恋","单元文","马甲文","游戏网游","超级英雄","开挂","脑洞","花季雨季","布衣生活","科幻","萌娃","东方玄幻",
    "西幻","清穿","白月光","废土","爆笑","复仇虐渣","异世大陆","边缘恋歌","反套路","恋爱合约","古代幻想","日久生情","虫族","江湖",
    "论坛体","机甲","家教","鬼灭","女扮男装","魔幻","火影","科举","排球少年","乙女向","第四天灾","天选之子","阴差阳错","随身空间",
    "足球","网王","电竞","平步青云","日韩泰","龙傲天","忠犬","港风","前世今生","综艺","宅斗","赛博朋克","武侠","创业","热血","田园",
    "制服情缘","全息","规则怪谈","古穿今","失忆","腹黑","海贼王","真假少爷","御姐","权谋","宫斗","虐文","炮灰","宋穿","学霸","灵魂转换",
    "异想天开","读心术","都市异闻","咸鱼","师徒","乔装改扮","吐槽","异闻传说","时代奇缘","古典名著","剧透","姐弟恋","唐穿","史诗奇幻",
    "汉穿","哨向","男配","红楼梦","猎人","对照组","职场","时代新风","卡牌","赶山赶海","刀剑乱舞","多重人格","真假千金","现实","明穿","燃",
    "弹幕","西方名著","签到流","吐槽役","星穹铁道","秦穿","灵气复苏","总裁","位面","神话传说","转生","预知","女尊","原神","黑篮","神豪流",
    "三国穿越","高智商","犬夜叉","公路文","非遗","NPC","纸片人","少女漫","民国","大冒险","时尚圈","剑网3","群穿","毒舌","冰山","国风幻想",
    "模拟器","读档流","性别转换","FGO","傲娇","替身","烧脑","召唤流","商战","美娱","极品亲戚","吃货","封神","洪荒","开荒","奇谭","七五",
    "app","漫穿","JOJO","银魂","齐神","蓝锁","网红","暖男","萌","中二","聊斋","骑士与剑","血族","中世纪","亡灵异族","原始社会","恶役","御兽",
    "七年之痒","天降","盲盒","魔法少女","蒸汽朋克","锦鲤","扶贫","亚人","特摄","交换人生","魔王勇者","BE","死神","悲剧","红包群","网配","曲艺",
    "对话体","港台","SD","婆媳","圣斗士","绝区零"
}

# ===================== 页面基础设置 =====================
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.caption("提示：点击图例可隐藏/显示曲线；支持框选缩放与悬停查看数值。")

# ===================== 读取 Ngram 数据 =====================
@st.cache_data
def load_ngram_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)

    # 兼容列名
    rename_map = {}
    if "N-gram" in df.columns:
        rename_map["N-gram"] = "Ngram"
    if "发表年份" in df.columns:
        rename_map["发表年份"] = "Year"
    if "频次" in df.columns:
        rename_map["频次"] = "Freq"
    if "N" in df.columns:
        rename_map["N"] = "N"
    df = df.rename(columns=rename_map)

    required = {"Year", "N", "Ngram", "Freq"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"ngram表缺少列：{missing}。需要：Year/N/Ngram/Freq")

    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["N"] = pd.to_numeric(df["N"], errors="coerce").astype("Int64")
    df["Ngram"] = df["Ngram"].astype(str)
    df["Freq"] = pd.to_numeric(df["Freq"], errors="coerce")
    df = df.dropna(subset=["Year", "N", "Ngram", "Freq"]).copy()

    df["Year"] = df["Year"].astype(int)
    df["N"] = df["N"].astype(int)
    df["Freq"] = df["Freq"].astype(int)
    return df

# ===================== 读取 Raw 数据并构建 year->tags（方案C）=====================
@st.cache_data
def load_year_tags(raw_path: str) -> pd.DataFrame:
    df_raw = pd.read_excel(raw_path)

    df_raw["发表年份"] = pd.to_datetime(df_raw["发表时间"], errors="coerce").dt.year
    df_raw = df_raw.dropna(subset=["发表年份"]).copy()
    df_raw["发表年份"] = df_raw["发表年份"].astype(int)

    def parse_list(x):
        if isinstance(x, list):
            return [str(i).strip() for i in x if str(i).strip()]
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return []
            try:
                v = ast.literal_eval(s)
                if isinstance(v, list):
                    return [str(i).strip() for i in v if str(i).strip()]
            except Exception:
                pass
            return [i.strip() for i in s.split() if i.strip()]
        return []

    if "内容标签_list" not in df_raw.columns:
        raise ValueError("RAW 表缺少列：内容标签_list")

    df_raw["内容标签_list"] = df_raw["内容标签_list"].apply(parse_list)

    df_year_tags = (
        df_raw.groupby("发表年份")["内容标签_list"]
        .apply(lambda x: sorted(set(tag for tags in x for tag in tags)))
        .reset_index(name="标签列表")
    )
    df_year_tags["标签数量"] = df_year_tags["标签列表"].apply(len)
    df_year_tags = df_year_tags.sort_values("发表年份").reset_index(drop=True)
    return df_year_tags

# ===================== 构建标签生命周期表 ======================
@st.cache_data
def build_tag_lifecycle(raw_path: str) -> pd.DataFrame:
    """
    从 RAW_XLSX（逐书标签）构建每个 tag 的生命周期：
    - 首次出现年份
    - 最后出现年份
    - 出现年数（出现过的年份数量）
    - 覆盖跨度（last-first+1）
    """
    df_raw = pd.read_excel(raw_path)
    df_raw["Year"] = pd.to_datetime(df_raw["发表时间"], errors="coerce").dt.year
    df_raw = df_raw.dropna(subset=["Year"]).copy()
    df_raw["Year"] = df_raw["Year"].astype(int)

    def parse_list(x):
        if isinstance(x, list):
            return [str(i).strip() for i in x if str(i).strip()]
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return []
            try:
                v = ast.literal_eval(s)
                if isinstance(v, list):
                    return [str(i).strip() for i in v if str(i).strip()]
            except Exception:
                pass
            return [i.strip() for i in s.split() if i.strip()]
        return []

    df_raw["tags"] = df_raw["内容标签_list"].apply(parse_list)

    # 展开成 Year-tag 行
    exploded = df_raw[["Year", "tags"]].explode("tags").dropna()
    exploded = exploded.rename(columns={"tags": "Tag"})
    exploded["Tag"] = exploded["Tag"].astype(str).str.strip()
    exploded = exploded[exploded["Tag"] != ""]

    # 生命周期：first/last + 出现年份数
    life = (
        exploded.groupby("Tag")["Year"]
        .agg(首次出现年份="min", 最后出现年份="max", 出现年数="nunique")
        .reset_index()
    )
    life["覆盖跨度"] = life["最后出现年份"] - life["首次出现年份"] + 1
    life = life.sort_values(["首次出现年份", "最后出现年份", "Tag"]).reset_index(drop=True)

    return life


# ===================== 加载数据（失败就 stop）=====================
try:
    ngram_df = load_ngram_table(NG_XLSX)
except Exception as e:
    st.error(f"读取 Ngram 表失败：{repr(e)}")
    st.stop()

try:
    df_year_tags = load_year_tags(RAW_XLSX)
    tag_life_df = build_tag_lifecycle(RAW_XLSX)

except Exception as e:
    st.error(f"读取 Raw 表失败：{repr(e)}")
    st.stop()

years = sorted(ngram_df["Year"].unique().tolist())
tags_sorted = sorted(list(VALID_TAGS))



# ===================== Sidebar：词表 & 控件 =====================
with st.sidebar:
    st.header("控制面板")

    with st.expander("📚 查看全部 VALID_TAGS（可复制）", expanded=False):
        st.write(f"共 {len(tags_sorted)} 个标签")
        st.code("，".join(tags_sorted))

    n_choice = st.radio("选择 N", [1, 2], horizontal=True, index=0)

    # 过滤阈值：减少低频噪声
    min_total = st.slider("过滤：全时期总频次 ≥", 1, 50, 3)

    # Top-K
    show_topk = st.checkbox("显示 Top-K（按年份）", value=True)
    topk = st.slider("Top-K", 3, 30, 10) if show_topk else 10
    year_pick = st.selectbox("选择年份", years, index=len(years)-1) if show_topk else years[-1]

# ===================== Tabs =====================
tab1, tab2 = st.tabs(["📈 Ngram 趋势", "📅 每年有哪些标签"])

# ===================== Tab1：Ngram Viewer =====================
with tab1:
    st.subheader("🔎 选择要分析的标签（支持搜索/多选）")

    if n_choice == 1:
        default_tags = [t for t in ["甜文", "情有独钟", "都市"] if t in VALID_TAGS]
        selected = st.multiselect("选择 1-gram 标签", options=tags_sorted, default=default_tags)
    else:
        bi_totals = (
            ngram_df[ngram_df["N"] == 2]
            .groupby("Ngram")["Freq"].sum()
            .sort_values(ascending=False)
        )
        bi_options = bi_totals[bi_totals >= min_total].index.tolist()
        default_bi = [x for x in ["甜文 情有独钟", "快穿 系统"] if x in bi_options]
        selected = st.multiselect(
            "选择 2-gram 组合（来自数据，已按阈值筛选）",
            options=bi_options,
            default=default_bi
        )

    # 总频次阈值过滤
    totals = (
        ngram_df[ngram_df["N"] == n_choice]
        .groupby("Ngram")["Freq"].sum()
        .reset_index(name="Total")
    )
    valid = set(totals[totals["Total"] >= min_total]["Ngram"].tolist())

    sub = ngram_df[(ngram_df["N"] == n_choice) & (ngram_df["Ngram"].isin(valid))].copy()

    if selected:
        sub = sub[sub["Ngram"].isin(selected)].copy()
    else:
        st.info("请选择至少一个标签/组合来绘制趋势图。")

    st.subheader("📈 时间趋势（交互式）")

    if not sub.empty:
        sub = sub.sort_values(["Year", "Ngram"])
        fig = px.line(sub, x="Year", y="Freq", color="Ngram", markers=True)
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # 下载当前选择的数据
        csv_bytes = sub.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="⬇️ 下载当前曲线数据 CSV",
            data=csv_bytes,
            file_name=f"jjwxc_ngram_selected_N{n_choice}.csv",
            mime="text/csv"
        )
    else:
        if selected:
            st.warning("当前选择的标签在阈值过滤后没有数据。你可以降低“全时期总频次 ≥”或换其他标签。")

    if show_topk:
        st.subheader(f"🏆 {year_pick} 年 Top-{topk}（N={n_choice}）")
        one_year = ngram_df[(ngram_df["Year"] == year_pick) & (ngram_df["N"] == n_choice)].copy()
        one_year = one_year.sort_values("Freq", ascending=False).head(topk)

        fig2 = px.bar(one_year[::-1], x="Freq", y="Ngram", orientation="h")
        st.plotly_chart(fig2, use_container_width=True)
        st.dataframe(one_year.reset_index(drop=True), use_container_width=True)

# ===================== Tab2：每年有哪些标签（方案C）=====================
with tab2:
    st.subheader("📉 每一年标签数量变化（交互式）")
    fig_count = px.line(df_year_tags, x="发表年份", y="标签数量", markers=True)
    fig_count.update_layout(hovermode="x unified")
    st.plotly_chart(fig_count, use_container_width=True)

    st.subheader("📅 查看某一年有哪些内容标签")
    year_choice2 = st.selectbox(
        "选择年份",
        sorted(df_year_tags["发表年份"].tolist()),
        index=len(df_year_tags) - 1,
        key="year_choice_tags"
    )

    tags_that_year = df_year_tags.loc[
        df_year_tags["发表年份"] == year_choice2, "标签列表"
    ].iloc[0]

    st.write(f"{year_choice2} 年共有 {len(tags_that_year)} 个标签：")
    st.write("，".join(tags_that_year))

    with st.expander("📋 查看年度标签表（可下载）", expanded=False):
        st.dataframe(df_year_tags[["发表年份", "标签数量"]], use_container_width=True)

        csv_bytes2 = df_year_tags.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ 下载 df_year_tags CSV",
            data=csv_bytes2,
            file_name="jjwxc_year_tags.csv",
            mime="text/csv"
        )
        st.divider()
    st.subheader("🧬 标签生命周期：首次出现 vs 最后出现（创新/衰退）")

    # ---- 1) 查询某个 tag 的生命周期 ----
    all_tags_life = tag_life_df["Tag"].tolist()
    tag_query = st.selectbox(
        "选择一个标签查看生命周期（可搜索）",
        options=all_tags_life,
        index=all_tags_life.index("甜文") if "甜文" in all_tags_life else 0,
        key="tag_lifecycle_pick"
    )

    row = tag_life_df[tag_life_df["Tag"] == tag_query].iloc[0]
    st.write(
        f"**{tag_query}**：首次出现 **{row['首次出现年份']}**，最后出现 **{row['最后出现年份']}**，"
        f"出现过 **{row['出现年数']}** 个年份（跨度 **{row['覆盖跨度']}** 年）"
    )

    # ---- 2) 创新曲线：每年“首次出现”的 tag 数量 ----
    first_counts = (
        tag_life_df.groupby("首次出现年份")["Tag"].nunique()
        .reset_index(name="新出现标签数")
        .rename(columns={"首次出现年份": "Year"})
        .sort_values("Year")
    )

    fig_new = px.line(first_counts, x="Year", y="新出现标签数", markers=True,
                      title="每年新出现标签数量（Innovation Curve）")
    fig_new.update_layout(hovermode="x unified")
    st.plotly_chart(fig_new, use_container_width=True)

    # ---- 3) 衰退曲线：每年“最后出现”的 tag 数量 ----
    last_counts = (
        tag_life_df.groupby("最后出现年份")["Tag"].nunique()
        .reset_index(name="最后出现标签数")
        .rename(columns={"最后出现年份": "Year"})
        .sort_values("Year")
    )

    fig_end = px.line(last_counts, x="Year", y="最后出现标签数", markers=True,
                      title="每年最后出现标签数量（Decline Curve）")
    fig_end.update_layout(hovermode="x unified")
    st.plotly_chart(fig_end, use_container_width=True)

    # ---- 4) 查看某一年新出现/消失了哪些标签（可解释性很强） ----
    st.subheader("📌 某一年：新出现/消失的标签列表")

    year_focus = st.selectbox(
        "选择年份",
        sorted(df_year_tags["发表年份"].tolist()),
        index=len(df_year_tags) - 1,
        key="year_focus_innov_decline"
    )

    new_tags = tag_life_df[tag_life_df["首次出现年份"] == year_focus]["Tag"].tolist()
    end_tags = tag_life_df[tag_life_df["最后出现年份"] == year_focus]["Tag"].tolist()

    colA, colB = st.columns(2)
    with colA:
        st.write(f"🆕 {year_focus} 年首次出现（{len(new_tags)} 个）")
        st.write("，".join(new_tags) if new_tags else "无")
    with colB:
        st.write(f"🥀 {year_focus} 年最后出现（{len(end_tags)} 个）")
        st.write("，".join(end_tags) if end_tags else "无")

    # ---- 5) 下载生命周期表 ----
    with st.expander("⬇️ 下载标签生命周期表", expanded=False):
        st.dataframe(tag_life_df, use_container_width=True)
        csv_life = tag_life_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "下载 tag_lifecycle.csv",
            data=csv_life,
            file_name="tag_lifecycle.csv",
            mime="text/csv"
        )


# ===================== 页脚信息 =====================
st.caption(
    "说明：本工具为平台定制 Ngram Viewer（基于晋江内容标签）。"
    "Ngram 趋势基于标签-年份频次表（jjwxc_ngram_analysis.xlsx）；"
    "年度标签集合基于清洗后的原始小说数据（jjwxc_top100_data_cleaned.xlsx）动态计算。"
)



# import pandas as pd
# import streamlit as st
# import plotly.express as px
# import ast

# # ===================== 配置 =====================
# NG_XLSX = "/Users/xin/Desktop/RA/jjwxc_ngram_analysis.xlsx"
# RAW_XLSX = "/Users/xin/Desktop/RA/jjwxc_top100_data_cleaned.xlsx"

#   # 你的 Ngram 汇总表（列：发表年份, N, N-gram, 频次）
# APP_TITLE = "JJWXC Content-Tag Ngram Viewer"

# # ===================== 你的标签词表（VALID_TAGS）=====================
# VALID_TAGS = {
#     "甜文","情有独钟","爽文","穿越时空","天作之合","强强","穿书","天之骄子","系统","成长","豪门世家","都市","日常","宫廷侯爵",
#     "种田文","重生","仙侠修真","年代文","业界精英","破镜重圆","先婚后爱","升级流","娱乐圈","万人迷","快穿","灵异神怪","无限流",
#     "幻想空间","校园","励志","基建","救赎","群像","沙雕","ABO","美食","欢喜冤家","年下","治愈","现代架空","末世","综漫",
#     "追爱火葬场","星际","HE","团宠","青梅竹马","逆袭","异能","直播","暗恋","因缘邂逅","生子","萌宠","悬疑推理","美强惨",
#     "高岭之花","囤货","相爱相杀","正剧","市井生活","少年漫","朝堂","西方罗曼","轻松","咒回","经营","打脸","历史衍生","柯南",
#     "英美衍生","女强","荒野求生","惊悚","狗血","未来架空","女配","体育竞技","克苏鲁","抽奖抽卡","文野","三教九流","钓系","玄学",
#     "近水楼台","迪化流","婚恋","单元文","马甲文","游戏网游","超级英雄","开挂","脑洞","花季雨季","布衣生活","科幻","萌娃","东方玄幻",
#     "西幻","清穿","白月光","废土","爆笑","复仇虐渣","异世大陆","边缘恋歌","反套路","恋爱合约","古代幻想","日久生情","虫族","江湖",
#     "论坛体","机甲","家教","鬼灭","女扮男装","魔幻","火影","科举","排球少年","乙女向","第四天灾","天选之子","阴差阳错","随身空间",
#     "足球","网王","电竞","平步青云","日韩泰","龙傲天","忠犬","港风","前世今生","综艺","宅斗","赛博朋克","武侠","创业","热血","田园",
#     "制服情缘","全息","规则怪谈","古穿今","失忆","腹黑","海贼王","真假少爷","御姐","权谋","宫斗","虐文","炮灰","宋穿","学霸","灵魂转换",
#     "异想天开","读心术","都市异闻","咸鱼","师徒","乔装改扮","吐槽","异闻传说","时代奇缘","古典名著","剧透","姐弟恋","唐穿","史诗奇幻",
#     "汉穿","哨向","男配","红楼梦","猎人","对照组","职场","时代新风","卡牌","赶山赶海","刀剑乱舞","多重人格","真假千金","现实","明穿","燃",
#     "弹幕","西方名著","签到流","吐槽役","星穹铁道","秦穿","灵气复苏","总裁","位面","神话传说","转生","预知","女尊","原神","黑篮","神豪流",
#     "三国穿越","高智商","犬夜叉","公路文","非遗","NPC","纸片人","少女漫","民国","大冒险","时尚圈","剑网3","群穿","毒舌","冰山","国风幻想",
#     "模拟器","读档流","性别转换","FGO","傲娇","替身","烧脑","召唤流","商战","美娱","极品亲戚","吃货","封神","洪荒","开荒","奇谭","七五",
#     "app","漫穿","JOJO","银魂","齐神","蓝锁","网红","暖男","萌","中二","聊斋","骑士与剑","血族","中世纪","亡灵异族","原始社会","恶役","御兽",
#     "七年之痒","天降","盲盒","魔法少女","蒸汽朋克","锦鲤","扶贫","亚人","特摄","交换人生","魔王勇者","BE","死神","悲剧","红包群","网配","曲艺",
#     "对话体","港台","SD","婆媳","圣斗士","绝区零"
# }

# # ===================== 页面基础设置 =====================
# st.set_page_config(page_title=APP_TITLE, layout="wide")
# st.title(APP_TITLE)
# st.caption("提示：点击图例可隐藏/显示曲线；支持框选缩放与悬停查看数值。")

# # ===================== 读取 Ngram 数据 =====================
# @st.cache_data
# def load_ngram_table(path: str) -> pd.DataFrame:
#     df = pd.read_excel(path)
#     # 兼容列名
#     rename_map = {}
#     if "N-gram" in df.columns:
#         rename_map["N-gram"] = "Ngram"
#     if "发表年份" in df.columns:
#         rename_map["发表年份"] = "Year"
#     if "频次" in df.columns:
#         rename_map["频次"] = "Freq"
#     if "N" in df.columns:
#         rename_map["N"] = "N"
#     df = df.rename(columns=rename_map)

#     required = {"Year", "N", "Ngram", "Freq"}
#     missing = required - set(df.columns)
#     if missing:
#         raise ValueError(f"ngram表缺少列：{missing}。需要：Year/N/Ngram/Freq")

#     df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
#     df["N"] = pd.to_numeric(df["N"], errors="coerce").astype("Int64")
#     df["Ngram"] = df["Ngram"].astype(str)
#     df["Freq"] = pd.to_numeric(df["Freq"], errors="coerce")
#     df = df.dropna(subset=["Year", "N", "Ngram", "Freq"]).copy()
#     df["Year"] = df["Year"].astype(int)
#     df["N"] = df["N"].astype(int)
#     df["Freq"] = df["Freq"].astype(int)
#     return df

# try:
#     ngram_df = load_ngram_table(NG_XLSX)
# except Exception as e:
#     st.error(f"读取 {NG_XLSX} 失败：{repr(e)}")
#     st.stop()

# years = sorted(ngram_df["Year"].unique().tolist())
# tags_sorted = sorted(list(VALID_TAGS))

# # ===================== Sidebar：词表 & 控件 =====================
# with st.sidebar:
#     st.header("控制面板")

#     with st.expander("📚 查看全部 VALID_TAGS（可复制）", expanded=False):
#         st.write(f"共 {len(tags_sorted)} 个标签")
#         st.code("，".join(tags_sorted))

#     n_choice = st.radio("选择 N", [1, 2], horizontal=True, index=0)

#     # 过滤阈值：减少低频噪声
#     min_total = st.slider("过滤：全时期总频次 ≥", 1, 50, 3)

#     # Top-K
#     show_topk = st.checkbox("显示 Top-K（按年份）", value=True)
#     topk = st.slider("Top-K", 3, 30, 10) if show_topk else 10
#     year_pick = st.selectbox("选择年份", years, index=len(years)-1) if show_topk else years[-1]

# # ===================== 主页面：选择标签（最佳组合核心） =====================
# st.subheader("🔎 选择要分析的标签（支持搜索/多选）")

# if n_choice == 1:
#     # unigram：直接用 VALID_TAGS
#     default_tags = [t for t in ["甜文", "情有独钟", "都市"] if t in VALID_TAGS]
#     selected = st.multiselect("选择 1-gram 标签", options=tags_sorted, default=default_tags)
# else:
#     # bigram：从数据中抽所有 N=2 的组合（可能很多，建议用总频次阈值筛选）
#     bi_totals = (
#         ngram_df[ngram_df["N"] == 2]
#         .groupby("Ngram")["Freq"].sum()
#         .sort_values(ascending=False)
#     )
#     # 只显示达到阈值的 bigram，避免列表过大
#     bi_options = bi_totals[bi_totals >= min_total].index.tolist()
#     default_bi = [x for x in ["甜文 情有独钟", "快穿 系统"] if x in bi_options]
#     selected = st.multiselect("选择 2-gram 组合（来自数据，已按阈值筛选）", options=bi_options, default=default_bi)

# # ===================== 数据筛选（按阈值 + 选择项） =====================
# # 总频次阈值过滤
# totals = (
#     ngram_df[ngram_df["N"] == n_choice]
#     .groupby("Ngram")["Freq"].sum()
#     .reset_index(name="Total")
# )
# valid = set(totals[totals["Total"] >= min_total]["Ngram"].tolist())

# sub = ngram_df[(ngram_df["N"] == n_choice) & (ngram_df["Ngram"].isin(valid))].copy()

# if selected:
#     sub = sub[sub["Ngram"].isin(selected)].copy()
# else:
#     st.info("请选择至少一个标签/组合来绘制趋势图。")

# # ===================== 趋势图 =====================
# st.subheader("📈 时间趋势（交互式）")

# if not sub.empty:
#     sub = sub.sort_values(["Year", "Ngram"])
#     fig = px.line(
#         sub,
#         x="Year",
#         y="Freq",
#         color="Ngram",
#         markers=True
#     )
#     fig.update_layout(hovermode="x unified")
#     st.plotly_chart(fig, use_container_width=True)

#     # 下载当前选择的数据
#     csv_bytes = sub.to_csv(index=False).encode("utf-8-sig")
#     st.download_button(
#         label="⬇️ 下载当前曲线数据 CSV",
#         data=csv_bytes,
#         file_name=f"jjwxc_ngram_selected_N{n_choice}.csv",
#         mime="text/csv"
#     )
# else:
#     if selected:
#         st.warning("当前选择的标签在阈值过滤后没有数据。你可以降低“全时期总频次 ≥”或换其他标签。")

# # ===================== Top-K（按年份） =====================
# if show_topk:
#     st.subheader(f"🏆 {year_pick} 年 Top-{topk}（N={n_choice}）")
#     one_year = ngram_df[(ngram_df["Year"] == year_pick) & (ngram_df["N"] == n_choice)].copy()
#     one_year = one_year.sort_values("Freq", ascending=False).head(topk)

#     fig2 = px.bar(one_year[::-1], x="Freq", y="Ngram", orientation="h")
#     st.plotly_chart(fig2, use_container_width=True)
#     st.dataframe(one_year.reset_index(drop=True), use_container_width=True)

# # ===================== 页脚信息 =====================
# st.caption(
#     "说明：本工具为平台定制 Ngram Viewer（基于晋江内容标签），"
#     "使用受控词表 VALID_TAGS 保证语义一致性与可复现性。"
# )


# df_raw = pd.read_excel(RAW_XLSX)

# df_raw["发表年份"] = pd.to_datetime(
#     df_raw["发表时间"], errors="coerce"
# ).dt.year

# def parse_list(x):
#     if isinstance(x, list):
#         return x
#     if isinstance(x, str):
#         try:
#             return ast.literal_eval(x)
#         except:
#             return [i for i in x.split() if i]
#     return []

# df_raw["内容标签_list"] = df_raw["内容标签_list"].apply(parse_list)

# df_year_tags = (
#     df_raw.groupby("发表年份")["内容标签_list"]
#     .apply(lambda x: sorted(set(tag for tags in x for tag in tags)))
#     .reset_index(name="标签列表")
# )

# df_year_tags["标签数量"] = df_year_tags["标签列表"].apply(len)

# st.subheader("📅 查看某一年有哪些内容标签")

# year_choice = st.selectbox(
#     "选择年份",
#     sorted(df_year_tags["发表年份"].tolist()),
#     index=len(df_year_tags) - 1
# )

# tags_that_year = df_year_tags.loc[
#     df_year_tags["发表年份"] == year_choice, "标签列表"
# ].iloc[0]

# st.write(f"{year_choice} 年共有 {len(tags_that_year)} 个标签：")
# st.write("，".join(tags_that_year))
