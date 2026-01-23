# 步骤三， 把步骤二生成的带有内容标签的 CSV 文件，按年份分 sheet 写入 XLSX 文件。

# csv_to_xlsx.py
#适用于包含大量文本内容的 CSV 文件，能有效避免 Excel 打开时因单元格过长而报错的问题。

import csv
import re
from pathlib import Path
from collections import defaultdict

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

# ========= CONFIG =========
CSV_PATH = "jjwxc_10yrs_withtags.csv"
OUT_XLSX = "jjwxc_10yrs_withtags_by_year.xlsx"

# 用哪一列来分 sheet（你 CSV 里是 sheet，通常是年份：2014/2015/...）
GROUP_COL = "sheet"

# Excel 单元格最大字符数（超过会报错/打不开）
EXCEL_CELL_LIMIT = 32767
TRUNC_SUFFIX = "...[TRUNCATED]"

# 如果你不想截断，改成 False（但可能写 xlsx 失败）
TRUNCATE_LONG_CELL = True


def sanitize_sheet_name(name: str) -> str:
    """Excel sheet 名称规则：不能含这些字符：[]:*?/\\ 且长度<=31"""
    if name is None:
        name = "UNKNOWN"
    name = str(name).strip() or "UNKNOWN"
    name = re.sub(r"[\[\]\:\*\?\/\\]", "_", name)
    name = name[:31].strip()
    return name or "UNKNOWN"


def clean_cell(v):
    """清理非法字符 + （可选）截断超长字符串"""
    if v is None:
        return None
    if isinstance(v, str):
        # 去掉 openpyxl 不允许的控制字符
        v = ILLEGAL_CHARACTERS_RE.sub("", v)

        # 可选：截断超长
        if TRUNCATE_LONG_CELL and len(v) > EXCEL_CELL_LIMIT:
            keep = EXCEL_CELL_LIMIT - len(TRUNC_SUFFIX)
            if keep < 0:
                keep = EXCEL_CELL_LIMIT
            v = v[:keep] + TRUNC_SUFFIX
        return v
    return v


def main():
    csv_path = Path(CSV_PATH)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    print(f"[INFO] Reading CSV (stream): {CSV_PATH}")

    # write_only=True：流式写入，适合 45k + 超长文本
    wb = Workbook(write_only=True)

    # 每个 group(年份)一个 worksheet
    ws_map = {}
    used_names = set()

    # 统计
    count_by_group = defaultdict(int)
    bad_rows = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise RuntimeError("CSV has no header row.")

        headers = list(reader.fieldnames)

        if GROUP_COL not in headers:
            raise RuntimeError(
                f"CSV missing column '{GROUP_COL}'. Available columns: {headers}"
            )

        for row_idx, row in enumerate(reader, start=2):  # start=2 because header is line 1
            try:
                group_val = row.get(GROUP_COL, "UNKNOWN")
                sheet_name = sanitize_sheet_name(group_val)

                # 避免同名冲突（很少见，但保险）
                base = sheet_name
                k = 1
                while sheet_name in used_names and sheet_name not in ws_map:
                    k += 1
                    sheet_name = sanitize_sheet_name(f"{base}_{k}")

                if sheet_name not in ws_map:
                    ws = wb.create_sheet(title=sheet_name)
                    ws.append(headers)  # 写 header
                    ws_map[sheet_name] = ws
                    used_names.add(sheet_name)

                ws = ws_map[sheet_name]

                # 按 header 顺序输出，保证列一致
                out_row = [clean_cell(row.get(h)) for h in headers]
                ws.append(out_row)

                count_by_group[sheet_name] += 1

                # 进度提示（不刷屏）
                if sum(count_by_group.values()) % 5000 == 0:
                    total = sum(count_by_group.values())
                    print(f"[INFO] Written rows: {total} (latest: {sheet_name})")

            except Exception:
                # 有些坏行（CSV断行/多列）在 DictReader 已经很少见，但留兜底
                bad_rows += 1
                continue

    print("[INFO] Saving XLSX...")
    wb.save(OUT_XLSX)

    total_written = sum(count_by_group.values())
    print(f"[DONE] Saved: {OUT_XLSX}")
    print(f"[DONE] Total rows written: {total_written}")
    if bad_rows:
        print(f"[WARN] Skipped bad rows: {bad_rows}")

    # 打印每个年份 sheet 写了多少行
    print("[INFO] Rows per sheet:")
    for k in sorted(count_by_group.keys()):
        print(f"  - {k}: {count_by_group[k]}")


if __name__ == "__main__":
    main()
