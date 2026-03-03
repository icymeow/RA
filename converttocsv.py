import pandas as pd

input_file = "jjwxc_10yrs_withtags_by_year.xlsx"
output_file = "jjwxc_10yrs_withtags_by_year.csv"

df = pd.read_excel(input_file)
df.to_csv(output_file, index=False, encoding="utf-8-sig")

print("Done ✅")