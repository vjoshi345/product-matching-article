import pandas as pd
import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl
from splink.duckdb.linker import DuckDBLinker
from splink.duckdb.blocking_rule_library import block_on

link_file_path = 'data\product_link_data_A_B_cleaned.txt'
file_path_a = 'data\product_data_A_cleaned_w_price_cat.txt'
file_path_b = 'data\product_data_B_cleaned_w_price_cat.txt'
labels_file_path = 'data\product_link_data_labels_full.csv'

df_a = pd.read_csv(file_path_a, sep='|')
print(df_a.shape)
print(df_a.head())

df_b = pd.read_csv(file_path_b, sep='|')
print(df_b.shape)
print(df_b.head())

df_labels = pd.read_csv(labels_file_path)
print(df_labels.shape)
print(df_labels.dtypes)

settings = {
    "link_type": "link_only",
    "comparisons": [
        cl.exact_match("Price_Cat"),
        ctl.name_comparison("Title"),
        ctl.name_comparison("Product_Type"),
        ctl.name_comparison("Brand")
    ],
}
print(settings['comparisons'][0].human_readable_description)
linker = DuckDBLinker([df_a, df_b], settings, input_table_aliases=["df_left", "df_right"])
linker.completeness_chart(cols=["Title", "Product_Type", "Brand", "Price"])

deterministic_rules = [
    "levenshtein(l.Title, r.Title) <= 1",
]

linker.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)

linker.estimate_u_using_random_sampling(max_pairs=1e6, seed=1)

session_price = linker.estimate_parameters_using_expectation_maximisation(block_on("Price_Cat"))
session_title = linker.estimate_parameters_using_expectation_maximisation(block_on("Title"))
session_brand = linker.estimate_parameters_using_expectation_maximisation(block_on("Brand"))

# Use the FS model to predict links
results = linker.predict(threshold_match_probability=0.90)
out_df = results.as_pandas_dataframe()
out_df.to_csv('results\output_fs_w_price_th_090.txt', sep='|', index=False)

# Evaluation
labels_table = linker.register_labels_table(df_labels)
roc_table = linker.truth_space_table_from_labels_table(labels_table)
roc_table_df = roc_table.as_pandas_dataframe()
print(roc_table_df.shape)
roc_table_df.to_csv('results\\roc_table_w_price.csv', index=False)
