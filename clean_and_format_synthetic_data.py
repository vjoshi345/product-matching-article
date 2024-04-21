import pandas as pd

file_dir = 'C:\WalmartLabs\EntityResolutionInSearch\synthetic_dataset'
link_file_path = 'C:\WalmartLabs\EntityResolutionInSearch\synthetic_dataset\product_link_data_A_B_iter2.txt'
file_path_a = 'C:\WalmartLabs\EntityResolutionInSearch\synthetic_dataset\product_data_A.txt'
file_path_b = 'C:\WalmartLabs\EntityResolutionInSearch\synthetic_dataset\product_data_B_iter2.txt'


# Data set of links between A and B
print('Reading linked dataset...')
data_list = []
with open(link_file_path, 'r') as f:
    for i, line in enumerate(f):
        if i > 0:
            row_list = line.strip().split('|')
            for j, col in enumerate(row_list):
                if len(data_list) < (j + 1):
                    data_list.append([])
                data_list[j].append(col.strip())

print(len(data_list))
for lst in data_list:
    print(len(lst), lst[0:5])

df_linked = pd.DataFrame({'Title_A': data_list[0], 'Product_Type_A': data_list[1], 'Brand_A': data_list[2],
                   'Price_A': data_list[3], 'Title_B': data_list[4], 'Product_Type_B': data_list[5],
                   'Brand_B': data_list[6], 'Price_B': data_list[7]})
print(df_linked.shape)
print('# of unique titles A:', df_linked['Title_A'].nunique())
print('# of unique titles B:', df_linked['Title_B'].nunique())
print('# of unique PTs A:', df_linked['Product_Type_A'].nunique())
print('# of unique PTs B:', df_linked['Product_Type_B'].nunique())
print('# of unique Brands A:', df_linked['Brand_A'].nunique())
print('# of unique Brands B:', df_linked['Brand_B'].nunique())

print()
print('*'*50)

# Dataset A
print('Reading product dataset A...')
data_list_a = []
with open(file_path_a, 'r') as f:
    for i, line in enumerate(f):
        if i > 0:
            row_list = line.strip().split('|')
            for j, col in enumerate(row_list):
                if len(data_list_a) < (j + 1):
                    data_list_a.append([])
                data_list_a[j].append(col.strip())

print(len(data_list_a))
for lst in data_list_a:
    print(len(lst), lst[0:5])

df_A = pd.DataFrame({'Title_A': data_list_a[0], 'Product_Type_A': data_list_a[1], 'Brand_A': data_list_a[2],
                     'Price_A': data_list_a[3]})
print(df_A.shape)
print('# of unique titles A:', df_A['Title_A'].nunique())
print('# of unique PTs A:', df_A['Product_Type_A'].nunique())
print('# of unique Brands A:', df_A['Brand_A'].nunique())

comb = list(df_linked['Title_A']) + list(df_A['Title_A'])
print(len(comb))
print(len(set(comb)))

print('*'*50)

# Dataset B
print('Reading product dataset B...')
data_list_b = []
with open(file_path_b, 'r') as f:
    for i, line in enumerate(f):
        if i > 0:
            row_list = line.strip().split('|')
            for j, col in enumerate(row_list):
                if len(data_list_b) < (j + 1):
                    data_list_b.append([])
                data_list_b[j].append(col.strip())

print(len(data_list_b))
for lst in data_list_b:
    print(len(lst), lst[0:5])

df_B = pd.DataFrame({'Title_B': data_list_b[0], 'Product_Type_B': data_list_b[1], 'Brand_B': data_list_b[2],
                     'Price_B': data_list_b[3]})
print(df_B.shape)
print('# of unique titles B:', df_B['Title_B'].nunique())
print('# of unique PTs B:', df_B['Product_Type_B'].nunique())
print('# of unique Brands B:', df_B['Brand_B'].nunique())

comb = list(df_linked['Title_B']) + list(df_B['Title_B'])
print('Length of title B + linked title B:', len(comb))
print('Length of set of title B + linked title B:', len(set(comb)))

comb = list(df_linked['Title_A']) + list(df_B['Title_B'])
print('Length of linked title A + title B:', len(comb))
print('Length of set of linked title A + title B:', len(set(comb)))

comb = list(df_A['Title_A']) + list(df_B['Title_B'])
print('Length of title A + title B:', len(comb))
print('Length of set of title A + title B:', len(set(comb)))

merged = pd.merge(df_B, df_linked, how='inner', left_on=['Title_B'], right_on=['Title_B'])
print(merged.shape)
print(merged[['Title_B']].head())

merged = pd.merge(df_B, df_linked, how='inner', left_on=['Title_B'], right_on=['Title_A'])
print(merged.shape)
print(merged[['Title_B_x']].head())

merged = pd.merge(df_B, df_A, how='inner', left_on=['Title_B'], right_on=['Title_A'])
print(merged.shape)
print(merged[['Title_B']].head())

# Add the linked products to the respective files
print()
print('*'*50)
print('Create a unique id column for each dataframe...')
na, nb, nl = len(df_A), len(df_B), len(df_linked)
df_A['unique_id_a'] = range(na)
df_B['unique_id_b'] = range(na, na+nb)
df_linked['unique_id_a'] = range(na+nb, na+nb+nl)
df_linked['unique_id_b'] = range(na+nb+nl, na+nb+nl+nl)

print('Adding the linked products to individual files...')
df_A = pd.concat([df_A, df_linked[['unique_id_a', 'Title_A', 'Product_Type_A', 'Brand_A', 'Price_A']]],
                 ignore_index=True)
print(df_A.shape)
df_B = pd.concat([df_B, df_linked[['unique_id_b', 'Title_B', 'Product_Type_B', 'Brand_B', 'Price_B']]], ignore_index=True)
print(df_B.shape)

# Convert price into a categorical variable
print()
print('*'*50)
print('Convert price to a categorical variable and create a new column...')
df_A = df_A.astype({'Price_A': float})
df_B = df_B.astype({'Price_B': float})

price_cat_dict = {0: '<100', 1: '100-200', 2: '200-300', 3: '300-400', 4: '400-500', 5: '500-600', 6: '600-700',
                  7: '700-800', 8: '800-900', 9: '900-1000', 10: '>=1000'}
price_cat_A = [price_cat_dict[min(pr//100, 10)] for pr in df_A['Price_A']]
price_cat_B = [price_cat_dict[min(pr//100, 10)] for pr in df_B['Price_B']]

df_A['Price_Cat'] = price_cat_A
df_B['Price_Cat'] = price_cat_B

print(df_A[['Price_A', 'Price_Cat']].head())
print(df_B[['Price_B', 'Price_Cat']].head())
print(df_A.groupby(['Price_Cat']).size())
print(df_B.groupby(['Price_Cat']).size())

# Save the cleaned datasets
print()
print('*'*50)
print('Saving the cleaned datasets...')
df_A.rename(columns={'unique_id_a': 'unique_id', 'Title_A': 'Title', 'Product_Type_A': 'Product_Type', 'Brand_A': 'Brand', 'Price_A': 'Price'},
            inplace=True)
df_B.rename(columns={'unique_id_b': 'unique_id', 'Title_B': 'Title', 'Product_Type_B': 'Product_Type', 'Brand_B': 'Brand', 'Price_B': 'Price'},
            inplace=True)
df_A.to_csv(file_dir+'\product_data_A_cleaned_w_price_cat.txt', sep='|', index=False)
df_B.to_csv(file_dir+'\product_data_B_cleaned_w_price_cat.txt', sep='|', index=False)
df_linked.to_csv(file_dir+'\product_link_data_A_B_cleaned.txt', sep='|', index=False)

# # Save the links in the required format
# print()
# print('*'*50)
# df_splink_format = df_linked[['unique_id_a', 'unique_id_b']].copy()
# df_splink_format.rename(columns={'unique_id_a': 'unique_id_l', 'unique_id_b': 'unique_id_r'}, inplace=True)
# df_splink_format['source_dataset_l'] = 'df_left'
# df_splink_format['source_dataset_r'] = 'df_right'
# df_splink_format['clerical_match_score'] = 1.0
#
# match_id_l, match_id_r = list(df_splink_format['unique_id_l']), list(df_splink_format['unique_id_r'])
# match_id = [(a, b) for a, b in zip(match_id_l, match_id_r)]
#
# df_cross = pd.merge(df_A[['unique_id']], df_B[['unique_id']], how='cross', suffixes=('_l', '_r'))
# df_cross['match_id'] = [(a, b) for a, b in zip(df_cross['unique_id_l'], df_cross['unique_id_r'])]
# df_cross = df_cross.loc[~df_cross['match_id'].isin(match_id), :]
# df_cross.drop(columns=['match_id'], inplace=True)
# df_cross['source_dataset_l'] = 'df_left'
# df_cross['source_dataset_r'] = 'df_right'
# df_cross['clerical_match_score'] = 0.0
# print(df_cross.shape)
# print(df_cross.dtypes)
# print(df_cross.head())
#
# df_splink_format_full = pd.concat([df_splink_format, df_cross])
# df_splink_format_sampled = pd.concat([df_splink_format, df_cross.sample(n=600, random_state=42, axis=0)])
#
# # Save the datasets
# df_splink_format.to_csv(file_dir+'\product_link_data_labels_pos.csv', index=False)
# df_splink_format_full.to_csv(file_dir+'\product_link_data_labels_full.csv', index=False)
# df_splink_format_sampled.to_csv(file_dir+'\product_link_data_labels_sampled.csv', index=False)
