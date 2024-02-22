# Function for extracting data via Socrata API
def extract_data(client, f_year, batch_size, offset):
    start_date = f"{f_year - 1}-07-01T00:00:00.000"
    end_date = f"{f_year}-07-01T00:00:00.000"
    results = client.get("m3tr-qhgy",
                         select=col_selected, 
                         where=f"(LOWER(name) LIKE '%hy-vee%' OR name LIKE '%WALL TO WALL WINE AND SPIRITS%') AND date >= '{start_date}' AND date < '{end_date}'", 
                         limit=batch_size, 
                         offset=offset)
    return results

# %%
# Function for transforming data 
def transform_data(df):
    # df = df.drop_duplicates()
    df.fillna(placeholders, inplace=True)
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    for col, col_type in num_col_dtype_map.items():
        if col_type == 'int':
            df[col] = df[col].astype(float).astype(int)
        else:
            df[col] = df[col].astype(float)
    df = df[(df['state_bottle_cost'] > 0) & (df['state_bottle_retail'] > 0) & (df['sale_bottles'] > 0)]
    return df

# %%
# Function for loading data to a MySQL database
def load_data(conn, cursor, df, batch_size, sql_insert_query):
    data_tuples = list(df.itertuples(index=False, name=None))
    for batch in [data_tuples[i:i + batch_size] for i in range(0, len(data_tuples), batch_size)]:
        for row in batch:
            try:
                cursor.execute(sql_insert_query, row)
            except pymysql.err.IntegrityError as e:
                if 'Duplicate entry' in str(e):
                    # Log the error and skip the duplicated row
                    print(f"Duplicate entry skipped: {row}")
                    continue
                else:
                    raise
        conn.commit()