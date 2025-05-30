import pandas as pd
import pandas_gbq

def hello_http():
    """
    Cloud Function to read data from a Google Sheet,
    take only the first row, select specific columns,
    convert their types, and load into BigQuery.
    """
    message = 'Function executed Successfully'
    
    sheet_id = '1itUidavSQf8OjGtupTXIPpjRrrkMToAuwQBISqR2zoI'
    sheet_name = "data"
    url_1 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    
    print(f"Attempting to read data from URL: {url_1}")
    
    try:
        # Read Google Sheet data into Pandas dataframe
        df = pd.read_csv(url_1)
        
        # Take only the first row
        df = df.head(1)
        
        # --- NEW CHANGE HERE: Select ONLY the columns that exist in your BQ table schema ---
        # And ensure their data types match BigQuery's INTEGER.
        # We handle potential NaN values by converting them to 0 before casting to int.
        # Or, use pandas 'Int64' for nullable integer type.
        
        required_columns = ['InvoiceNo', 'Quantity']
        
        # Select only the required columns
        df = df[required_columns]
        
        # Convert to appropriate types for BigQuery INTEGER
        # Use pd.to_numeric with errors='coerce' to turn non-numeric into NaN,
        # then fill NaNs with 0 (or some other appropriate default) before converting to int.
        # Alternatively, use 'Int64' (capital I) for a nullable integer type in pandas.
        
        df['InvoiceNo'] = pd.to_numeric(df['InvoiceNo'], errors='coerce').fillna(0).astype(int)
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
        
        # --- End of NEW CHANGE ---

        print("\nDataFrame after selecting required columns and converting types:")
        print(df)
        print("\nDataFrame Data Types:")
        print(df.dtypes)
        print(f"\nSuccessfully processed {len(df)} row(s) from Google Sheet for BigQuery.")

        # BigQuery details
        project_id = 'deep-dive-459409'
        dataset_id = 'ads_data'
        table_id = 'ads_data_table' 
        full_table_name = f"{dataset_id}.{table_id}"
        
        print(f"Attempting to write data to BigQuery2 table: {project_id}.{full_table_name}")

        df.to_gbq(full_table_name,
                  project_id=project_id,
                  chunksize=100000, 
                  if_exists='append'
                 )
        print("Data loaded successfully into BigQuery.")
        message = 'Data loaded successfully into BigQuery!'

    except Exception as e:
        print(f"An error occurred during function execution: {e}")
        message = f'Error during execution: {e}'
        
    return message

def main():
    """
    Main function for local testing.
    """
    print("--- Starting local test of hello_http ---")
    response = hello_http()
    print(f"--- Function finished. Response: {response} ---")

if __name__ == '__main__':
    main()