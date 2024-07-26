import pandas as pd


def cost_preprocess():
    # Define the path to your CSV file
    csv_file_path = 'Datasets/Original_Cost.csv'
    output_csv_path = 'Datasets/preprocessed_cost.csv'

    # Read the CSV file
    df = pd.read_csv(csv_file_path)

    # Select the relevant columns
    df = df[['SettlementDate', 'SettlementPeriod', 'Price']]

    # Remove duplicate rows if necessary
    df = df.drop_duplicates()

    # Save the preprocessed data to a new CSV file
    df.to_csv(output_csv_path, index=False)

    print(f"Filtered CSV file saved to {output_csv_path}")


def generation_preprocess():
    # Define the path to your CSV file
    csv_file_path = 'Datasets/Original_Generation.csv'
    output_csv_path = 'Datasets/preprocessed_generation.csv'

    # Read the CSV file
    df = pd.read_csv(csv_file_path)

    # Select the relevant columns
    df = df[['SettlementDate', 'SettlementPeriod', 'FuelType', 'Generation']]

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Convert 'SettlementDate' to datetime format
    df['SettlementDate'] = pd.to_datetime(df['SettlementDate'])

    # Define the cutoff dates
    start_date = pd.Timestamp('2016-01-06')
    cutoff_date = pd.Timestamp('2024-04-01')

    # Filter out rows within the date range
    filtered_df = df[(df['SettlementDate'] > start_date) & (df['SettlementDate'] < cutoff_date)]

    # Reset index before grouping to avoid the warning and potential future issues
    filtered_df.reset_index(drop=True, inplace=True)

    # Group and sum the Generation
    result = filtered_df.groupby(['SettlementDate', 'SettlementPeriod'], as_index=False)['Generation'].sum()

    # Save the result to a new CSV file
    result.to_csv(output_csv_path, index=False)

    print(f"Filtered CSV file saved to {output_csv_path}")


def demand_preprocess():
    # Define the path to your CSV file
    csv_file_path = 'Datasets/Original_Demand.csv'
    output_csv_path = 'Datasets/preprocessed_demand.csv'

    # Read the CSV file
    df = pd.read_csv(csv_file_path)

    # Select and rename columns
    df = df[['SETTLEMENT_DATE', 'SETTLEMENT_PERIOD', 'TSD']]
    df.columns = ['SettlementDate', 'SettlementPeriod', 'Demand']

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Convert 'SettlementDate' to datetime format
    df['SettlementDate'] = pd.to_datetime(df['SettlementDate'])

    # Define the cutoff dates
    start_date = pd.Timestamp('2016-01-06')
    cutoff_date = pd.Timestamp('2024-04-01')

    # Filter out rows within the date range
    filtered_df = df[(df['SettlementDate'] > start_date) & (df['SettlementDate'] < cutoff_date)]

    # Save the preprocessed data to a new CSV file
    filtered_df.to_csv(output_csv_path, index=False)

    print(f"Filtered CSV file saved to {output_csv_path}")


def generate_full_settlement_periods(df):
    # Ensure 'SettlementDate' is in datetime format
    df['SettlementDate'] = pd.to_datetime(df['SettlementDate'])

    # Sort the DataFrame by 'SettlementDate' and 'SettlementPeriod' in descending order
    df_sorted = df.sort_values(by=['SettlementDate', 'SettlementPeriod'], ascending=[True, False])

    # Drop duplicates to keep only the latest 'SettlementPeriod' for each 'SettlementDate'
    df_latest_period = df_sorted.drop_duplicates(subset=['SettlementDate'], keep='first')

    # Generate the full range of SettlementPeriods
    all_periods = df_latest_period.apply(lambda row: pd.DataFrame({
        'SettlementDate': [row['SettlementDate']] * row['SettlementPeriod'],
        'SettlementPeriod': range(1, row['SettlementPeriod'] + 1)
    }), axis=1).values.tolist()

    # Flatten the list of DataFrames into a single DataFrame
    df_complete_calendar = pd.concat(all_periods)

    return df_complete_calendar


def interpolation(df):
    # Sort the DataFrame by 'SettlementDate' and 'SettlementPeriod' for interpolation
    df.sort_values(by=['SettlementDate', 'SettlementPeriod'], inplace=True)

    # Convert columns to numeric where possible
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = pd.to_numeric(df[col], errors='ignore')

    # Interpolate missing values
    df.interpolate(method='linear', inplace=True)

    return df


def merge_generation_demand():
    # Define paths
    generation_csv_path = 'Datasets/preprocessed_generation.csv'
    demand_csv_path = 'Datasets/preprocessed_demand.csv'
    merged_csv_path = 'Datasets/merged_generation_demand.csv'

    # Read the CSV files
    df_generation = pd.read_csv(generation_csv_path)
    df_demand = pd.read_csv(demand_csv_path)

    # Convert 'SettlementDate' to datetime if not already
    df_generation['SettlementDate'] = pd.to_datetime(df_generation['SettlementDate'])
    df_demand['SettlementDate'] = pd.to_datetime(df_demand['SettlementDate'])

    # Merge the DataFrames on 'SettlementDate' and 'SettlementPeriod' using a full outer join
    merged_df = pd.merge(df_generation, df_demand, on=['SettlementDate', 'SettlementPeriod'], how='outer')
    df_complete = generate_full_settlement_periods(merged_df)
    df_final = pd.merge(df_complete, merged_df, on=['SettlementDate', 'SettlementPeriod'], how='outer')

    df_final = interpolation(df_final)

    # Save the final merged DataFrame to a CSV file
    df_final.to_csv(merged_csv_path, index=False)


def preprocess():
    demand_preprocess()
    generation_preprocess()
    cost_preprocess()


if __name__ == "__main__":
    preprocess()
    merge_generation_demand()
