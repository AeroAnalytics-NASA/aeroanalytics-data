import pandas as pd
import os
from datetime import datetime

def sample_csv():
    # Input file path
    input_file = 'tempo_no2_north_america.csv'
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found")
        return
    
    print(f"Reading {input_file}...")
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    print(f"Original dataset size: {len(df):,} rows")
    
    # Take random sample of 500,000 rows
    sampled_df = df.sample(n=500000, random_state=42)
    
    # Create output directory if it doesn't exist
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Save sampled data with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f'tempo_no2_sampled_{timestamp}.csv')
    sampled_df.to_csv(output_file, index=False)
    
    print(f"Sampled dataset saved to: {output_file}")
    print(f"Final dataset size: {len(sampled_df):,} rows")

if __name__ == "__main__":
    sample_csv()