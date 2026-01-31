import pandas as pd
import os

# 1. Path
input_file = 'datasets/BitcoinHeistData.csv'
output_file = 'datasets/mixed_btc_data.csv'

if not os.path.exists(input_file):
    print(f"❌ Error: {input_file} not found.")
else:
    print("🚀 Loading dataset (this might take a minute)...")
    df = pd.read_csv(input_file)

    # 2. Separate Classes
    white_df = df[df['label'] == 'white']
    ransom_df = df[df['label'] != 'white']

    # 3. Sample for Testing (50k each to avoid system hang)
    n_samples = 50000 
    print(f"Mixing {n_samples} samples from each class...")
    
    mixed_df = pd.concat([
        white_df.sample(n=min(n_samples, len(white_df))),
        ransom_df.sample(n=min(n_samples, len(ransom_df)))
    ])

    # 4. SHUFFLE - This is key!
    mixed_df = mixed_df.sample(frac=1).reset_index(drop=True)

    # 5. Save
    mixed_df.to_csv(output_file, index=False)
    print(f"✅ Success! Mixed file saved at: {output_file}")