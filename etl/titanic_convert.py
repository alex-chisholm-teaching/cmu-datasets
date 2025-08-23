import pandas as pd
import os

output_dir = 'clean/titanic'
os.makedirs(output_dir, exist_ok=True)

df = pd.read_csv('clean/titanic.csv')

# 1. CSV Variations (pandas.DataFrame.to_csv)

df.to_csv(f'{output_dir}/titanic.csv', index=False)  # Standard comma
df.to_csv(f'{output_dir}/titanic_semicolon.csv', sep=';', index=False)  # European
df.to_csv(f'{output_dir}/titanic_pipe.csv', sep='|', index=False)  # Pipe-delimited

# 2. TSV (pandas.DataFrame.to_csv with tab separator)
df.to_csv(f'{output_dir}/titanic.tsv', sep='\t', index=False)

# 3. JSON Formats (pandas.DataFrame.to_json)

df.to_json(f'{output_dir}/titanic_records.json', orient='records', indent=2)
df.to_json(f'{output_dir}/titanic_index.json', orient='index', indent=2)
df.to_json(f'{output_dir}/titanic_values.json', orient='values', indent=2)
df.to_json(f'{output_dir}/titanic_split.json', orient='split', indent=2)
df.to_json(f'{output_dir}/titanic_table.json', orient='table', indent=2)

# 4. Parquet (pandas.DataFrame.to_parquet)

try:
    df.to_parquet(f'{output_dir}/titanic.parquet', index=False)
    print("success")
except ImportError:
    print("failure")

# 5. Feather (pandas.DataFrame.to_feather)
try:
    df.to_feather(f'{output_dir}/titanic.feather')
    print("success")
except ImportError:
    print("failure")

# 6. Pickle (pandas.DataFrame.to_pickle)

df.to_pickle(f'{output_dir}/titanic.pkl')


# 7. Excel (pandas.DataFrame.to_excel)
try:
    # Single sheet
    df.to_excel(f'{output_dir}/titanic.xlsx', index=False, sheet_name='Passengers')
    
    # Multiple sheets
    with pd.ExcelWriter(f'{output_dir}/titanic_multi.xlsx') as writer:
        df.to_excel(writer, sheet_name='All_Passengers', index=False)
        survivors = df[df['survived'] == 1]
        non_survivors = df[df['survived'] == 0]
        survivors.to_excel(writer, sheet_name='Survivors', index=False)
        non_survivors.to_excel(writer, sheet_name='Non_Survivors', index=False)
    
    print("success")
except ImportError:
    print("failure")


files_in_output = []
if os.path.exists(output_dir):
    for file in sorted(os.listdir(output_dir)):
        file_path = os.path.join(output_dir, file)
        if os.path.isfile(file_path):
            size = os.path.getsize(file_path)
            files_in_output.append((file, size))

print(f"{'Filename':<25} {'Size (bytes)':<12} {'Size (KB)':<10} {'Format Type'}")
print("-" * 65)

for filename, size in files_in_output:
    size_kb = size / 1024
    
    # Determine format type
    if filename.endswith('.csv') or filename.endswith('.tsv'):
        format_type = "Text"
    elif filename.endswith('.json'):
        format_type = "Text"
    else:
        format_type = "Binary"
    
    print(f"{filename:<25} {size:<12,} {size_kb:<10.1f} {format_type}")

# READING IN 

# Test text format
csv_test = pd.read_csv(f'{output_dir}/titanic.csv')
print(f"CSV read successfully: {csv_test.shape}")

# Test JSON format
json_test = pd.read_json(f'{output_dir}/titanic_records.json')
print(f"JSON read successfully: {json_test.shape}")

# Test binary format (pickle always works)
pickle_test = pd.read_pickle(f'{output_dir}/titanic.pkl')
print(f"Pickle read successfully: {pickle_test.shape}")

# Verify data integrity
print(f"All formats contain same data: {df.equals(csv_test) and df.equals(json_test) and df.equals(pickle_test)}")

print("\n" + "="*70)
print("FORMAT CHARACTERISTICS")
print("="*70)

characteristics = '''
TEXT FORMATS (Human Readable):
• CSV/TSV: Universal, Excel-compatible, largest size
• JSON: Web APIs, structured data, very readable

BINARY FORMATS (Computer Optimized):  
• Parquet: Analytics, columnar storage, efficient compression
• Feather: Fast pandas I/O, temporary files
• Pickle: Python objects, preserves all data types
• Excel: Business reports, formatted output
• HDF5: Scientific data, hierarchical structure
• ORC: Big data analytics, Hadoop ecosystem

PANDAS ADVANTAGES:
✓ Consistent API across all formats
✓ Automatic data type inference
✓ Built-in compression options
✓ Memory-efficient reading
'''

print(characteristics)

print(f"\n✓ All files created in {output_dir}/")
print("✓ Students can now compare formats and understand trade-offs")
