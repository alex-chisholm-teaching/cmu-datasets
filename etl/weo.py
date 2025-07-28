import pandas as pd
import janitor


raw = pd.read_csv('data/weo-2025-04-full.xls', sep='\t', encoding='utf-16-le')
raw = raw.clean_names() # improve the column names

# remove projection columns. 2025 and above.

years_to_drop = ['2025', '2026', '2027', '2028', '2029', '2030']
raw = raw.drop(columns=years_to_drop)




# remove final row that is a source statement

# Get year columns
year_cols = [str(year) for year in range(1980, 2025)]

# Convert year columns from string to numeric, removing commas
for col in year_cols:
    if col in raw.columns:
        raw[col] = pd.to_numeric(raw[col].astype(str).str.replace(',', ''), errors='coerce')



raw = raw[:-1].copy()

# append country information
country_info = pd.read_csv("data/country_info.csv")

raw = raw.merge(country_info, on='country', how='left')


# get most recent value

# Get the year columns (1980-2024)
year_cols = [str(year) for year in range(1980, 2025)]

# Create the most_recent column (LAST non-null value, going right to left)
raw['most_recent'] = raw[year_cols].ffill(axis=1).iloc[:, -1]

# Create the most_recent_year column (which year it came from)
def get_most_recent_year(row):
    for year in reversed(year_cols):  # Start from 2024 and go backwards
        if pd.notna(row[year]):
            return year
    return None

raw['most_recent_year'] = raw.apply(get_most_recent_year, axis=1)


# create simple data frame with core variables

subject_codes = ['PPPGDP', # GDP PPP International $
                'PPPPC', # GPD Per Capita PPP International $
                'PCPIPCH', # Inflation %
                'LUR', # Unemplpyment rate
                'LP', # population
]



simple_df = raw[raw['weo_subject_code'].isin(subject_codes)].reset_index(drop=True)


desired_cols = ['country', 'region7', 'subject_descriptor', 'most_recent', 'most_recent_year', 'econ_group', 'group_g7', 'group_european_union', 'group_asean5']
simple_df = simple_df[desired_cols]


subject_mapping = {
    'Gross domestic product, current prices': 'gdp',
    'Gross domestic product per capita, current prices': 'gdp_capita',
    'Inflation, average consumer prices': 'inflation',
    'Unemployment rate': 'unemployment',
    'Population': 'population'
}


# Pivot the data
pivoted_df = simple_df.pivot(index=['country', 'region7','econ_group', 'group_g7', 'group_european_union', 'group_asean5'], 
                            columns='subject_descriptor', 
                            values='most_recent')

# Rename the columns using the mapping
pivoted_df = pivoted_df.rename(columns=subject_mapping)

# Reset index to make country a regular column again
pivoted_df = pivoted_df.reset_index()

# Clean up the column names (remove the name from columns index)
pivoted_df.columns.name = None

pivoted_df.to_csv('clean/weo-simple.csv', index=False)
