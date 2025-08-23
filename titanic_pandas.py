import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


##### things before
## create data frame from scratch
## concat similar rows/cols



#####

df = pd.read_csv("clean/titanic/titanic.csv")

# What does our entire dataset look like?
len(df)
df.columns # do we understand these? if not, how can we?
# sibsp: Number of Siblings/Spouses Aboard
# parch: Number of Parents/Children Aboard

df.head()
df.tail(20)

# rename: "old": "new" ---> must reassign
df = df.rename(columns={
    'sibsp':'siblings_spouses',
    'parch':'parents_children'
})


df.info() # let's decode this... what do you notice?

# handling missing

df_full = df.dropna() # check the index
df_full = df.dropna().reset_index()

df_filled = df.fillna(999) # kind of silly for non-numerics
df_filled = df.fillna(999) 

df_filled['age'] = df['age'].fillna(999)
mean_age = df['age'].mean()


df_filled['age'] = df['age'].fillna(mean_age) # maybe this is wierd (maybe good - to identify)

df_filled['age'] = df['age'].fillna(round(mean_age, 0))

df['embark_town']
df.embark_town

df['class']
df.class # error

df['class'].nunique()
df['class'].value_counts()
df['class'].value_counts(ascending=True)
df['class'].value_counts(ascending=True, normalize=True)

class_order = ['First', 'Second', 'Third']
df['class'] = pd.Categorical(df['class'], categories=class_order, ordered=True)
df['class'].value_counts()
df['class'].value_counts(sort=False)


# some missing values: especially in age

# other more limited helpers
df.shape
df.dtypes

# quick summary stats
df.describe()
df.describe(include=['object'])
df.describe(include='all')

# how old were the passengers?

df['age'].describe()

df['age'].sum()
df['age'].count() # non-null
df['age'].median()
df['age'].quantile(0.5)
df['age'].min()
df['age'].max()
df['age'].mean()
df['age'].var()
df['age'].std()

# can we create new variables
df['age_days'] = df['age'] * 365 
df.assign(age_days2=lambda df: df.age * 365)

df.age_days
df.describe()

# how old were the oldest/youngest passengers

df.nlargest(10, 'age')
df.nsmallest(10, 'age')

df.age_buckets = pd.qcut(df.age, 10, labels=False)
df['age_buckets'] = pd.qcut(df.age, 10, labels=False)
df.age_buckets
df['age_ranges'] = pd.qcut(df.age, 10)






#### reshaping data

luggage = pd.read_csv("clean/titanic/titanic_luggage.csv")
luggage

luggage_wide = luggage.pivot(index='passenger_id', 
        columns='bag_number', 
        values='weight_kgs').reset_index()

# Add new variables: one by one
# 1. Count number of bags per passenger (non-null values)
# "For each passenger, look at their bag weight columns (columns 1, 2, 3, 4, 5), count how many of those columns actually have numbers in them (not empty), and put that count in a new column called 'bag_count'."
luggage_wide['bag_count'] = luggage_wide.iloc[:, 1:].notna().sum(axis=1)
luggage_wide.iloc[:, 1:].notna() # we sum because this returns boolean

# alt
luggage.groupby('passenger_id').count()



# 2. Total weight per passenger
luggage_wide['total_weight'] = luggage_wide.iloc[:, 1:-1].sum(axis=1, skipna=True)

# 3. Average weight per passenger
luggage_wide['average_weight'] = luggage_wide.iloc[:, 1:-2].mean(axis=1, skipna=True)


# Assign is bett4er

bag_cols = [1,2,3,4,5]
bag_cols = [col for col in luggage_wide.columns if col != 'passenger_id'] # or

luggage_wide = luggage_wide.assign(
    bag_count=luggage_wide[bag_cols].notna().sum(axis=1),
    total_weight=luggage_wide[bag_cols].sum(axis=1, skipna=True),
    average_weight=luggage_wide[bag_cols].mean(axis=1, skipna=True)
)


#### group by

luggage.groupby('passenger_id').count() # gives all and redundant

luggage.groupby('passenger_id').size()
luggage.groupby('passenger_id')['bag_number'].count()
luggage.groupby('passenger_id')['bag_number'].count().reset_index()
luggage.value_counts('passenger_id').reset_index() # because we know that each line is a baggage record

luggage.groupby('passenger_id')['weight_kgs'].sum()
luggage.groupby('passenger_id')['weight_kgs'].mean()

# we can be more efficient

luggage_summary = luggage.groupby('passenger_id')['weight_kgs'].agg([
    ('bag_count', 'count'),
    ('total_weight', 'sum'),
    ('mean_weight', 'mean')
]).reset_index()


# now that we have a unique passenger as the main unit of analysis, we can combine it into the main dataset

# left table = df (original passenger)
# right table = luggage_summary (per-passenger)

# add records to all original passengers
combo = pd.merge(df, luggage_summary, how = 'left', on = 'passenger_id')
combo

# only keep people who were im both datasets based on the on var: "inner"
pd.merge(df, luggage_summary, how = 'inner', on='passenger_id') # same rows --> everyone had at least one bag


combo['fare'].corr(combo['bag_count'])
combo.corr() # error, why?

combo.dtypes # not sure why it is even looking at the object, should be just numeric by default


combo.select_dtypes(include=[np.number]).corr() # force it to only look at numerics

combo.plot.scatter(x = 'fare', y = 'total_weight')
combo.plot.scatter(x = 'bag_count', y = 'total_weight')

combo.plot.hist(y = 'total_weight')
combo.plot.hist(y = 'total_weight', bins = 20)
combo['weight_bins'] = pd.qcut(combo['total_weight'], 20)

# in hindsight, what is bad about the total_weight variable name?

combo
melted_df = pd.melt(combo, 
                    id_vars=['passenger_id'],           # Column to keep as identifier
                    value_vars=['siblings_spouses', 'parents_children'],         # Columns to melt
                    var_name='metric',                  # Name for the new column with variable names
                    value_name='value')   

melted_df.boxplot(column='value', by='metric', figsize=(8, 6))
combo.plot.hist(y = 'siblings_spouses')
combo.plot.hist(y = 'parents_children')

combo_family = combo.query("siblings_spouses > 0 and parents_children > 0").reset_index()
combo_family.shape

melted_df = pd.melt(combo_family, 
                    id_vars=['passenger_id'],           # Column to keep as identifier
                    value_vars=['siblings_spouses', 'parents_children'],         # Columns to melt
                    var_name='metric',                  # Name for the new column with variable names
                    value_name='value')   

melted_df.boxplot(column='value', by='metric')

combo['with_family'] = (combo['siblings_spouses'] > 0) | (combo['parents_children'] > 0)

combo['with_family'].value_counts()
combo.value_counts('with_family')

combo.groupby(['with_family', 'survived']).size()

# method chaining
(combo.groupby(['with_family', 'survived'])
    .size()
    .reset_index(name='count')
    .assign(percentage=lambda x: x.groupby('with_family')['count'].transform(lambda y: y / y.sum() * 100)))


# subset, don't add

combo[(combo['siblings_spouses'] > 0) | (combo['parents_children'] > 0)]
combo.query("siblings_spouses > 0 or parents_children > 0")


combo.sample(frac = 0.2)
combo.sample(n = 400)


combo['age'] # get series
combo[['age']] # keep df
combo[['age', 'fare']] # keep df

# select contains
## https://regex101.com/
## https://regexr.com/

combo.filter(regex = '_') # contains underscore
combo.filter(items=[col for col in combo.columns if '_' in col])

# does not contain
combo.filter(regex='^(?!.*_).*$')
underscore_cols = combo.filter(regex='_').columns
combo[underscore_cols]
no_underscore_cols = combo.columns.difference(underscore_cols) # returns all column names that exist in the first set but not in the second set - essentially set subtraction for pandas indexes.
combo[no_underscore_cols]

# starts with

combo.filter(regex='^s') # starts with s 
combo.filter(items=[col for col in combo.columns if col.startswith('s')])

# does not start with

combo.filter(regex='^(?!s)')  # Negative lookahead
combo.filter(items=[col for col in combo.columns if not col.startswith('s')])

### ends with

combo.filter(regex='^(s|p)') # s or p
combo.filter(regex='s$') # ends with s
combo.filter(items=[col for col in combo.columns if col.endswith('s')])

### does not end with
combo.filter(regex='(?<!s)$')  # Negative lookbehind
combo.filter(items=[col for col in combo.columns if not col.endswith('s')])


# so many ways, does it work!

# next: filter column strings with contains does not contain, etc... 