import pandas as pd

df = pd.read_csv("clean/titanic/titanic.csv")

# What does our entire dataset look like?
len(df)
df.columns # do we understand these? if not, how can we?
# sibsp: Number of Siblings/Spouses Aboard
# parch: Number of Parents/Children Aboard

# rename: "old": "new" ---> must reassign
df = df.rename(columns={
    'sibsp':'siblings_spouses',
    'parch':'parents_children'
})


df.info() # let's decode this... what do you notice?

# handling missing

df_full = df.dropna() # check the index
df_full = df.dropna().reset_index()
df_full = df.dropna().reset_index()
df_filled = df.fillna(999) # kind of silly for non-numerics
df_filled = df.fillna(999) 

df_filled['age'] = df['age'].fillna(999)
mean_age = df['age'].mean()
df_filled['age'] = df['age'].fillna(mean_age) # maybe this is wierd (maybe good)
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




