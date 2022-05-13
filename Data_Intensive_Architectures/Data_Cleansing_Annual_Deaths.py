"""This file is responsible for the data cleansing of the annual-number-of-deaths-by-world-region file.The following steps are applied:- All records for continents are removed.- Continent added to each country record."""# Importing necessary libraries:import pandas as pdimport os# Importing csv file:df = pd.read_csv("D:/Git/Data-Intensive-Architecture-Final-Project/Datasets/annual-number-of-deaths-by-world-region.csv")print("Here are the first 5 rows of our dataset: ", "\n", df.head(5), "\n")# Next print columns names:print("Here are all the columns present within the dataset: ")for col in df.columns:    print(col)print("\n")# Next print unique records within the 'Entity' column:print("The 'Entity' column has the following unique value ", "\n", df.Entity.unique(), "\n" , "which is a total of ",      df['Entity'].nunique(), "unique values, being a mixture of countries and continents", "\n")# Next we create a list containing all continent names and all instances of continent names:continents = ['Africa', 'Asia', 'Asia, Central', 'Australia & New Zealand', 'Caribbean', 'Central African Republic', 'Central America',              'Central and Southern Asia', 'Channel Islands', 'Eastern Africa', 'Eastern Asia', 'Eastern Europe', 'Eastern and South-Eastern Asia',              'Europe', 'Europe and Northern America', 'Europe, Western', 'High income countries', 'Land-locked Developing Countries (LLDC)',              'Latin America and the Caribbean', 'Least Developed Countries', 'Less Developed Regions', 'Less Developed Regions, excluding China',              'Less Developed Regions, excluding Least Developed Countries', 'Low-income countries', 'Lower-middle-income countries',              'Melanesia',              'Micronesia (country)', 'Middle Africa', 'Middle-income countries', 'More Developed Regions', 'No income group available',              'Northern Africa', 'Northern Africa and Western Asia', 'Northern America', 'Northern Europe', 'Oceania',              'Oceania (excluding Australia and New Zealand)', 'Reunion', 'Small Island Developing States (SIDS)',              'South Africa', 'South America', 'South Eastern Asia', 'South Sudan', 'Southern Africa', 'Southern Asia',              'Southern Europe', 'Sub-Saharan Africa', 'United States Virgin Islands', 'Upper-middle-income countries',              'Western Africa', 'Western Asia', 'Western Sahara', 'World']# Next we filter out all instances of continent names from the df:for n in continents:    df = df[df.Entity != n]# List of all 194 countries in the world.#print(df.Entity.unique())print("We have successfully filtered out the continents as we now have: ", df['Entity'].nunique(), "of unique values")# Last we check for any null values:print("Below are the number of missing values within each column present: ", "\n", "\n", df.isnull().sum(), "\n")# Sanity checkprint(df[df.isnull().any(axis=1)])# To conclude we export the cleansed file:def export():# Restrict file from duplicating. - change the path to your desired location.    if os.path.exists(        "C:/Users/Rober/Downloads/Cleansed_Annual_Deaths-by-Country.csv"):        pass    else:# Export to csv        df.to_csv(            "C:/Users/Rober/Downloads/Cleansed_Annual_Deaths-by-Country.csv",            index=False, encoding='utf-8-sig', header=False)export()