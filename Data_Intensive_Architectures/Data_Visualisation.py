import pandas as pd
import seaborn as sns
sns.set_theme(style="whitegrid")
import scipy.stats as stats
import matplotlib.pyplot as plt

# Importing our dataframe:
df = pd.read_csv('/Users/polinaprinii/Documents/GitHub/Data-Intensive-Architecture-Final-Project/Data_Intensive_Architectures/Output/Final_Data.csv')
print(df.head(5), '\n')

# Assigning our x (independent) and y dependent variables.
x = df['Average_Fertility_Rate'].values
y = df['Total_Number_of_Deaths'].values

# Fitting our model
model = stats.linregress( x, y )

# Evaluating results
print('The R-squared value for the linear relationship between "Total Number of Deaths" and "Average Fertility Rate" is: ',
      round(model.rvalue, 2), '\n')


# We now convert out our Total Number of Deaths from billions to a singular number to by diving by 1000 million.
df['Total_Number_of_Deaths'] = df.groupby('Country')['Total_Number_of_Deaths'].transform(lambda x: round((x / 1000000), 2))
print(df.head(5), '\n')

# First we set the ordering of ASC or DESC for the bar plot of the world view:
plot_order_W = df.groupby('Country')['Total_Number_of_Deaths'].sum().sort_values(ascending=False).index.values

# Next we map the bar plot:
sns.set(rc = {'figure.figsize':(25,35)})
plot_W = sns.barplot(x="Total_Number_of_Deaths", y="Country", data=df,
                   order=plot_order_W) #.set(title='World View of Annual Deaths Over 70 Years')
plot_W.bar_label(plot_W.containers[0])
plot_W.set_xlabel("Total Number of Deaths in 100 Millions", fontsize = 35)
plot_W.set_ylabel("Country", fontsize = 35)
plt.show()

# Now we filter a Europe view:
countries = ['Russia', 'Germany', 'United Kingdom', 'France', 'Italy', 'Spain', 'Ukraine', 'Poland', 'Romania',
               'Netherlands', 'Belgium', 'Czechia', 'Greece', 'Portugal', 'Sweden', 'Hungary', 'Belarus', 'Austria',
               'Serbia', 'Switzerland', 'Bulgaria', 'Denmark', 'Finland', 'Slovakia', 'Norway', 'Ireland', 'Croatia',
               'Moldova', 'Bosnia and Herzegovina', 'Albania', 'Lithuania', 'North Macedonia', 'Slovenia', 'Latvia',
               'Kosovo', 'Estonia', 'Montenegro', 'Luxembourg', 'Malta', 'Iceland', 'Andorra', 'Monaco', 'Liechtenstein',
               'San Marino', 'Holy See']

# Now we filter the df to only return values for the above countries in the list.
df = df.loc[df['Country'].isin(countries)]

# Now we re-plot a Europe view of total number of deaths:
plot_order_E = df.groupby('Country')['Total_Number_of_Deaths'].sum().sort_values(ascending=False).index.values
sns.set(rc = {'figure.figsize':(15,20)})
plot_E = sns.barplot(x="Total_Number_of_Deaths", y="Country", data=df,
                   order=plot_order_E) #.set(title='Europe View of Annual Deaths Over 70 Years')
plot_E.bar_label(plot_E.containers[0])
plot_E.set_xlabel("Total Number of Deaths in 100 Millions", fontsize = 25)
plot_E.set_ylabel("Country", fontsize = 25)
plt.show()

# Now we look at the average fertility rate for Europe in a Pie format:
country_data = df["Country"]
fertility_data = df["Average_Fertility_Rate"]
plt.pie(fertility_data, labels=country_data, autopct='%1.1f%%', shadow=True, startangle=140)
plt.title("Average Fertility Rate within Europe Over 70 Years")
plt.show()

# Determining Pearson's correlation between the data.
# Mapping our dataframe to a correlation matrix.
cor = df.corr(method='pearson')

# Plotting the matrix.
plt.figure(figsize = (20, 10))
sns.heatmap(cor, annot = True)
plt.show()





