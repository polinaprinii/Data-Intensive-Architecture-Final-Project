import pandas as pd
import seaborn as sns
sns.set_theme()
import scipy.stats as stats

# Importing our dataframe:
df = pd.read_csv('/Users/polinaprinii/Documents/GitHub/Data-Intensive-Architecture-Final-Project/Data_Intensive_Architectures/Output/Final_Data.csv'
                 )

# Assigning our x (independent) and y dependent variables.
x = df['Average Fertility Rate'].values
y = df['Total Number of Deaths'].values


# If you need the model coefficients stored in variables for later use, do:
model = stats.linregress( x, y )
beta0 = model.intercept
beta1 = model.slope

# If you just need to see the coefficients (and some other related data),
# do this alone:
print('The R-squared value for the linear relationship between "Total Number of Deaths" and "Average Fertility Rate" is: ',
      round(model.rvalue, 2), '\n')

