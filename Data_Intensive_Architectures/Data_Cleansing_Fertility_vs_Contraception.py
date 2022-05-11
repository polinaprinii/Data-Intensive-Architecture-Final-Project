"""
This file is responsible for the data cleansing of the fertility-vs-contraception file.
The following steps are applied:
- All records for continents are removed.
- Continent added to each country record.
"""

# Importing necessary libraries:
import pandas as pd
import os

# Importing csv file:
df = pd.read_csv("D:/Git/Data-Intensive-Architecture-Final-Project/Datasets/fertility-vs-contraception.csv")
print("Here are the first 5 rows of our dataset: ", "\n", df.head(5), "\n")

# Next print columns names:
print("Here are all the columns present within the dataset: ")
for col in df.columns:
    print(col)

print("\n")

# Next we drop the column 'Continent'
df = df.drop('Continent', 1)

# Next print unique records within the 'Entity' column:
print("The 'Entity' column has the following unique value ", "\n", df.Entity.unique(), "\n", "which is a total of ",
      df['Entity'].nunique(), "unique values, being a mixture of countries and continents", "\n")

# Next we drop all rows were the Code is not present, thus ultimately removing all continents and unwanted countries.
df = df[df.Code.notnull()]

# Now we check for any odd country codes:
print("Here are all the country codes ", "\n", df.Code.unique(), "\n")

# Next we filter out any instances of OWID within the country codes based on the output from above:
discard = ["OWID_"]

# Now we remove all instances of OWID
df = df[~df.Code.str.contains('|'.join(discard))]

# Next we import a list derived from the cleansing of the annual deaths files to ensure same regions are covered.
countries = ['Afghanistan', 'Albania', 'Algeria', 'Angola', 'Antigua and Barbuda',
 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan',
 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize',
 'Benin', 'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana', 'Brazil',
 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon',
 'Canada', 'Cape Verde', 'Chad', 'Chile', 'China', 'Colombia', 'Comoros', 'Congo',
 'Costa Rica', "Cote d'Ivoire", 'Croatia', 'Cuba', 'Curacao', 'Cyprus',
 'Czechia', 'Democratic Republic of Congo', 'Denmark', 'Djibouti',
 'Dominican Republic', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea',
 'Eritrea', 'Estonia', 'Eswatini', 'Ethiopia', 'Fiji', 'Finland', 'France',
 'French Guiana', 'French Polynesia', 'Gabon', 'Gambia', 'Georgia', 'Germany',
 'Ghana', 'Greece', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guinea',
 'Guinea-Bissau', 'Guyana', 'Haiti', 'Honduras', 'Hong Kong', 'Hungary',
 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy',
 'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', 'Kuwait',
 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya',
 'Lithuania', 'Luxembourg', 'Macao', 'Madagascar', 'Malawi', 'Malaysia',
 'Maldives', 'Mali', 'Malta', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte',
 'Melanesia', 'Mexico', 'Moldova', 'Mongolia', 'Montenegro', 'Morocco',
 'Mozambique', 'Myanmar', 'Namibia', 'Nepal', 'Netherlands', 'New Caledonia',
 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'North Korea',
 'North Macedonia', 'Norway', 'Oman', 'Pakistan', 'Palestine', 'Panama',
 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal',
 'Puerto Rico', 'Qatar', 'Romania', 'Russia', 'Rwanda', 'Saint Lucia',
 'Saint Vincent and the Grenadines', 'Samoa', 'Sao Tome and Principe',
 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore',
 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Korea', 'Spain',
 'Sri Lanka', 'Sudan', 'Suriname', 'Sweden', 'Switzerland', 'Syria', 'Taiwan',
 'Tajikistan', 'Tanzania', 'Thailand', 'Timor', 'Togo', 'Tonga',
 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Uganda',
 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States',
 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Venezuela', 'Vietnam', 'Yemen', 'Zambia',
 'Zimbabwe']

# Now we filter the df to only return values for the above countries in the list.
df = df.loc[df['Entity'].isin(countries)]

print("We now have a total of: ", df['Entity'].nunique(), "having removed all continents and unwanted countries",
      "with an expected total of 193, which in our case is correct.", "\n")

print("Below are the number of missing values within each column present: ", "\n", "\n", df.isnull().sum(), "\n")


# To conclude we export the cleansed file:
def export():
# Restrict file from duplicating. - change the path to your desired location.
    if os.path.exists(
        "C:/Users/Rober/Downloads/Cleansed_Fertility-vs-Contraception.csv"):
        pass

    else:
# Export to csv
        df.to_csv(
             "C:/Users/Rober/Downloads/Cleansed_Fertility-vs-Contraception.csv",
            index=False, encoding='utf-8-sig')

export()