#!/usr/bin/env python
# coding: utf-8

# In terminal
# 
# python -m venv venv
# 
# venv_Create\Scripts\activate

# Reading the files

# In[114]:


## In terminal do pip install pandas 

import os
import pandas as pd

# -------------------------------
# Step 1: Load CSV files
# -------------------------------

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")

providers = pd.read_csv(os.path.join(DATA_DIR, "providers_data.csv"))
receivers = pd.read_csv(os.path.join(DATA_DIR, "receivers_data.csv"))
food = pd.read_csv(os.path.join(DATA_DIR, "food_listings_data.csv"))
claims = pd.read_csv(os.path.join(DATA_DIR, "claims_data.csv"))


# Checking the data inside the variables

# In[115]:


dataframes = {
    'claims data': claims, 
    'food data': food, 
    'providers data': providers, 
    'receivers data': receivers
}

for name, df in dataframes.items():
    print(f"\n{name.capitalize()} (first 5 rows):")
    print(df.head())


# Schema creation

# In[116]:


schema = {
    'providers data': {
        'Provider_ID': 'int',
        'name': 'str',
        'type': 'str',
        'address': 'str',
        'city': 'str',
        'contact': 'str'
    },
    'receivers data': {
        'receiver_id': 'int',
        'name': 'str',
        'type': 'str',
        'city': 'str',
        'contact': 'str'
    },
    'food data': {
        'food_id': 'int',
        'food_name': 'str',
        'quantity': 'int',
        'expiry_date': 'date',
        'provider_id': 'int'
    },
    'claims data': {
        'claim_id': 'int',
        'food_id': 'int',
        'receiver_id': 'int',
        'claim_date': 'datetime',
        'status': 'str'
    }
}


# Checking Duplicates and nulls and missing values :in the table

# In[117]:


for name, df in dataframes.items():
    print(f"\n--- {name.capitalize()}  ---")

    # Check for duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        print(f"Number of duplicate rows: {duplicates}")
    else:
        print("No duplicate rows found.")

    # Check for nulls
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        print("\nNull values per column:")
        print(null_counts[null_counts > 0])
    else:
        print("No null values found in any column.")


# In[118]:


for name, df in dataframes.items():
    initial_rows = len(df)

    df.drop_duplicates(inplace=True)
    if len(df) < initial_rows:
        print(f"Dropped {initial_rows - len(df)} duplicate rows from {name}.")

    initial_rows_after_duplicates = len(df)

    df.dropna(inplace=True)
    if len(df) < initial_rows_after_duplicates:
        print(f"Dropped {initial_rows_after_duplicates - len(df)} rows with null values from {name}.")

    # Update the original dataframe variables
    if name == 'claims data': claims = df
    elif name == 'food data': food = df
    elif name == 'providers data': providers = df
    elif name == 'receivers data': receivers = df



# In[119]:


# -------------------------------
# Step: Fix DATE formats
# -------------------------------

# Convert expiry_date in food table
food['Expiry_Date'] = pd.to_datetime(
    food['Expiry_Date'], 
    dayfirst=True,   # Important for Indian format
    errors='coerce'
)

claims['Timestamp'] = pd.to_datetime(
    claims['Timestamp'],
    errors='coerce'
)


# In[120]:


for name, df in dataframes.items():
    print(f"\n{name.capitalize()} (first 5 rows):")
    print(df.head())


# Validating the data types and formats after cleaning and transformations.

# In[121]:


def validate_column(df, col, dtype):

    if dtype == 'int':
        invalid = df[~df[col].astype(str).str.match(r'^\d+$', na=False)]

    elif dtype == 'str':
        # allow alphabets + spaces (customize if needed)
        invalid = df[df[col].astype(str).str.contains(r'\d', na=False)]

    elif dtype == 'date':
        parsed = pd.to_datetime(df[col], errors='coerce')
        invalid = df[parsed.isnull()]

    elif dtype == 'datetime':
        parsed = pd.to_datetime(df[col], errors='coerce')
        invalid = df[parsed.isnull()]

    else:
        invalid = pd.DataFrame()

    return invalid


# In[122]:


for table_name, df in dataframes.items():
    print(f"\n Checking table: {table_name}")

    for col, dtype in schema[table_name].items():

        if col in df.columns:
            invalid_rows = validate_column(df, col, dtype)

            if not invalid_rows.empty:
                print(f"\n Column issue: {col} (Expected: {dtype})")
                print(invalid_rows[[col]].head())
            else:
                print(f" {col} is valid")


# In[123]:


print("Data cleaning completed.")


# SQL connection

# In[124]:


# pip install mysql-connector-python 

import mysql.connector
HOSTName = "127.0.0.1"
USERName = "root"
Password = "Eswari@99"

# -------------------------------
# Step 2: Connect to MySQL
# -------------------------------
conn = mysql.connector.connect(
    host=HOSTName,
    user=USERName,
    password=Password
)

print(conn)

mycursor=conn.cursor()

print(mycursor)


# Creating the database 

# In[ ]:


mycursor.execute("DROP DATABASE IF EXISTS FOOD_MANGEMENT")

mycursor.execute("CREATE DATABASE FOOD_MANGEMENT")


# In[ ]:


mycursor.execute("USE FOOD_MANGEMENT")


# In[ ]:


# Step 2: Create tables

mycursor.execute("DROP TABLE IF EXISTS claims;")
mycursor.execute("DROP TABLE IF EXISTS food_listings;")
mycursor.execute("DROP TABLE IF EXISTS providers;")
mycursor.execute("DROP TABLE IF EXISTS receivers;")

mycursor.execute("""CREATE TABLE providers (
    provider_id INT AUTO_INCREMENT  PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,
    address TEXT NOT NULL,
    city VARCHAR(50) NOT NULL,
    contact TEXT NOT NULL
);""")

mycursor.execute("""CREATE TABLE receivers (
    receiver_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL,
    contact TEXT NOT NULL
);""")

mycursor.execute("""CREATE TABLE food_listings (
    food_id INT AUTO_INCREMENT PRIMARY KEY,
    food_name VARCHAR(100) NOT NULL,
    quantity INT NOT NULL,
    expiry_date DATE NOT NULL,
    provider_id INT NOT NULL,
    provider_type VARCHAR(50) NOT NULL,
    location VARCHAR(50) NOT NULL,
    food_type VARCHAR(50) NOT NULL,
    meal_type VARCHAR(50) NOT NULL,
    FOREIGN KEY (provider_id) REFERENCES providers(provider_id)
);""")

mycursor.execute("""CREATE TABLE claims (
    claim_id INT AUTO_INCREMENT PRIMARY KEY,
    food_id INT NOT NULL,
    receiver_id INT NOT NULL,
    claim_date VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    FOREIGN KEY (food_id) REFERENCES food_listings(food_id),
    FOREIGN KEY (receiver_id) REFERENCES receivers(receiver_id)
);""")


# In[ ]:


# Step 3: Insert data into tables

for _, row in providers.iterrows():
    mycursor.execute("""
    INSERT INTO providers VALUES (%s,%s,%s,%s,%s,%s)
    """, tuple(row))

for _, row in receivers.iterrows():
    mycursor.execute("""
    INSERT INTO receivers VALUES (%s,%s,%s,%s,%s)
    """, tuple(row))

for _, row in food.iterrows():
    mycursor.execute("""
    INSERT INTO food_listings VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, tuple(row))

for _, row in claims.iterrows():
    mycursor.execute("""
    INSERT INTO claims VALUES (%s,%s,%s,%s,%s)
    """, tuple(row))


# In[ ]:


conn.commit()

mycursor.close()

conn.close()


# SQL QUERIES FOR ANALYSIS

# In[ ]:


## command to run query and get results as dataframe

def run_query(query):
    conn = mysql.connector.connect(
        host=HOSTName,
        user=USERName,
        password=Password,
        database="FOOD_MANGEMENT"
    )

    df = pd.read_sql(query, conn)
    conn.close()
    return df


# In[ ]:


## Providers & Receivers by City

q1 = """
SELECT p.city,
COUNT(DISTINCT p.provider_id) AS providers,
COUNT(DISTINCT r.receiver_id) AS receivers
FROM providers p
LEFT JOIN receivers r ON p.city = r.city
GROUP BY p.city;
"""

print(run_query(q1))


# In[ ]:


## Top Providers by Food Quantity

q2 = """
SELECT provider_type, SUM(quantity) AS total_food
FROM food_listings
GROUP BY provider_type
ORDER BY total_food DESC;
"""

print(run_query(q2))


# In[ ]:


## Provider details

q3 = """
SELECT name, contact FROM providers;
"""

print(run_query(q3))


# In[ ]:


## Top Receivers 

q4 = """
SELECT receiver_id, COUNT(*) AS total_claims
FROM claims
GROUP BY receiver_id
ORDER BY total_claims DESC;
"""

print(run_query(q4))


# In[ ]:


## Total Food

q5 = "SELECT SUM(quantity) AS total_food FROM food_listings;"

print(run_query(q5))


# In[ ]:


## city wth highest listings

q6 = """
SELECT location, COUNT(*) AS listings
FROM food_listings
GROUP BY location
ORDER BY listings DESC;
"""

print(run_query(q6))


# In[ ]:


## Types of food commonly listed

q7 = """
SELECT food_type, COUNT(*) FROM food_listings
GROUP BY food_type;
"""

print(run_query(q7))


# In[ ]:


## Food calims

q8 = """
SELECT food_id, COUNT(*) FROM claims
GROUP BY food_id;
"""

print(run_query(q8))


# In[ ]:


## Top provider by claims

q9 = """
SELECT f.provider_id, COUNT(*) 
FROM food_listings f
JOIN claims c ON f.food_id = c.food_id
WHERE c.status='Completed'
GROUP BY f.provider_id
ORDER BY COUNT(*) DESC;
"""

print(run_query(q9))


# In[ ]:


## claims by status

q10 = """
SELECT status,
COUNT(*) * 100 / (SELECT COUNT(*) FROM claims) AS percentage
FROM claims
GROUP BY status;
"""

print(run_query(q10))


# In[ ]:


## average quantity claimed per receiver

q11 = """
SELECT c.receiver_id, AVG(f.quantity)
FROM claims c
JOIN food_listings f ON c.food_id = f.food_id
GROUP BY c.receiver_id;
"""

print(run_query(q11))


# In[ ]:


## Most claimed food items

q12 = """
SELECT f.meal_type, COUNT(*)
FROM food_listings f
JOIN claims c ON f.food_id = c.food_id
GROUP BY f.meal_type
ORDER BY COUNT(*) DESC;
"""

print(run_query(q12))


# In[ ]:


## Total food per provider type

q13 = """
SELECT provider_id, SUM(quantity)
FROM food_listings
GROUP BY provider_id;
"""

print(run_query(q13))


# STREAMLIT APP  

# In[ ]:


query_map = {
    "Providers & Receivers per City": q1,
    "Top Provider Type": q2,
    "Provider Contacts by City": q3,
    "Top Receivers": q4,
    "Total Food": q5,
    "City Highest Listings": q6,
    "Common Food Types": q7,
    "Claims per Food": q8,
    "Top Provider by Claims": q9,
    "Claim Status %": q10,
    "Avg Quantity per Receiver": q11,
    "Most Claimed Meal": q12,
    "Total Food per Provider": q13
}


# In[ ]:


##jupyter nbconvert --to python analysis_details.ipynb --output analysis_details.py

###get_ipython().system('jupyter nbconvert --to python analysis_details.ipynb')

