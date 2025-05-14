# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python [conda env:base] *
#     language: python
#     name: conda-base-py
# ---

# %% [markdown]
# # Task 6D: pandas vs SQL
# ### Name: Bhuvan Purushothaman Subramani
# ### Student Number: 224113776
# ### Email: s224113776@deakin.edu.au
# ### Program: Postgraduate (SIT731)

# %% [markdown]
# # *Introduction*
#  * This notebook demonstrates how SQL-style queries can be implemented using the pandas library in Python.
#  * The dataset used is `nycflights13`, which includes flight, weather, plane, airline, and airport data for all flights that departed from NYC in 2013.
#  * We compare SQL results from a SQLite database to equivalent pandas results and verify their correctness.
#


# %%
# *Imports*
import pandas as pd
import sqlite3
# Load CSV files into pandas (assumes files are in the same folder as this script)*
flights = pd.read_csv("flights.csv", comment="#")
airlines = pd.read_csv("airlines.csv", comment="#")
airports = pd.read_csv("airports.csv", comment="#")
planes = pd.read_csv("planes.csv", comment="#")
weather = pd.read_csv("weather.csv", comment="#")

# %%
# Create SQLite database and export DataFrames*
conn = sqlite3.connect("flights.db")

flights.to_sql("flights", conn, if_exists="replace", index=False)
airlines.to_sql("airlines", conn, if_exists="replace", index=False)
airports.to_sql("airports", conn, if_exists="replace", index=False)
planes.to_sql("planes", conn, if_exists="replace", index=False)
weather.to_sql("weather", conn, if_exists="replace", index=False)

print("Data loaded into SQLite and pandas.")

# %% [markdown]
# ### *Query 1 - Distinct Engine Types*
# *Explanation:*
# This query retrieves all unique engine types present in the planes dataset.
# - SQL: SELECT DISTINCT engine FROM planes
# - pandas: Use drop_duplicates() on the 'engine' column.

# %%
query1_sql = pd.read_sql_query("""
    SELECT DISTINCT engine FROM planes
""", conn)

query1_pandas = planes[['engine']].drop_duplicates().reset_index(drop=True)

pd.testing.assert_frame_equal(query1_sql.sort_values(by="engine").reset_index(drop=True),
                              query1_pandas.sort_values(by="engine").reset_index(drop=True))

print("Query 1 passed: pandas and SQL results match.")

# %% [markdown]
# ### *Query 2 - Distinct type and engine combinations*
# *Explanation:*
# This query lists all unique (type, engine) combinations in the planes dataset.
# We use drop_duplicates on both columns together.

# %%
query2_sql = pd.read_sql_query("""
    SELECT DISTINCT type, engine FROM planes
""", conn)

query2_pandas = planes[['type', 'engine']].drop_duplicates().reset_index(drop=True)

pd.testing.assert_frame_equal(query2_sql.sort_values(by=["type", "engine"]).reset_index(drop=True),
                              query2_pandas.sort_values(by=["type", "engine"]).reset_index(drop=True))

print("Query 2 passed: pandas and SQL results match.")

# %% [markdown]
# ### *Query 3 - Count of Planes by Engine Type*
# *Explanation:*
# This query counts the number of planes for each engine type.
# - SQL: SELECT COUNT(*), engine FROM planes GROUP BY engine
# - pandas: Use groupby() and count().

# %%
# SQL solution
query3_sql = pd.read_sql_query("""
    SELECT count(*), engine FROM planes GROUP BY engine
""", conn)

# pandas solution
query3_pandas = planes.groupby('engine', as_index=False).size().rename(columns={'size': 'count'})

# Rename the 'count' column to match SQL result column name 'count(*)'
query3_pandas = query3_pandas.rename(columns={'count': 'count(*)'})

# Reorder pandas columns to match SQL result order
query3_pandas = query3_pandas[['count(*)', 'engine']]

# Sort before comparing to handle row order differences
pd.testing.assert_frame_equal(query3_sql.sort_values(by="engine").reset_index(drop=True),
                              query3_pandas.sort_values(by="engine").reset_index(drop=True))

print("Query 3 passed: pandas and SQL results match.")


# %% [markdown]
# ### *Query 4 - Count of Planes by Engine and Type*
# *Explanation:*
# This query counts the number of planes grouped by both engine type and plane type.
# - SQL: SELECT COUNT(*), engine, type FROM planes GROUP BY engine, type
# - pandas: Use groupby() on both columns and size().

# %%
# SQL solution
query4_sql = pd.read_sql_query("""
    SELECT COUNT(*), engine, type FROM planes GROUP BY engine, type
""", conn)

# pandas solution
query4_pandas = planes.groupby(['engine', 'type']).size().reset_index(name='count')

# Rename the 'count' column to match SQL result column name 'COUNT(*)'
query4_pandas = query4_pandas.rename(columns={'count': 'COUNT(*)'})

# Reorder pandas columns to match SQL result order
query4_pandas = query4_pandas[['COUNT(*)', 'engine', 'type']]

# Sort before comparing to handle row order differences
pd.testing.assert_frame_equal(query4_sql.sort_values(by=["engine", "type"]).reset_index(drop=True),
                              query4_pandas.sort_values(by=["engine", "type"]).reset_index(drop=True))

print("Query 4 passed: pandas and SQL results match.")


# %% [markdown]
# ### *Query 5 - Minimum, Average, and Maximum Year by Engine and Manufacturer*
# *Explanation:*
# This query computes the minimum, average, and maximum plane manufacturing year for each engine type and manufacturer.
# - SQL: SELECT MIN(year), AVG(year), MAX(year), engine, manufacturer FROM planes GROUP BY engine, manufacturer
# - pandas: Use groupby() and aggregation functions like min(), mean(), and max().

# %%
# SQL solution
query5_sql = pd.read_sql_query("""
    SELECT MIN(year), AVG(year), MAX(year), engine, manufacturer
    FROM planes
    GROUP BY engine, manufacturer
""", conn)

# pandas solution
query5_pandas = planes.groupby(['engine', 'manufacturer'])['year'].agg(['min', 'mean', 'max']).reset_index()

# Rename pandas columns to match SQL result
query5_pandas = query5_pandas.rename(columns={
    'min': 'MIN(year)',
    'mean': 'AVG(year)',
    'max': 'MAX(year)'
})

# Reorder columns to match SQL output order
query5_pandas = query5_pandas[['MIN(year)', 'AVG(year)', 'MAX(year)', 'engine', 'manufacturer']]

# Sort before comparing
pd.testing.assert_frame_equal(
    query5_sql.sort_values(by=["engine", "manufacturer"]).reset_index(drop=True),
    query5_pandas.sort_values(by=["engine", "manufacturer"]).reset_index(drop=True)
)

print("Query 5 passed: pandas and SQL results match.")


# %% [markdown]
# ### *Query 6 - Planes with Non-Null Speed*
# *Explanation:*
# This query retrieves all planes where the speed is not null.
# - SQL: SELECT * FROM planes WHERE speed IS NOT NULL
# - pandas: Filter the dataframe to exclude rows where 'speed' is null.

# %%
query6_sql = pd.read_sql_query("""
    SELECT * FROM planes WHERE speed IS NOT NULL
""", conn)

query6_pandas = planes[planes['speed'].notnull()]

pd.testing.assert_frame_equal(query6_sql.sort_values(by="tailnum").reset_index(drop=True),
                              query6_pandas.sort_values(by="tailnum").reset_index(drop=True))

print("Query 6 passed: pandas and SQL results match.")

# %% [markdown]
# ### *Query 7 - Planes with Seats between 150 and 210 and Year >= 2011*
# *Explanation:*
# This query retrieves the tailnum of planes where seats are between 150 and 210 and the year is greater than or equal to 2011.
# - SQL: SELECT tailnum FROM planes WHERE seats BETWEEN 150 AND 210 AND year >= 2011
# - pandas: Filter the dataframe based on the conditions.

# %%
# SQL solution
query7_sql = pd.read_sql_query("""
    SELECT tailnum FROM planes
    WHERE seats BETWEEN 150 AND 210 AND year >= 2011
""", conn)

# pandas solution
query7_pandas = planes[(planes['seats'] >= 150) & (planes['seats'] <= 210) & (planes['year'] >= 2011)][['tailnum']]

# Sort before comparing
pd.testing.assert_frame_equal(
    query7_sql.sort_values(by="tailnum").reset_index(drop=True),
    query7_pandas.sort_values(by="tailnum").reset_index(drop=True)
)

print("Query 7 passed: pandas and SQL results match.")


# %% [markdown]
# ### *Query 8 - Planes from Specific Manufacturers with Seats > 390*
# *Explanation:*
# This query retrieves planes from specific manufacturers (BOEING, AIRBUS, EMBRAER) with more than 390 seats.
# - SQL: SELECT tailnum, manufacturer, seats FROM planes WHERE manufacturer IN ("BOEING", "AIRBUS", "EMBRAER") AND seats > 390
# - pandas: Filter the dataframe based on the manufacturer and seats conditions.

# %%
query8_sql = pd.read_sql_query("""
    SELECT tailnum, manufacturer, seats FROM planes
    WHERE manufacturer IN ("BOEING", "AIRBUS", "EMBRAER") AND seats > 390
""", conn)

query8_pandas = planes[(planes['manufacturer'].isin(["BOEING", "AIRBUS", "EMBRAER"])) & (planes['seats'] > 390)][['tailnum', 'manufacturer', 'seats']]

pd.testing.assert_frame_equal(query8_sql.sort_values(by="tailnum").reset_index(drop=True),
                              query8_pandas.sort_values(by="tailnum").reset_index(drop=True))

print("Query 8 passed: pandas and SQL results match.")
# %% [markdown]
# ### *Query 9 - Distinct Year and Seats, Ordered by Year and Seats*
# *Explanation:*
# This query retrieves distinct combinations of year and seats, ordered by year ascending and seats descending.
# - SQL: SELECT DISTINCT year, seats FROM planes WHERE year >= 2012 ORDER BY year ASC, seats DESC
# - pandas: Use drop_duplicates() and sort_values().

# %%
query9_sql = pd.read_sql_query("""
    SELECT DISTINCT year, seats FROM planes
    WHERE year >= 2012 ORDER BY year ASC, seats DESC
""", conn)

query9_pandas = planes[planes['year'] >= 2012][['year', 'seats']].drop_duplicates().sort_values(by=['year', 'seats'], ascending=[True, False]).reset_index(drop=True)

pd.testing.assert_frame_equal(query9_sql.sort_values(by=["year", "seats"], ascending=[True, False]).reset_index(drop=True),
                              query9_pandas.sort_values(by=["year", "seats"], ascending=[True, False]).reset_index(drop=True))

print("Query 9 passed: pandas and SQL results match.")

# %% [markdown]
# ### *Query 10 - Distinct Year and Seats, Ordered by Seats and Year*
# *Explanation:*
# This query retrieves distinct combinations of year and seats, ordered by seats descending and year ascending.
# - SQL: SELECT DISTINCT year, seats FROM planes WHERE year >= 2012 ORDER BY seats DESC, year ASC
# - pandas: Use drop_duplicates() and sort_values().

# %%
query10_sql = pd.read_sql_query("""
    SELECT DISTINCT year, seats FROM planes
    WHERE year >= 2012 ORDER BY seats DESC, year ASC
""", conn)

query10_pandas = planes[planes['year'] >= 2012][['year', 'seats']].drop_duplicates().sort_values(by=['seats', 'year'], ascending=[False, True]).reset_index(drop=True)

pd.testing.assert_frame_equal(query10_sql.sort_values(by=["seats", "year"], ascending=[False, True]).reset_index(drop=True),
                              query10_pandas.sort_values(by=["seats", "year"], ascending=[False, True]).reset_index(drop=True))

print("Query 10 passed: pandas and SQL results match.")

# %% [markdown]
# ### *Query 11 - Count of Manufacturers with Planes Having More Than 200 Seats*
# *Explanation:*
# This query counts how many planes with more than 200 seats exist for each manufacturer.
# - SQL: SELECT manufacturer, COUNT(*) FROM planes WHERE seats > 200 GROUP BY manufacturer
# - pandas: Use groupby() and size() with a condition on the seats.

# %%
# SQL solution
query11_sql = pd.read_sql_query("""
    SELECT manufacturer, COUNT(*) FROM planes
    WHERE seats > 200 GROUP BY manufacturer
""", conn)

# Rename SQL column for consistency
query11_sql = query11_sql.rename(columns={'COUNT(*)': 'count'})

# pandas solution
query11_pandas = planes[planes['seats'] > 200].groupby('manufacturer').size().reset_index(name='count')

# Compare after sorting
pd.testing.assert_frame_equal(
    query11_sql.sort_values(by="manufacturer").reset_index(drop=True),
    query11_pandas.sort_values(by="manufacturer").reset_index(drop=True)
)

print("Query 11 passed: pandas and SQL results match.")


# %% [markdown]
# ### *Query 12 - Manufacturers with More Than 10 Planes*
# *Explanation:*
# This query retrieves manufacturers that have more than 10 planes.
# - SQL: SELECT manufacturer, COUNT(*) FROM planes GROUP BY manufacturer HAVING COUNT(*) > 10
# - pandas: Use groupby() and size() with a filter.

# %%
# SQL solution
query12_sql = pd.read_sql_query("""
    SELECT manufacturer, COUNT(*) FROM planes
    GROUP BY manufacturer HAVING COUNT(*) > 10
""", conn)

# Rename SQL column to match pandas
query12_sql = query12_sql.rename(columns={'COUNT(*)': 'count'})

# pandas solution
query12_pandas = planes.groupby('manufacturer').size().reset_index(name='count')
query12_pandas = query12_pandas[query12_pandas['count'] > 10]

# Compare after sorting
pd.testing.assert_frame_equal(
    query12_sql.sort_values(by="manufacturer").reset_index(drop=True),
    query12_pandas.sort_values(by="manufacturer").reset_index(drop=True)
)

print("Query 12 passed: pandas and SQL results match.")


# %% [markdown]
# ### *Query 13 - Manufacturers with More Than 10 Planes Having More Than 200 Seats*
# *Explanation:*
# This query retrieves manufacturers with more than 10 planes that have more than 200 seats.
# - SQL: SELECT manufacturer, COUNT(*) FROM planes WHERE seats > 200 GROUP BY manufacturer HAVING COUNT(*) > 10
# - pandas: Combine groupby(), size(), and a condition on both the seats and count.

# %%
# SQL solution
query13_sql = pd.read_sql_query("""
    SELECT manufacturer, COUNT(*) FROM planes
    WHERE seats > 200 GROUP BY manufacturer HAVING COUNT(*) > 10
""", conn)

# Rename SQL column to match pandas
query13_sql = query13_sql.rename(columns={'COUNT(*)': 'count'})

# pandas solution
query13_pandas = planes[planes['seats'] > 200].groupby('manufacturer').size().reset_index(name='count')
query13_pandas = query13_pandas[query13_pandas['count'] > 10]

# Compare after sorting
pd.testing.assert_frame_equal(
    query13_sql.sort_values(by="manufacturer").reset_index(drop=True),
    query13_pandas.sort_values(by="manufacturer").reset_index(drop=True)
)

print("Query 13 passed: pandas and SQL results match.")


# %% [markdown]
# ### *Query 14 - Top 10 Manufacturers by Plane Count*
# *Explanation:*
# This query retrieves the top 10 manufacturers with the most planes.
# - SQL: SELECT manufacturer, COUNT(*) AS howmany FROM planes GROUP BY manufacturer ORDER BY howmany DESC LIMIT 10
# - pandas: Use groupby(), size(), and sort_values().

# %%
query14_sql = pd.read_sql_query("""
    SELECT manufacturer, COUNT(*) AS howmany
    FROM planes
    GROUP BY manufacturer
    ORDER BY howmany DESC LIMIT 10
""", conn)

query14_pandas = planes.groupby('manufacturer').size().reset_index(name='howmany')
query14_pandas = query14_pandas.sort_values(by='howmany', ascending=False).head(10).reset_index(drop=True)

pd.testing.assert_frame_equal(query14_sql.sort_values(by="howmany", ascending=False).reset_index(drop=True),
                              query14_pandas.sort_values(by="howmany", ascending=False).reset_index(drop=True))

print("Query 14 passed: pandas and SQL results match.")

# %% [markdown]
# ### *Query 15 - Flights with Plane Details*
# *Explanation:*
# This query retrieves all flight data along with the year, speed, and seats of the corresponding planes.
# - SQL: SELECT flights.*, planes.year AS plane_year, planes.speed AS plane_speed, planes.seats AS plane_seats
# FROM flights LEFT JOIN planes ON flights.tailnum=planes.tailnum
# - pandas: Use merge() to join the dataframes.

# %%
# SQL version
query15_sql = pd.read_sql_query("""
    SELECT flights.*, planes.year AS plane_year, planes.speed AS plane_speed, planes.seats AS plane_seats
    FROM flights LEFT JOIN planes ON flights.tailnum=planes.tailnum
""", conn)

# Rename columns in planes BEFORE merge to prevent suffix issues
planes_subset = planes.rename(columns={
    'year': 'plane_year',
    'speed': 'plane_speed',
    'seats': 'plane_seats'
})[['tailnum', 'plane_year', 'plane_speed', 'plane_seats']]

# Perform the merge
query15_pandas = pd.merge(flights, planes_subset, on='tailnum', how='left')

# Compare after sorting
pd.testing.assert_frame_equal(
    query15_sql.sort_values(by="tailnum").reset_index(drop=True),
    query15_pandas.sort_values(by="tailnum").reset_index(drop=True),
    check_dtype=False  # Optional, helps avoid issues with minor dtype differences
)

print("Query 15 passed: pandas and SQL results match.")


# %% [markdown]
# ### *Query 16 - Planes and Airlines Information*
# *Explanation:*
# This query retrieves plane and airline details by first getting distinct carrier and tailnum combinations from flights, then joining with planes and airlines.
# - SQL: SELECT planes.*, airlines.* FROM (SELECT DISTINCT carrier, tailnum FROM flights) AS cartail
# INNER JOIN planes ON cartail.tailnum=planes.tailnum
# INNER JOIN airlines ON cartail.carrier=airlines.carrier
# - pandas: Use merge() twice.

# %%
# SQL version
query16_sql = pd.read_sql_query("""
    SELECT planes.*, airlines.* FROM
    (SELECT DISTINCT carrier, tailnum FROM flights) AS cartail
    INNER JOIN planes ON cartail.tailnum=planes.tailnum
    INNER JOIN airlines ON cartail.carrier=airlines.carrier
""", conn)

# Pandas version
cartail = flights[['carrier', 'tailnum']].drop_duplicates()
query16_pandas = pd.merge(cartail, planes, on='tailnum', how='inner')
query16_pandas = pd.merge(query16_pandas, airlines, on='carrier', how='inner')

# Reorder columns: planes.* followed by airlines.*
planes_cols = planes.columns.tolist()
airlines_cols = airlines.columns.tolist()
query16_pandas = query16_pandas[planes_cols + airlines_cols]

# Compare after sorting
pd.testing.assert_frame_equal(
    query16_sql.sort_values(by=["carrier", "tailnum"]).reset_index(drop=True),
    query16_pandas.sort_values(by=["carrier", "tailnum"]).reset_index(drop=True),
    check_dtype=False
)

print("Query 16 passed: pandas and SQL results match.")


# %%
# SQL version
query17_sql = pd.read_sql_query("""
    SELECT
    flights2.*,
    atemp,
    ahumid
    FROM (
        SELECT * FROM flights WHERE origin='EWR'
    ) AS flights2
    LEFT JOIN (
        SELECT
        year, month, day,
        AVG(temp) AS atemp,
        AVG(humid) AS ahumid
        FROM weather
        WHERE origin='EWR'
        GROUP BY year, month, day
    ) AS weather2
    ON flights2.year=weather2.year
    AND flights2.month=weather2.month
    AND flights2.day=weather2.day
""", conn)

# Pandas version
flights2 = flights[flights['origin'] == 'EWR']

weather2 = weather[weather['origin'] == 'EWR'].groupby(['year', 'month', 'day'], as_index=False).agg({
    'temp': 'mean',
    'humid': 'mean'
}).rename(columns={'temp': 'atemp', 'humid': 'ahumid'})

# Merge the two DataFrames on year, month, and day using a left join
query17_pandas = pd.merge(flights2, weather2, on=['year', 'month', 'day'], how='left')

# Sort and reset index before comparing
pd.testing.assert_frame_equal(query17_sql.sort_values(by="year").reset_index(drop=True),
                              query17_pandas.sort_values(by="year").reset_index(drop=True))

print("Query 17 passed: pandas and SQL results match.")

