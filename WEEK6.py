# ---
# jupyter:
#   jupytext:
#     formats: py:percent
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

# %%
# %% [markdown]
# # SIT731 Task 6D: Pandas vs SQL
# Name: Your Name  
# Student Number: 123456789  
# Email: your.email@example.com  
# Program: SIT731 - Postgraduate

# %% [markdown]
# ## Introduction
# This notebook compares SQL queries to equivalent pandas solutions using the nycflights13 dataset.
# The aim is to demonstrate understanding of data manipulation using pandas and SQL.

# %%
import pandas as pd
import sqlite3

# Create SQLite connection
conn = sqlite3.connect("flights.db")

# %%
# Load CSVs into pandas
flights = pd.read_csv("flights.csv", comment="#")
airlines = pd.read_csv("airlines.csv", comment="#")
airports = pd.read_csv("airports.csv", comment="#")
planes = pd.read_csv("planes.csv", comment="#")
weather = pd.read_csv("weather.csv", comment="#")

# Write to SQLite
flights.to_sql("flights", conn, if_exists="replace", index=False)
airlines.to_sql("airlines", conn, if_exists="replace", index=False)
airports.to_sql("airports", conn, if_exists="replace", index=False)
planes.to_sql("planes", conn, if_exists="replace", index=False)
weather.to_sql("weather", conn, if_exists="replace", index=False)

# %%

task1_sql = pd.read_sql_query("SELECT DISTINCT engine FROM planes", conn)
task1_my = planes[["engine"]].drop_duplicates()
print(task1_my)
pd.testing.assert_frame_equal(task1_sql.sort_values(by="engine").reset_index(drop=True), task1_my.sort_values(by="engine").reset_index(drop=True))

# %%
# %% [markdown]
# ## Task 2
# `SELECT DISTINCT type, engine FROM planes`
# %%
task2_sql = pd.read_sql_query("SELECT DISTINCT type, engine FROM planes", conn)
task2_my = planes[["type", "engine"]].drop_duplicates()
print(task2_my)
pd.testing.assert_frame_equal(task2_sql.sort_values(by=["type", "engine"]).reset_index(drop=True), task2_my.sort_values(by=["type", "engine"]).reset_index(drop=True))

# %%
