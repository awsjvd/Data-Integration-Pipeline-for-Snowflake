# Python ETL Pipeline for Employee Data

## Table of Contents
- [Introduction](#introduction)
- [Problem Statement](#problem-statement)
- [About the Challenge](#about-the-challenge)
- [Solution](#solution)

## Introduction
This project involves building a simple **ETL pipeline** that reads employee data from an Excel file, transforms it, and then loads it into a **Snowflake** database for further analysis. The ETL process is performed using Python's `pandas` and `snowflake-connector` libraries. 

The data, originally stored in an Excel file, contains employee details such as name, department, hire date, and salary. The goal of this pipeline is to:
1. Extract the employee data from the Excel file.
2. Transform the data by removing unnecessary columns.
3. Load the cleaned data into a **Snowflake** database for future analysis.

## Problem Statement
The problem is to automate the process of:
1. Extracting employee data from an Excel file.
2. Cleaning the data by dropping unnecessary columns.
3. Loading the transformed data into Snowflake for analysis.

The objective is to create a reliable and automated ETL process that will help the business team to easily work with the employee data in Snowflake. The solution will also handle the connection between Python and Snowflake, creating necessary tables and loading data efficiently.

## About the Challenge
The project focuses on reading an Excel sheet containing employee details, performing some data preprocessing (such as dropping columns that aren't necessary), and loading the cleaned data into a Snowflake table. 

In the solution, the following steps are performed:

1. **Data Extraction**: Read the employee data from an Excel file using `pandas.read_excel()`.
2. **Data Transformation**: Drop unnecessary columns such as 'Job Rating', 'New Salary', 'Tax Rate', and others from the employee data.
3. **Data Loading**: Load the cleaned employee data into Snowflake using the `snowflake.connector` library and the `write_pandas` function.

## Solution

### Steps:

1. **Extract Data**: 
   The data is extracted from an Excel file (`H+ Sport Employees.xlsx`) that contains employee records in a sheet called 'Emp'.

2. **Transform Data**: 
   After loading the data into a Pandas DataFrame, unnecessary columns are dropped, such as 'Job Rating', 'New Salary', 'Tax Rate', etc.

3. **Load Data**: 
   The cleaned data is then loaded into a Snowflake table named `test` in the `FINANCE` schema.

The code handles establishing a connection to Snowflake, creating the table (if not already created), and inserting the data into the table.

