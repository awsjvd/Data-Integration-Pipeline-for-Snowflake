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
2. Clean and transform the data by removing unnecessary columns and cleaning up names.
3. Load the cleaned data into a **Snowflake** database for future analysis.

## Problem Statement
The challenge is to automate the process of:
1. Extracting employee data from an Excel file.
2. Cleaning the data by dropping unnecessary columns and formatting employee names.
3. Loading the transformed data into Snowflake for analysis.

The objective is to create a reliable and automated ETL process that will help the business team easily access and analyze the employee data in Snowflake. The solution also handles the connection between Python and Snowflake, dynamically creating necessary tables and efficiently loading data.

## About the Challenge
The project focuses on reading an Excel sheet containing employee details, performing data preprocessing (such as dropping unnecessary columns and cleaning up data), and loading the cleaned data into a Snowflake table. 

The following steps are performed as part of the solution:

1. **Data Extraction**: Read the employee data from an Excel file using `pandas.read_excel()`.
2. **Data Transformation**: 
   - Drop unnecessary columns such as 'Job Rating', 'New Salary', and 'Tax Rate'.
   - Clean the `Employee Name` column by removing non-ASCII characters, commas, and extra spaces.
3. **Data Loading**: Load the cleaned employee data into Snowflake using the `snowflake.connector` library and the `write_pandas` function.

## Solution

### Steps:

1. **Extract Data**:
   The data is extracted from an Excel file specified in an environment variable (`EMPLOYEE_FILE_PATH`) that contains employee records in a sheet named 'Emp'.

2. **Transform Data**:
   After loading the data into a Pandas DataFrame, unnecessary columns are dropped, and the `Employee Name` column is cleaned to ensure consistency and readability.

3. **Load Data**:
   The cleaned data is loaded into a Snowflake table named `Employees` in the specified schema and database. The code:
   - Establishes a connection to Snowflake using environment variables for credentials.
   - Creates the table (if not already created) with the appropriate schema.
   - Uses the `write_pandas` function to efficiently load the data into the Snowflake table.

---

### **Key Features**:
- Automates the ETL process using Python.
- Dynamically creates the required Snowflake table if it doesn't exist.


