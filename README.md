# dbflowix

## Overview

Dbflowix is a Python-based project that appears to be focused on data integration and analysis, specifically with regards to store visits and customer behavior. The project utilizes the pandas library for data manipulation and the requests library for making API calls.

## Features
Retrieves store visit data from an API
Renames and formats the data for easier analysis
Divides the data into separate DataFrames for stores, companies, timezones, and segments
Loads column name mappings from a JSON file

## Requirements
Python 3.x
pandas
requests
json

## Installation

To install the required libraries, run the following command: `pip install -r requirements.txt`

## Usage

The main entry point of the project is the app.py file. To run the project, execute the following command: `python app.py`

## Configuration

The project uses **environment** variables to store API keys and other sensitive information. To configure the project, set the following environment variables:

accept
Content_Type
x_api_key

## Contributing

Contributions are welcome! To contribute to the project, please fork the repository and submit a pull request with your changes.

### License

The project is licensed under the MIT License. See the LICENSE file for more information.