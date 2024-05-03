import os
from datetime import datetime

import numpy as np
import pandas as pd
import json

from mysql.connector import connect


def extract_aggregated_insurance_country_data(dir_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Initialize an empty list to store extracted data
    extracted_data = []

    # Loop through each file in the directory
    for file_name in list_all_files:
        temp_dict_data = {}

        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)

        # Open and load the JSON file
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            # Extract required data
            data = data['data']
            # print(data)
            temp_dict_data['from_date'] = datetime.utcfromtimestamp(data['from'] / 1000).strftime('%Y-%m-%d')
            temp_dict_data['to_date'] = datetime.utcfromtimestamp(data['to'] / 1000).strftime('%Y-%m-%d')
            temp_dict_data['year'] = year
            temp_dict_data['transaction_name'] = data['transactionData'][0]['name']
            temp_dict_data['payment_instruments_type'] = data['transactionData'][0]['paymentInstruments'][0][
                'type']
            temp_dict_data['payment_instruments_count'] = data['transactionData'][0]['paymentInstruments'][0][
                'count']
            temp_dict_data['payment_instruments_amount'] = data['transactionData'][0]['paymentInstruments'][0][
                'amount']
            temp_dict_data["quarter"] = quarter


        # Append extracted data to the list
        extracted_data.append(temp_dict_data)

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(extracted_data)

    return df


def extract_aggregated_insurance_state_data(dir_name, state_name, year):
    # Initialize an empty list to store extracted data
    extracted_data = []

    # Loop through each file in the directory
    for file_name in os.listdir(dir_name):
        temp_dict_data = {}
        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)
        # print(file_path)
        # Check if the file is a JSON file
        if os.path.isfile(file_path) and file_name.endswith('.json'):
            # Open and load the JSON file
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)
                if file_name.startswith("1"):
                    quarter = "Q1"
                elif file_name.startswith("2"):
                    quarter = "Q2"
                elif file_name.startswith("3"):
                    quarter = "Q3"
                else:
                    quarter = "Q4"
                # Extract required data
                data = data['data']
                # print(data)
                temp_dict_data['from_date'] = datetime.utcfromtimestamp(data['from'] / 1000).strftime('%Y-%m-%d')
                temp_dict_data['to_date'] = datetime.utcfromtimestamp(data['to'] / 1000).strftime('%Y-%m-%d')
                if data['transactionData']:
                    temp_dict_data['year'] = year
                    temp_dict_data['state'] = state_name
                    temp_dict_data['transaction_name'] = data['transactionData'][0]['name']
                    temp_dict_data['payment_instruments_type'] = data['transactionData'][0]['paymentInstruments'][0][
                        'type']
                    temp_dict_data['payment_instruments_count'] = data['transactionData'][0]['paymentInstruments'][0][
                        'count']
                    temp_dict_data['payment_instruments_amount'] = data['transactionData'][0]['paymentInstruments'][0][
                        'amount']
                    temp_dict_data['quarter'] = quarter

                # Append extracted data to the list
                extracted_data.append(temp_dict_data)

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(extracted_data)

    return df


def extract_aggregated_transaction_country_data(dir_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Initialize an empty list to store extracted data
    extracted_data = []

    # Loop through each file in the directory
    for file_name in list_all_files:
        if file_name.startswith("1"):
            quarter = "Q1"
        elif file_name.startswith("2"):
            quarter = "Q2"
        elif file_name.startswith("3"):
            quarter = "Q3"
        else:
            quarter = "Q4"
        temp_dict_data = {}

        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)

        # Open and load the JSON file
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            # Extract required data
            data = data['data']
            temp_dict_data['from_date'] = datetime.utcfromtimestamp(data['from'] / 1000).strftime('%Y-%m-%d')
            temp_dict_data['to_date'] = datetime.utcfromtimestamp(data['to'] / 1000).strftime('%Y-%m-%d')
            temp_dict_data['year'] = year

            # Extract transaction data if available
            if 'transactionData' in data:
                for transaction in data['transactionData']:
                    temp_dict = temp_dict_data.copy()  # Make a copy of temp_dict_data for each transaction
                    temp_dict['transaction_name'] = transaction['name']
                    if 'paymentInstruments' in transaction:
                        for payment_instrument in transaction['paymentInstruments']:
                            temp_dict['payment_instruments_amount'] = payment_instrument['amount']
                            temp_dict['payment_instruments_type'] = payment_instrument['type']
                            temp_dict['payment_instruments_count'] = payment_instrument['count']
                            temp_dict["quarter"] = quarter
                            extracted_data.append(temp_dict)  # Append extracted data for each payment instrument

    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(extracted_data)

    return df


def extract_aggregated_transaction_state_data(dir_name, state_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Initialize an empty list to store extracted data
    extracted_data = []

    # Loop through each file in the directory
    for file_name in list_all_files:
        temp_dict_data = {}

        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)

        # Open and load the JSON file
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            # Extract required data
            data = data['data']
            temp_dict_data['from_date'] = datetime.utcfromtimestamp(data['from'] / 1000).strftime('%Y-%m-%d')
            temp_dict_data['to_date'] = datetime.utcfromtimestamp(data['to'] / 1000).strftime('%Y-%m-%d')
            temp_dict_data['year'] = year
            temp_dict_data['state'] = state_name
            # Extract transaction data if available
            if 'transactionData' in data:
                for transaction in data['transactionData']:
                    temp_dict = temp_dict_data.copy()  # Make a copy of temp_dict_data for each transaction
                    temp_dict['transaction_name'] = transaction['name']
                    if 'paymentInstruments' in transaction:
                        for payment_instrument in transaction['paymentInstruments']:
                            temp_dict['payment_instruments_amount'] = payment_instrument['amount']
                            temp_dict['payment_instruments_type'] = payment_instrument['type']
                            temp_dict['payment_instruments_count'] = payment_instrument['count']
                            temp_dict['quarter'] = quarter
                            extracted_data.append(temp_dict)  # Append extracted data for each payment instrument

    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(extracted_data)

    return df


def extract_aggregated_user_country_data(dir_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Initialize an empty list to store extracted data
    extracted_data = []

    # Loop through each file in the directory
    for file_name in list_all_files:
        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)

        # Open and load the JSON file
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            # Extract required data
            data = data['data']
            data_aggregated = data.get('aggregated', {})  # Use .get() to safely access dictionary keys
            registered_users = data_aggregated.get('registeredUsers', 0)  # Use .get() to safely access dictionary keys

            # Extract user by device data if available
            user_by_device = data.get('usersByDevice')
            if user_by_device is not None:  # Check if user_by_device is not None
                for item in user_by_device:
                    temp_dict_data = {}  # Create a new dictionary for each item
                    temp_dict_data['registered_users'] = registered_users
                    temp_dict_data['year'] = year
                    temp_dict_data['brand'] = item.get('brand')
                    temp_dict_data['count'] = item.get('count')
                    temp_dict_data['percentage'] = item.get('percentage')
                    temp_dict_data["quarter"] = quarter
                    extracted_data.append(temp_dict_data)

    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(extracted_data)
    return df


def extract_aggregated_user_state_data(dir_name, state_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Initialize an empty list to store extracted data
    extracted_data = []

    # Loop through each file in the directory
    for file_name in list_all_files:
        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)
        # Open and load the JSON file
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            # Extract required data
            data = data['data']
            data_aggregated = data.get('aggregated', {})  # Use .get() to safely access dictionary keys
            registered_users = data_aggregated.get('registeredUsers', 0)  # Use .get() to safely access dictionary keys

            # Extract user by device data if available
            user_by_device = data.get('usersByDevice')
            if user_by_device is not None:  # Check if user_by_device is not None
                for item in user_by_device:
                    temp_dict_data = {}  # Create a new dictionary for each item
                    temp_dict_data['registered_users'] = registered_users
                    temp_dict_data['state'] = state_name
                    temp_dict_data['year'] = year
                    temp_dict_data['brand'] = item.get('brand')
                    temp_dict_data['count'] = item.get('count')
                    temp_dict_data['percentage'] = item.get('percentage')
                    temp_dict_data['quarter'] = quarter
                    extracted_data.append(temp_dict_data)

    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(extracted_data)
    return df


def extract_map_insurance_country_data(dir_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Loop through each file in the directory
    final_data = []

    for file_name in list_all_files:
        # print(file_name)
        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)
        with open(file_path, "r") as f:
            data = json.load(f)
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            data = data["data"]["data"]
            lat_long_data = data["data"]
            # print(lat_long_data)
            for item in lat_long_data:
                temp_dict = {
                    "year": year, "quarter": quarter,
                    "lat": item[0], "lng": item[1], "metric": item[2], "state": item[3],
                }
                final_data.append(temp_dict)
    df = pd.DataFrame(final_data)
    return df


def extract_map_insurance_state_data(dir_name, state_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Loop through each file in the directory
    final_data = []
    for file_name in list_all_files:
        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)
        with open(file_path, "r") as f:
            data = json.load(f)
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            data = data["data"]["data"]
            lat_long_data = data["data"]

            # print(lat_long_data)
            for item in lat_long_data:
                temp_dict = {
                    "year": year, "quarter": quarter,
                    "lat": item[0], "lng": item[1], "metric": item[2], "state": state_name,
                    "district": item[3]
                }
                final_data.append(temp_dict)
    df = pd.DataFrame(final_data)
    return df


def extract_map_transaction_hover_country_data(dir_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Loop through each file in the directory
    for file_name in list_all_files:
        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)
        with open(file_path, "r") as f:
            data = json.load(f)
            data = data["data"]["hoverDataList"]
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            final_data = []
            # print(lat_long_data)
            for item in data:
                temp_dict = {
                    "year": year, "quarter": quarter, "state": item["name"], "type": item["metric"][0]["type"],
                    "count": item["metric"][0]["count"], "amount": item["metric"][0]["amount"]
                }
                final_data.append(temp_dict)
            df = pd.DataFrame(final_data)
            return df


def extract_map_transaction_hover_state_data(dir_name, state_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Loop through each file in the directory
    for file_name in list_all_files:
        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)
        with open(file_path, "r") as f:
            data = json.load(f)
            data = data["data"]["hoverDataList"]
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            final_data = []
            # print(lat_long_data)
            for item in data:
                temp_dict = {
                    "year": year, "quarter": quarter, "state": state_name, "district": item["name"], "type": item["metric"][0]["type"],
                    "count": item["metric"][0]["count"], "amount": item["metric"][0]["amount"]
                }
                final_data.append(temp_dict)
            df = pd.DataFrame(final_data)
            return df



def extract_map_user_hover_country_data(dir_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Loop through each file in the directory
    final_data = []
    for file_name in list_all_files:
        with open(dir_name+"/"+file_name, "r") as f:
            data = json.load(f)
            data = data["data"]["hoverData"]
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            for key, val in data.items():
                temp_dict = {
                    "year": year, "quarter": quarter, "state": key, "registered_users": val["registeredUsers"],
                    "app_opens": val["appOpens"],
                }
                final_data.append(temp_dict)
            df = pd.DataFrame(final_data)
            return df


def extract_map_user_hover_state_data(dir_name, state_name, year):
    # List all files in the directory
    list_all_files = os.listdir(dir_name)

    # Loop through each file in the directory
    for file_name in list_all_files:
        # Get the full path of the file
        file_path = os.path.join(dir_name, file_name)
        with open(file_path, "r") as f:
            data = json.load(f)
            if file_name.startswith("1"):
                quarter = "Q1"
            elif file_name.startswith("2"):
                quarter = "Q2"
            elif file_name.startswith("3"):
                quarter = "Q3"
            else:
                quarter = "Q4"
            final_data = []

            data = data["data"]
            data = data["hoverData"]
            for key, val in data.items():
                temp_dict = {
                    "year": year, "quarter": quarter, "state": state_name, "district": key,
                    "registered_users": val["registeredUsers"],
                    "app_opens": val["appOpens"],
                }
                final_data.append(temp_dict)
            df = pd.DataFrame(final_data)
            return df


def process_extract_map_user_hover_country_data(dir_path):
    # Get list of all directories
    # dir_path = '/home/saranrajgandhi/Guvi_Capestone_Projects/Phonepe_Pulse_Data_Visualization/phonepe_data_visualization/phonepe-data-viz/data/map/user/hover/country/india'
    all_directories = [os.path.join(dir_path, d) for d in os.listdir(dir_path) if
                       os.path.isdir(os.path.join(dir_path, d)) and d != 'state']

    # Initialize an empty list to store individual dataframes
    dfs = []

    # Iterate through each directory, extract data, and append to dfs list
    for dir_full_path in all_directories:
        year = dir_full_path.split('/')[-1]
        extracted_df = extract_map_user_hover_country_data(dir_full_path, year)
        dfs.append(extracted_df)

    # Concatenate all dataframes in dfs list into a single dataframe
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


def process_aggregated_and_map_data_state(state_dir_path):
    # Get list of all state directories
    # state_dir_path = '/home/saranrajgandhi/Guvi_Capestone_Projects/Phonepe_Pulse_Data_Visualization/phonepe_data_visualization/phonepe-data-viz/data/map/user/hover/country/india/state'
    state_directories = [os.path.join(state_dir_path, d) for d in os.listdir(state_dir_path) if
                         os.path.isdir(os.path.join(state_dir_path, d))]

    # Initialize an empty list to store individual dataframes
    dfs = []

    # Iterate through each state directory
    for state_dir in state_directories:
        # Get list of all year directories in the state directory
        year_directories = [os.path.join(state_dir, d) for d in os.listdir(state_dir) if
                            os.path.isdir(os.path.join(state_dir, d))]
        state_name = state_dir.split('/')[-1]
        # Iterate through each year directory
        for year_dir in year_directories:
            year = year_dir.split('/')[-1]
            # Extract data from JSON files in the year directory
            extracted_df = extract_map_user_hover_state_data(year_dir, state_name, year)
            dfs.append(extracted_df)

    # Concatenate all dataframes in dfs list into a single dataframe
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


def extract_top_insurance_country_data(country_directory):
    final_data = []
    for dir_path in country_directory:
        year = dir_path.split("/")[-1]
        list_files = os.listdir(dir_path)

        for file_name in list_files:
            with open(dir_path+"/"+file_name, "r") as f:
                file = file_name.split("/")[-1]
                data = json.load(f)
                data = data["data"]["states"]
                if file.startswith("1"):
                    quarter = "Q1"
                elif file.startswith("2"):
                    quarter = "Q2"
                elif file.startswith("3"):
                    quarter = "Q3"
                else:
                    quarter = "Q4"
                for item in data:
                    temp_data_dict = {"year": year, "quarter": quarter, "state": item["entityName"], "type": item["metric"]["type"],
                                      "count": item["metric"]["count"], "amount": item["metric"]["amount"]}
                    final_data.append(temp_data_dict)
    df = pd.DataFrame(final_data)
    return df


def process_extract_top_insurance_country_data():
    dir_path = "/home/saranrajgandhi/Guvi_Capestone_Projects/Phonepe_Pulse_Data_Visualization/phonepe_data_visualization/phonepe-data-viz/data/top/insurance/country/india"
    all_directories = [os.path.join(dir_path, d) for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d)) and d != 'state']
    data = extract_top_insurance_country_data(all_directories)
    return data


def extract_top_insurance_state_data(country_directory):
    final_data = []
    for state_dir in os.listdir(country_directory):
        state_path = os.path.join(country_directory, state_dir)
        if os.path.isdir(state_path):
            for year_dir in os.listdir(state_path):
                year_path = os.path.join(state_path, year_dir)
                if os.path.isdir(year_path):
                    for file_name in os.listdir(year_path):
                        if file_name.endswith('.json'):
                            with open(os.path.join(year_path, file_name), 'r') as f:
                                data = json.load(f)
                                state_name = state_dir.title()
                                year = year_dir
                                if file_name.startswith("1"):
                                    quarter = "Q1"
                                elif file_name.startswith("2"):
                                    quarter = "Q2"
                                elif file_name.startswith("3"):
                                    quarter = "Q3"
                                else:
                                    quarter = "Q4"
                                for item in data["data"]["districts"]:
                                    temp_data_dict = {
                                        "year": year,
                                        "quarter": quarter,
                                        "state": state_name,
                                        "district": item.get("entityName", {}),
                                        "type": item.get("metric", {}).get("type"),
                                        "count": item.get("metric", {}).get("count"),
                                        "amount": item.get("metric", {}).get("amount")
                                    }
                                    final_data.append(temp_data_dict)
    df = pd.DataFrame(final_data)
    return df


def process_extract_top_insurance_state_data():
    country_directory = "/home/saranrajgandhi/Guvi_Capestone_Projects/Phonepe_Pulse_Data_Visualization/phonepe_data_visualization/phonepe-data-viz/data/top/insurance/country/india/state"
    data = extract_top_insurance_state_data(country_directory)
    return data


def extract_top_transaction_country_data(country_directory):
    final_data = []
    for dir_path in country_directory:
        year = dir_path.split("/")[-1]
        list_files = os.listdir(dir_path)

        for file_name in list_files:
            with open(dir_path+"/"+file_name, "r") as f:
                file = file_name.split("/")[-1]
                data = json.load(f)
                data = data["data"]["states"]
                if file.startswith("1"):
                    quarter = "Q1"
                elif file.startswith("2"):
                    quarter = "Q2"
                elif file.startswith("3"):
                    quarter = "Q3"
                else:
                    quarter = "Q4"
                for item in data:
                    temp_data_dict = {"year": year, "quarter": quarter, "state": item["entityName"], "type": item["metric"]["type"],
                                      "count": item["metric"]["count"], "amount": item["metric"]["amount"]}
                    final_data.append(temp_data_dict)
    df = pd.DataFrame(final_data)
    return df


def process_extract_top_transaction_country_data():
    dir_path = "/home/saranrajgandhi/Guvi_Capestone_Projects/Phonepe_Pulse_Data_Visualization/phonepe_data_visualization/phonepe-data-viz/data/top/transaction/country/india"
    all_directories = [os.path.join(dir_path, d) for d in os.listdir(dir_path) if
                       os.path.isdir(os.path.join(dir_path, d)) and d != 'state']
    data = extract_top_transaction_country_data(all_directories)
    return data


def extract_top_transaction_state_data(country_directory):
    final_data = []
    for state_dir in os.listdir(country_directory):
        state_path = os.path.join(country_directory, state_dir)
        if os.path.isdir(state_path):
            for year_dir in os.listdir(state_path):
                year_path = os.path.join(state_path, year_dir)
                if os.path.isdir(year_path):
                    for file_name in os.listdir(year_path):
                        if file_name.endswith('.json'):
                            with open(os.path.join(year_path, file_name), 'r') as f:
                                data = json.load(f)
                                state_name = state_dir.title()
                                year = year_dir
                                if file_name.startswith("1"):
                                    quarter = "Q1"
                                elif file_name.startswith("2"):
                                    quarter = "Q2"
                                elif file_name.startswith("3"):
                                    quarter = "Q3"
                                else:
                                    quarter = "Q4"
                                for item in data["data"]["districts"]:
                                    temp_data_dict = {
                                        "year": year,
                                        "quarter": quarter,
                                        "state": state_name,
                                        "district": item.get("entityName", {}),
                                        "type": item.get("metric", {}).get("type"),
                                        "count": item.get("metric", {}).get("count"),
                                        "amount": item.get("metric", {}).get("amount")
                                    }
                                    final_data.append(temp_data_dict)
    df = pd.DataFrame(final_data)
    return df


def process_extract_top_transaction_state_data():
    country_directory = "/home/saranrajgandhi/Guvi_Capestone_Projects/Phonepe_Pulse_Data_Visualization/phonepe_data_visualization/phonepe-data-viz/data/top/transaction/country/india/state"
    data = extract_top_transaction_state_data(country_directory)
    return data


def extract_to_user_country_data(country_directory):
    final_data = []
    for dir_path in country_directory:
        year = dir_path.split("/")[-1]
        list_files = os.listdir(dir_path)

        for file_name in list_files:
            with open(dir_path+"/"+file_name, "r") as f:
                file = file_name.split("/")[-1]
                data = json.load(f)
                data = data["data"]["states"]
                if file.startswith("1"):
                    quarter = "Q1"
                elif file.startswith("2"):
                    quarter = "Q2"
                elif file.startswith("3"):
                    quarter = "Q3"
                else:
                    quarter = "Q4"
                for item in data:
                    # print(data)
                    temp_data_dict = {"year": year, "quarter": quarter, "state": item["name"],
                                      "registered_users": item["registeredUsers"]}
                    final_data.append(temp_data_dict)
    df = pd.DataFrame(final_data)
    return df


def process_extract_top_user__country_data():
    dir_path = "/home/saranrajgandhi/Guvi_Capestone_Projects/Phonepe_Pulse_Data_Visualization/phonepe_data_visualization/phonepe-data-viz/data/top/user/country/india"
    all_directories = [os.path.join(dir_path, d) for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d)) and d != 'state']
    data = extract_to_user_country_data(all_directories)
    return data


def extract_top_user_state_data(country_directory):
    final_data = []
    for state_dir in os.listdir(country_directory):
        state_path = os.path.join(country_directory, state_dir)
        if os.path.isdir(state_path):
            for year_dir in os.listdir(state_path):
                year_path = os.path.join(state_path, year_dir)
                if os.path.isdir(year_path):
                    for file_name in os.listdir(year_path):
                        if file_name.endswith('.json'):
                            with open(os.path.join(year_path, file_name), 'r') as f:
                                data = json.load(f)
                                state_name = state_dir.title()
                                year = year_dir
                                if file_name.startswith("1"):
                                    quarter = "Q1"
                                elif file_name.startswith("2"):
                                    quarter = "Q2"
                                elif file_name.startswith("3"):
                                    quarter = "Q3"
                                else:
                                    quarter = "Q4"
                                for item in data["data"]["districts"]:
                                    # print(data)
                                    temp_data_dict = {"year": year, "quarter": quarter, "state":  state_name,
                                                      "district": item["name"],
                                                      "registered_users": item["registeredUsers"]}
                                    final_data.append(temp_data_dict)
    df = pd.DataFrame(final_data)
    return df


def process_extract_top_user_state_data():
    country_directory = "/home/saranrajgandhi/Guvi_Capestone_Projects/Phonepe_Pulse_Data_Visualization/phonepe_data_visualization/phonepe-data-viz/data/top/user/country/india/state"
    data = extract_top_insurance_state_data(country_directory)
    return data


# --- Data Insertion to MySql using mysql-python-connector
def insert_data_into_mysql_table(data, table_name, config):
    try:
        # Create the MySQL connection
        conn = connect(
            host=config["host"],
            user=config["user"],
            passwd=config["password"],
            database=config["database"]
        )

        cursor = conn.cursor()

        # Fetch column names from the table
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = cursor.fetchall()
        column_names = [col[0] for col in columns if col[0] in data.columns]

        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in data.values]

        # Generate placeholders for values in the insert query
        placeholders = ', '.join(['%s'] * len(column_names))

        # Define the insert query
        insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"

        # Execute the insert query
        cursor.executemany(insert_query, data_tuples)

        # Commit the transaction
        conn.commit()

        # Close cursor and connection
        cursor.close()
        conn.close()

        return 1  # Indicate success
    except Exception as e:
        print("Error:", e)
        return 0  # Indicate failure


def process_insertion(data, config):
    insert_data_into_mysql_table(data, "aggregated_insurance_state_data", config)
    return 1
