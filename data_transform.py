import os
import json
import pandas as pd
import shutil
import git
import mysql.connector

# Get the current directory by using '.'
current_directory = '.'

# Define the path where you want to store the cloned data
clone_folder = 'pulse_repository'

# Clone the GitHub repository
repo_url = 'https://github.com/phonepe/pulse.git'

if clone_folder in os.listdir(current_directory):
    shutil.rmtree(clone_folder)

git.Repo.clone_from(repo_url, clone_folder)

# Aggregated
# Transaction
# All India
aggregated_dir_transaction_AllIndia = 'pulse_repository/data/aggregated/transaction/country/india/'

aggregatedTransactionState_Data_AllIndia = {'Year': [], 'Quarter': [], 'Transaction_type': [],
                                            'Transaction_count': [],
                                            'Transaction_amount': []}

# Get a list of all subfolders in the source folder
subfolders = [subfolder for subfolder in os.listdir(aggregated_dir_transaction_AllIndia) if
              os.path.isdir(os.path.join(aggregated_dir_transaction_AllIndia, subfolder))]

# Filter and print the year folders
year_folders = [subfolder for subfolder in subfolders if subfolder.isdigit() and len(subfolder) == 4]

for year in year_folders:
    year_path = os.path.join(aggregated_dir_transaction_AllIndia, year)
    year_path_format = year_path + "/"
    aggregated_year_list = os.listdir(year_path)

    for quartersFile in aggregated_year_list:
        quartersFile_path = year_path_format + quartersFile
        Data = open(quartersFile_path, 'r')
        quartersFile_data = json.load(Data)

        for transaction_Data in quartersFile_data['data']['transactionData']:
            Name = transaction_Data['name']
            count = transaction_Data['paymentInstruments'][0]['count']
            amount = transaction_Data['paymentInstruments'][0]['amount']
            aggregatedTransactionState_Data_AllIndia['Year'].append(year)
            aggregatedTransactionState_Data_AllIndia['Quarter'].append(int(quartersFile.strip('.json')))
            aggregatedTransactionState_Data_AllIndia['Transaction_type'].append(Name)
            aggregatedTransactionState_Data_AllIndia['Transaction_count'].append(count)
            aggregatedTransactionState_Data_AllIndia['Transaction_amount'].append(amount)

df_aggregated_transaction_AllIndia = pd.DataFrame(aggregatedTransactionState_Data_AllIndia)
# State

# Base directory for the JSON files
aggregated_dir_transaction_state = 'pulse_repository/data/aggregated/transaction/country/india/state/'

aggregatedTransactionState = os.listdir(aggregated_dir_transaction_state)

aggregatedTransactionState_Data = {'State': [], 'Year': [], 'Quarter': [], 'Transaction Type': [],
                                   'Transaction Count': [],
                                   'Transaction Amount': []}

for states in aggregatedTransactionState:
    states_path = aggregated_dir_transaction_state + states + "/"
    aggregated_year = os.listdir(states_path)

    for year in aggregated_year:
        year_path = states_path + year + "/"
        aggregated_year_list = os.listdir(year_path)

        for quartersFile in aggregated_year_list:
            quartersFile_path = year_path + quartersFile
            Data = open(quartersFile_path, 'r')
            quartersFile_data = json.load(Data)

            for transaction_Data in quartersFile_data['data']['transactionData']:
                Name = transaction_Data['name']
                count = transaction_Data['paymentInstruments'][0]['count']
                amount = transaction_Data['paymentInstruments'][0]['amount']
                aggregatedTransactionState_Data['State'].append(states)
                aggregatedTransactionState_Data['Year'].append(year)
                aggregatedTransactionState_Data['Quarter'].append(int(quartersFile.strip('.json')))
                aggregatedTransactionState_Data['Transaction Type'].append(Name)
                aggregatedTransactionState_Data['Transaction Count'].append(count)
                aggregatedTransactionState_Data['Transaction Amount'].append(amount)

df_aggregated_transaction_state = pd.DataFrame(aggregatedTransactionState_Data)
# Users

aggregated_dir_User_AllIndia = 'pulse_repository/data/aggregated/user/country/india/'

aggregatedUser_Data_AllIndia = {'Year': [], 'Quarter': [], 'Brands': [], 'User Count': [], 'User Percentage': []}

for year in year_folders:
    year_path = os.path.join(aggregated_dir_User_AllIndia, year)
    year_path_format = year_path + "/"
    aggregated_year_list = os.listdir(year_path)

    for quartersFile in aggregated_year_list:
        quartersFile_path = year_path_format + quartersFile
        Data = open(quartersFile_path, 'r')
        quartersFile_data = json.load(Data)

        try:
            for user_Data in quartersFile_data["data"]["usersByDevice"]:
                brand_name = user_Data["brand"]
                count_ = user_Data["count"]
                ALL_percentage = user_Data["percentage"]
                aggregatedUser_Data_AllIndia["Year"].append(year)
                aggregatedUser_Data_AllIndia["Quarter"].append(int(quartersFile.strip('.json')))
                aggregatedUser_Data_AllIndia["Brands"].append(brand_name)
                aggregatedUser_Data_AllIndia["User Count"].append(count_)
                aggregatedUser_Data_AllIndia["User Percentage"].append(ALL_percentage * 100)
        except Exception as e:
            pass
df_aggregated_user_AllIndia = pd.DataFrame(aggregatedUser_Data_AllIndia)

# States

aggregated_dir_User_state = 'pulse_repository/data/aggregated/user/country/india/state/'
aggregatedUserState = os.listdir(aggregated_dir_User_state)

aggregatedUser_Data_State = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [],
                             'User_Percentage': []}

for State in aggregatedUserState:
    State_Path = aggregated_dir_User_state + State + "/"
    aggregated_Year = os.listdir(State_Path)

    for year in aggregated_Year:
        year_Path = State_Path + year + "/"
        aggregated_year_list = os.listdir(year_Path)

        for quartersFile in aggregated_year_list:
            quartersFile_path = year_Path + quartersFile
            Data = open(quartersFile_path, 'r')
            quartersFile_data = json.load(Data)

            try:
                for User_Data in quartersFile_data["data"]["usersByDevice"]:
                    brand_name = User_Data["brand"]
                    count_ = User_Data["count"]
                    ALL_percentage = User_Data["percentage"]
                    aggregatedUser_Data_State["State"].append(State)
                    aggregatedUser_Data_State["Year"].append(year)
                    aggregatedUser_Data_State["Quarter"].append(int(quartersFile.strip('.json')))
                    aggregatedUser_Data_State["Brands"].append(brand_name)
                    aggregatedUser_Data_State["User_Count"].append(count_)
                    aggregatedUser_Data_State["User_Percentage"].append(ALL_percentage * 100)
            except:
                pass

df_aggregated_user_state = pd.DataFrame(aggregatedUser_Data_State)

# MAP
# Transaction
# All India
map_dir_transaction_AllIndia = 'pulse_repository/data/map/transaction/hover/country/india/'

mapTransactionState_Data_AllIndia = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_Count': [],
                                     'Transaction_Amount': []}

for year in year_folders:
    year_path = os.path.join(map_dir_transaction_AllIndia, year)
    year_path_format = year_path + "/"
    map_year_list = os.listdir(year_path)

    for quartersFile in map_year_list:
        quartersFile_path = year_path_format + quartersFile
        Data = open(quartersFile_path, 'r')
        quartersFile_data = json.load(Data)

        for transaction_Data in quartersFile_data["data"]["hoverDataList"]:
            State = transaction_Data["name"]
            count = transaction_Data["metric"][0]["count"]
            amount = transaction_Data["metric"][0]["amount"]
            mapTransactionState_Data_AllIndia['Year'].append(year)
            mapTransactionState_Data_AllIndia['Quarter'].append(int(quartersFile.strip('.json')))
            mapTransactionState_Data_AllIndia["State"].append(State)
            mapTransactionState_Data_AllIndia["Transaction_Count"].append(count)
            mapTransactionState_Data_AllIndia["Transaction_Amount"].append(amount)

df_map_transaction_AllIndia = pd.DataFrame(mapTransactionState_Data_AllIndia)

# MAP
# Transaction
# State
map_dir_transaction_State = 'pulse_repository/data/map/transaction/hover/country/india/state/'
maptransactionState = os.listdir(map_dir_transaction_State)

mapTransactionState_Data = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_Count': [],
                            'Transaction_Amount': []}

for states in maptransactionState:
    states_path = map_dir_transaction_State + states + "/"
    map_year = os.listdir(states_path)

    for year in map_year:
        year_path = states_path + year + "/"
        aggregated_year_list = os.listdir(year_path)

        for quartersFile in aggregated_year_list:
            quartersFile_path = year_path + quartersFile
            Data = open(quartersFile_path, 'r')
            quartersFile_data = json.load(Data)

            for transaction_Data in quartersFile_data["data"]["hoverDataList"]:
                District = transaction_Data["name"]
                count = transaction_Data["metric"][0]["count"]
                amount = transaction_Data["metric"][0]["amount"]
                mapTransactionState_Data['State'].append(states)
                mapTransactionState_Data['Year'].append(year)
                mapTransactionState_Data['Quarter'].append(int(quartersFile.strip('.json')))
                mapTransactionState_Data["District"].append(District)
                mapTransactionState_Data["Transaction_Count"].append(count)
                mapTransactionState_Data["Transaction_Amount"].append(amount)

df_map_transaction_state = pd.DataFrame(mapTransactionState_Data)

# Users - All India
# Users

map_dir_User_AllIndia = 'pulse_repository/data/map/user/hover/country/india/'

map_Data_AllIndia = {"State": [], "Year": [], "Quarter": [], "Registered_User": []}

for year in year_folders:
    year_path = os.path.join(map_dir_User_AllIndia, year)
    year_path_format = year_path + "/"
    map_year_list = os.listdir(year_path)

    for quartersFile in map_year_list:
        quartersFile_path = year_path_format + quartersFile
        Data = open(quartersFile_path, 'r')
        quartersFile_data = json.load(Data)

        try:
            for user_Data in quartersFile_data["data"]["hoverData"].items():
                State = user_Data[0]
                registered_user = user_Data[1]["registeredUsers"]
                map_Data_AllIndia['Year'].append(year)
                map_Data_AllIndia['Quarter'].append(int(quartersFile.strip('.json')))
                map_Data_AllIndia["State"].append(State)
                map_Data_AllIndia["Registered_User"].append(registered_user)
        except Exception as e:
            pass
df_map_user_AllIndia = pd.DataFrame(map_Data_AllIndia)

# States

map_dir_User_state = 'pulse_repository/data/map/user/hover/country/india/state/'
mapUserState = os.listdir(map_dir_User_state)

mapUser_Data_State = {"State": [], "Year": [], "Quarter": [], "District": [], "Registered_User": []}

for State in mapUserState:
    State_Path = map_dir_User_state + State + "/"
    aggregated_Year = os.listdir(State_Path)

    for year in aggregated_Year:
        year_Path = State_Path + year + "/"
        aggregated_year_list = os.listdir(year_Path)

        for quartersFile in aggregated_year_list:
            quartersFile_path = year_Path + quartersFile
            Data = open(quartersFile_path, 'r')
            quartersFile_data = json.load(Data)

            try:
                for User_Data in quartersFile_data["data"]["hoverData"].items():
                    district = User_Data[0]
                    register_user = User_Data[1]["registeredUsers"]
                    mapUser_Data_State['State'].append(State)
                    mapUser_Data_State['Year'].append(year)
                    mapUser_Data_State['Quarter'].append(int(quartersFile.strip('.json')))
                    mapUser_Data_State["District"].append(district)
                    mapUser_Data_State["Registered_User"].append(register_user)
            except:
                pass

df_map_user_state = pd.DataFrame(mapUser_Data_State)

# Top
# Transaction
# All India
top_dir_transaction_AllIndia = 'pulse_repository/data/top/transaction/country/india/'

topTransactionState_Data_AllIndia = {'Year': [], 'Quarter': [], 'State': [], 'Transaction_count': [],
                                     'Transaction_amount': []}
topTransactionDistrict_Data_AllIndia = {'Year': [], 'Quarter': [], 'District': [], 'Transaction_count': [],
                                        'Transaction_amount': []}
topTransactionPincode_Data_AllIndia = {'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],
                                       'Transaction_amount': []}

for year in year_folders:
    year_path = os.path.join(top_dir_transaction_AllIndia, year)
    year_path_format = year_path + "/"
    top_year_list = os.listdir(year_path)

    for quartersFile in top_year_list:
        quartersFile_path = year_path_format + quartersFile
        Data = open(quartersFile_path, 'r')
        quartersFile_data = json.load(Data)

        for transaction_Data in quartersFile_data['data']['states']:
            Name = transaction_Data['entityName']
            count = transaction_Data['metric']['count']
            amount = transaction_Data['metric']['amount']
            topTransactionState_Data_AllIndia['Year'].append(year)
            topTransactionState_Data_AllIndia['Quarter'].append(int(quartersFile.strip('.json')))
            topTransactionState_Data_AllIndia['State'].append(Name)
            topTransactionState_Data_AllIndia['Transaction_count'].append(count)
            topTransactionState_Data_AllIndia['Transaction_amount'].append(amount)

        for transaction_Data in quartersFile_data['data']['districts']:
            Name = transaction_Data['entityName']
            count = transaction_Data['metric']['count']
            amount = transaction_Data['metric']['amount']
            topTransactionDistrict_Data_AllIndia['Year'].append(year)
            topTransactionDistrict_Data_AllIndia['Quarter'].append(int(quartersFile.strip('.json')))
            topTransactionDistrict_Data_AllIndia['District'].append(Name)
            topTransactionDistrict_Data_AllIndia['Transaction_count'].append(count)
            topTransactionDistrict_Data_AllIndia['Transaction_amount'].append(amount)

        for transaction_Data in quartersFile_data['data']['pincodes']:
            Name = transaction_Data['entityName']
            count = transaction_Data['metric']['count']
            amount = transaction_Data['metric']['amount']
            topTransactionPincode_Data_AllIndia['Year'].append(year)
            topTransactionPincode_Data_AllIndia['Quarter'].append(int(quartersFile.strip('.json')))
            topTransactionPincode_Data_AllIndia['Pincode'].append(Name)
            topTransactionPincode_Data_AllIndia['Transaction_count'].append(count)
            topTransactionPincode_Data_AllIndia['Transaction_amount'].append(amount)
df_top_transactionState_AllIndia = pd.DataFrame(topTransactionState_Data_AllIndia)
df_top_transactionDistrict_AllIndia = pd.DataFrame(topTransactionDistrict_Data_AllIndia)
df_top_transactionPinCode_AllIndia = pd.DataFrame(topTransactionPincode_Data_AllIndia)

# Transaction
# State
top_dir_transaction_state = 'pulse_repository/data/top/transaction/country/india/state/'

topTransactionState = os.listdir(top_dir_transaction_state)
topTransactionDistrict_Data_State = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_count': [],
                                     'Transaction_amount': []}
topTransactionPincode_Data_State = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],
                                    'Transaction_amount': []}

for states in topTransactionState:
    states_path = top_dir_transaction_state + states + "/"
    map_year = os.listdir(states_path)

    for year in map_year:
        year_path = states_path + year + "/"
        top_year_list = os.listdir(year_path)

        for quartersFile in top_year_list:
            quartersFile_path = year_path + quartersFile
            Data = open(quartersFile_path, 'r')
            quartersFile_data = json.load(Data)

            for transaction_Data in quartersFile_data['data']['districts']:
                Name = transaction_Data['entityName']
                count = transaction_Data['metric']['count']
                amount = transaction_Data['metric']['amount']
                topTransactionDistrict_Data_State['State'].append(states)
                topTransactionDistrict_Data_State['Year'].append(year)
                topTransactionDistrict_Data_State['Quarter'].append(int(quartersFile.strip('.json')))
                topTransactionDistrict_Data_State['District'].append(Name)
                topTransactionDistrict_Data_State['Transaction_count'].append(count)
                topTransactionDistrict_Data_State['Transaction_amount'].append(amount)

            for transaction_Data in quartersFile_data['data']['pincodes']:
                Name = transaction_Data['entityName']
                count = transaction_Data['metric']['count']
                amount = transaction_Data['metric']['amount']
                topTransactionPincode_Data_State['State'].append(states)
                topTransactionPincode_Data_State['Year'].append(year)
                topTransactionPincode_Data_State['Quarter'].append(int(quartersFile.strip('.json')))
                topTransactionPincode_Data_State['Pincode'].append(Name)
                topTransactionPincode_Data_State['Transaction_count'].append(count)
                topTransactionPincode_Data_State['Transaction_amount'].append(amount)

df_top_transactionDistrict_State = pd.DataFrame(topTransactionDistrict_Data_State)
df_top_transactionPinCode_State = pd.DataFrame(topTransactionPincode_Data_State)
df_top_transactionPinCode_State = df_top_transactionPinCode_State.dropna()
# User
# All India
top_dir_User_AllIndia = 'pulse_repository/data/top/user/country/india/'

topUserState_Data_AllIndia = {'Year': [], 'Quarter': [], 'State': [], 'Registered_User': []}
topUserDistrict_Data_AllIndia = {'Year': [], 'Quarter': [], 'District': [], 'Registered_User': []}
topUserPincode_Data_AllIndia = {'Year': [], 'Quarter': [], 'Pincode': [], 'Registered_User': []}

for year in year_folders:
    year_path = os.path.join(top_dir_User_AllIndia, year)
    year_path_format = year_path + "/"
    top_year_list = os.listdir(year_path)

    for quartersFile in top_year_list:
        quartersFile_path = year_path_format + quartersFile
        Data = open(quartersFile_path, 'r')
        quartersFile_data = json.load(Data)

        for top_Data in quartersFile_data['data']['states']:
            Name = top_Data['name']
            registeredUser = top_Data['registeredUsers']
            topUserState_Data_AllIndia['Year'].append(year)
            topUserState_Data_AllIndia['Quarter'].append(int(quartersFile.strip('.json')))
            topUserState_Data_AllIndia['State'].append(Name)
            topUserState_Data_AllIndia['Registered_User'].append(registeredUser)

        for top_Data in quartersFile_data['data']['districts']:
            Name = top_Data['name']
            registeredUser = top_Data['registeredUsers']
            topUserDistrict_Data_AllIndia['Year'].append(year)
            topUserDistrict_Data_AllIndia['Quarter'].append(int(quartersFile.strip('.json')))
            topUserDistrict_Data_AllIndia['District'].append(Name)
            topUserDistrict_Data_AllIndia['Registered_User'].append(registeredUser)

        for top_Data in quartersFile_data['data']['pincodes']:
            Name = top_Data['name']
            registeredUser = top_Data['registeredUsers']
            topUserPincode_Data_AllIndia['Year'].append(year)
            topUserPincode_Data_AllIndia['Quarter'].append(int(quartersFile.strip('.json')))
            topUserPincode_Data_AllIndia['Pincode'].append(Name)
            topUserPincode_Data_AllIndia['Registered_User'].append(registeredUser)

df_top_UserState_AllIndia = pd.DataFrame(topUserState_Data_AllIndia)
df_top_UserDistrict_AllIndia = pd.DataFrame(topUserDistrict_Data_AllIndia)
df_top_UserPinCode_AllIndia = pd.DataFrame(topUserPincode_Data_AllIndia)

# states
top_dir_User_state = 'pulse_repository/data/top/user/country/india/state/'
topuserState = os.listdir(top_dir_User_state)
topUserDistrict_Data_state = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Registered_User': []}
topUserPincode_Data_state = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Registered_User': []}

for states in topuserState:
    states_path = top_dir_User_state + states + "/"
    top_year = os.listdir(states_path)

    for year in top_year:
        year_path = states_path + year + "/"
        top_year_list = os.listdir(year_path)

        for quartersFile in top_year_list:
            quartersFile_path = year_path + quartersFile
            Data = open(quartersFile_path, 'r')
            quartersFile_data = json.load(Data)

            for top_Data in quartersFile_data['data']['districts']:
                Name = top_Data['name']
                registeredUser = top_Data['registeredUsers']
                topUserDistrict_Data_state['State'].append(states)
                topUserDistrict_Data_state['Year'].append(year)
                topUserDistrict_Data_state['Quarter'].append(int(quartersFile.strip('.json')))
                topUserDistrict_Data_state['District'].append(Name)
                topUserDistrict_Data_state['Registered_User'].append(registeredUser)

            for top_Data in quartersFile_data['data']['pincodes']:
                Name = top_Data['name']
                registeredUser = top_Data['registeredUsers']
                topUserPincode_Data_state['State'].append(states)
                topUserPincode_Data_state['Year'].append(year)
                topUserPincode_Data_state['Quarter'].append(int(quartersFile.strip('.json')))
                topUserPincode_Data_state['Pincode'].append(Name)
                topUserPincode_Data_state['Registered_User'].append(registeredUser)

df_top_UserDistrict_state = pd.DataFrame(topUserDistrict_Data_state)
df_top_UserPinCode_state = pd.DataFrame(topUserPincode_Data_state)

db_connection = mysql.connector.connect(
    host=" Santhoshsivans-MacBook-Air.local",
    user="root",
    password="MSss2926LoVe",

)

cursor = db_connection.cursor()
create_db_query = "CREATE DATABASE IF NOT EXISTS phonepe;"
cursor.execute(create_db_query)

use_db_query = "USE phonepe;"
cursor.execute(use_db_query)

table_queries = [
    '''CREATE TABLE IF NOT EXISTS Year_Quarter (
    Year INT,
    Quarter INT,
    PRIMARY KEY (Year, Quarter)
);''',
    '''CREATE TABLE IF NOT EXISTS Brand_Wise_User_Count_All_India (
    Year INT,
    Quarter INT,
    Brands VARCHAR(255),
    User_Count INT,
    User_Percentage DECIMAL(5, 2),
    PRIMARY KEY (Year, Quarter, Brands),
    FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
);''',
    '''CREATE TABLE IF NOT EXISTS Brand_Wise_User_Count_State_Wise (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Brands VARCHAR(255),
    User_Count INT,
    User_Percentage DECIMAL(5, 2),
    PRIMARY KEY (State, Year, Quarter, Brands),
    FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
);''',
    '''CREATE TABLE IF NOT EXISTS Registered_User_All_India (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Registered_User INT,
    PRIMARY KEY (State, Year, Quarter),
    FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
);
''',
    '''CREATE TABLE IF NOT EXISTS Registered_User_All_States (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    District VARCHAR(255),
    Registered_User INT,
    PRIMARY KEY (State, Year, Quarter, District),
    FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
);
''',
    '''CREATE TABLE IF NOT EXISTS Top_10_Registered_Users_All_India_State (
    Year INT,
    Quarter INT,
    State VARCHAR(255),
    Registered_User INT,
    PRIMARY KEY (Year, Quarter, State),
    FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
);
''',
    '''CREATE TABLE IF NOT EXISTS Top_10_Registered_Users_All_India_District (
    Year INT,
    Quarter INT,
    District VARCHAR(255),
    Registered_User INT,
    PRIMARY KEY (Year, Quarter, District),
    FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
);
''',
    '''CREATE TABLE IF NOT EXISTS Top_10_Registered_Users_All_India_Pincode (
    Year INT,
    Quarter INT,
    Pincode INT,
    Registered_User INT,
    PRIMARY KEY (Year, Quarter, Pincode),
    FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
);
''',
    '''CREATE TABLE IF NOT EXISTS Top_10_Registered_Users_State_District (
    Year INT,
    Quarter INT,
    State VARCHAR(255),
    District VARCHAR(255),
    Registered_User INT,
    PRIMARY KEY (Year, Quarter, State, District),
    FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
);
''',
    '''CREATE TABLE IF NOT EXISTS Top_10_Registered_Users_State_Pincode (
    Year INT,
    Quarter INT,
    State VARCHAR(255),
    Pincode INT,
    Registered_User INT,
    PRIMARY KEY (Year, Quarter, State, Pincode),
    FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
);
''',
    """CREATE TABLE IF NOT EXISTS Transaction_Type_Count_Amount_All_India (
           Year INT,
           Quarter INT,
           Transaction_type VARCHAR(255),
           Transaction_count BIGINT,
           Transaction_amount DECIMAL(18, 2),
           PRIMARY KEY (Year, Quarter, Transaction_type),
           FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
       );""",
    """CREATE TABLE IF NOT EXISTS Transaction_Type_Count_Amount_State (
        State VARCHAR(255),
        Year INT,
        Quarter INT,
        Transaction_Type VARCHAR(255),
        Transaction_Count BIGINT,
        Transaction_Amount DECIMAL(18, 2),
        PRIMARY KEY (State, Year, Quarter, Transaction_Type),
        FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
    );""",
    """CREATE TABLE IF NOT EXISTS Transaction_Count_Amount_All_India_State_Wise (
        State VARCHAR(255),
        Year INT,
        Quarter INT,
        Transaction_Count BIGINT,
        Transaction_Amount DECIMAL(18, 2),
        PRIMARY KEY (State, Year, Quarter),
        FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
    );""",
    """CREATE TABLE IF NOT EXISTS Transaction_Count_Amount_Each_State_District_Wise (
        State VARCHAR(255),
        Year INT,
        Quarter INT,
        District VARCHAR(255),
        Transaction_Count BIGINT,
        Transaction_Amount DECIMAL(18, 2),
        PRIMARY KEY (State, Year, Quarter, District),
        FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
    );""",
    """CREATE TABLE IF NOT EXISTS Top_10_Transaction (
        Year INT,
        Quarter INT,
        State VARCHAR(255),
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(18, 2),
        PRIMARY KEY (Year, Quarter, State),
        FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
    );""",
    """CREATE TABLE IF NOT EXISTS Top_10_Transaction_District (
        Year INT,
        Quarter INT,
        District VARCHAR(255),
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(18, 2),
        PRIMARY KEY (Year, Quarter, District),
        FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
    );""",
    """CREATE TABLE IF NOT EXISTS Top_10_Transaction_Pincode (
        Year INT,
        Quarter INT,
        Pincode INT,
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(18, 2),
        PRIMARY KEY (Year, Quarter, Pincode),
        FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
    );""",
    """CREATE TABLE IF NOT EXISTS Top_10_Transaction_State_District (
        Year INT,
        Quarter INT,
        State VARCHAR(255),
        District VARCHAR(255),
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(18, 2),
        PRIMARY KEY (Year, Quarter, State, District),
        FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
    );""",
    """CREATE TABLE IF NOT EXISTS Top_10_Transaction_State_Pincode (
        Year INT,
        Quarter INT,
        State VARCHAR(255),
        Pincode INT,
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(18, 2),
        PRIMARY KEY (Year, Quarter, State, Pincode),
        FOREIGN KEY (Year, Quarter) REFERENCES Year_Quarter(Year, Quarter)
    );"""
]

for query in table_queries:
    try:
        cursor.execute(query)
        db_connection.commit()
    except mysql.connector.Error as err:
        print("Error:", err)

all_year_quarters = {'Year': [], 'Quarter': []}
for year in range(2018, 2024):
    for quarter in range(1, 5):
        all_year_quarters['Year'].append((year))
        all_year_quarters['Quarter'].append((quarter))
df_Year_Quarter = pd.DataFrame(all_year_quarters)
print(df_top_transactionPinCode_State.isnull().sum())

for index, row in df_Year_Quarter.iterrows():
    # Convert NumPy int64 to Python int
    year = int(row['Year'])
    quarter = int(row['Quarter'])

    # Define the SQL INSERT query
    insert_query = """
        INSERT INTO Year_Quarter (Year, Quarter)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year), Quarter = VALUES(Quarter)
        """

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (year, quarter))
for index, row in df_aggregated_user_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Brand_Wise_User_Count_All_India (Year, Quarter, Brands, User_Count, User_Percentage)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Brands = VALUES(Brands),
        User_Count = VALUES(User_Count),
        User_Percentage = VALUES(User_Percentage);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['Brands'],
        int(row['User Count']),
        float(row['User Percentage'])))

for index, row in df_aggregated_user_state.iterrows():
    insert_query = """
        INSERT INTO Brand_Wise_User_Count_State_Wise (State, Year, Quarter, Brands, User_Count, User_Percentage)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Brands = VALUES(Brands),
        User_Count = VALUES(User_Count),
        User_Percentage = VALUES(User_Percentage);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        row['State'],
        int(row['Year']),
        int(row['Quarter']),
        row['Brands'],
        int(row['User_Count']),
        float(row['User_Percentage'])))
for index, row in df_map_user_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Registered_User_All_India (State, Year, Quarter, Registered_User)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Registered_User = VALUES(Registered_User);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        row['State'],
        int(row['Year']),
        int(row['Quarter']),
        int(row['Registered_User'])))

for index, row in df_map_user_state.iterrows():
    insert_query = """
        INSERT INTO Registered_User_All_States (State, Year, Quarter, District, Registered_User)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        District = VALUES(District),
        Registered_User = VALUES(Registered_User);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        row['State'],
        int(row['Year']),
        int(row['Quarter']),
        row['District'],
        int(row['Registered_User'])))

for index, row in df_top_UserState_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Top_10_Registered_Users_All_India_State (Year, Quarter, State, Registered_User)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        State = VALUES(State),
        Registered_User = VALUES(Registered_User);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['State'],
        int(row['Registered_User'])))

for index, row in df_top_UserDistrict_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Top_10_Registered_Users_All_India_District (Year, Quarter, District, Registered_User)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        District = VALUES(District),
        Registered_User = VALUES(Registered_User);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['District'],
        int(row['Registered_User'])))
for index, row in df_top_UserPinCode_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Top_10_Registered_Users_All_India_Pincode (Year, Quarter, Pincode, Registered_User)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Pincode = VALUES(Pincode),
        Registered_User = VALUES(Registered_User);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['Pincode'],
        int(row['Registered_User'])))

for index, row in df_top_UserDistrict_state.iterrows():
    insert_query = """
        INSERT INTO Top_10_Registered_Users_State_District (Year, Quarter, State, District, Registered_User)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        State = VALUES(State),
        District = VALUES(District),
        Registered_User = VALUES(Registered_User);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['State'],
        row['District'],
        int(row['Registered_User'])))
for index, row in df_top_UserPinCode_state.iterrows():
    insert_query = """
        INSERT INTO Top_10_Registered_Users_State_Pincode (Year, Quarter, State, Pincode, Registered_User)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        State = VALUES(State),
        Pincode = VALUES(Pincode),
        Registered_User = VALUES(Registered_User);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['State'],
        int(row['Pincode']),
        int(row['Registered_User'])))

for index, row in df_aggregated_transaction_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Transaction_Type_Count_Amount_All_India (Year, Quarter, Transaction_type, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Transaction_type = VALUES(Transaction_type),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['Transaction_type'],
        int(row['Transaction_count']),
        float(row['Transaction_amount'])))

for index, row in df_aggregated_transaction_state.iterrows():
    insert_query = """
        INSERT INTO Transaction_Type_Count_Amount_State (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Transaction_type = VALUES(Transaction_type),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        row['State'],
        int(row['Year']),
        int(row['Quarter']),
        row['Transaction Type'],
        int(row['Transaction Count']),
        float(row['Transaction Amount'])))

for index, row in df_map_transaction_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Transaction_Count_Amount_All_India_State_Wise (State, Year, Quarter, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        row['State'],
        int(row['Year']),
        int(row['Quarter']),
        int(row['Transaction_Count']),
        float(row['Transaction_Amount'])))

for index, row in df_map_transaction_state.iterrows():
    insert_query = """
        INSERT INTO Transaction_Count_Amount_Each_State_District_Wise (State, Year, Quarter, District, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        District = VALUES(District),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        row['State'],
        int(row['Year']),
        int(row['Quarter']),
        row['District'],
        int(row['Transaction_Count']),
        float(row['Transaction_Amount'])))
for index, row in df_top_transactionState_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Top_10_Transaction (Year, Quarter, State, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        State = VALUES(State),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['State'],
        int(row['Transaction_count']),
        float(row['Transaction_amount'])))
for index, row in df_top_transactionDistrict_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Top_10_Transaction_District (Year, Quarter, District, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        District = VALUES(District),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['District'],
        int(row['Transaction_count']),
        float(row['Transaction_amount'])))
for index, row in df_top_transactionPinCode_AllIndia.iterrows():
    insert_query = """
        INSERT INTO Top_10_Transaction_Pincode (Year, Quarter, Pincode, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Pincode = VALUES(Pincode),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['Pincode'],
        int(row['Transaction_count']),
        float(row['Transaction_amount'])))
for index, row in df_top_transactionDistrict_State.iterrows():
    insert_query = """
        INSERT INTO Top_10_Transaction_State_District (Year, Quarter, State, District, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        State = VALUES(State),
        District = VALUES(District),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['State'],
        row['District'],
        int(row['Transaction_count']),
        float(row['Transaction_amount'])))

for index, row in df_top_transactionPinCode_State.iterrows():
    insert_query = """
        INSERT INTO Top_10_Transaction_State_Pincode (Year, Quarter, State, Pincode, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        State = VALUES(State),
        Pincode = VALUES(Pincode),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);"""

    # Execute the INSERT query with row values
    cursor.execute(insert_query, (
        int(row['Year']),
        int(row['Quarter']),
        row['State'],
        int(row['Pincode']),
        int(row['Transaction_count']),
        float(row['Transaction_amount'])))

# Commit the changes
db_connection.commit()

cursor.close()
db_connection.close()
