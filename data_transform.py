import os
import json
import pandas as pd


# Aggregated
# Transaction
# All India
aggregated_dir_transaction_AllIndia = 'pulse_repository/data/aggregated/transaction/country/india/'

aggregatedTransactionState_Data_AllIndia = {'Year': [], 'Quarter': [], 'Transaction_type': [],
                                            'Transaction_count': [],
                                            'Transaction_amount': []}
aggregated_years_All_India = ['2018', '2019', '2020', '2021', '2022', '2023']

for year in aggregated_years_All_India:
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

for year in aggregated_years_All_India:
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
map_years_All_India = ['2018', '2019', '2020', '2021', '2022', '2023']

for year in map_years_All_India:
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

for year in map_years_All_India:
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
top_years_All_India = ['2018', '2019', '2020', '2021', '2022', '2023']

for year in top_years_All_India:
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
topTransactionPincode_Data_State = {'State': [],'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],
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

# User
# All India
top_dir_User_AllIndia = 'pulse_repository/data/top/user/country/india/'

topUserState_Data_AllIndia = {'Year': [], 'Quarter': [], 'State': [], 'Registered_User': []}
topUserDistrict_Data_AllIndia = {'Year': [], 'Quarter': [], 'District': [], 'Registered_User': []}
topUserPincode_Data_AllIndia = {'Year': [], 'Quarter': [], 'Pincode': [], 'Registered_User': []}


for year in top_years_All_India:
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
topUserDistrict_Data_state = {'State':[], 'Year': [], 'Quarter': [], 'District': [], 'Registered_User': []}
topUserPincode_Data_state = {'State':[], 'Year': [], 'Quarter': [], 'Pincode': [], 'Registered_User': []}


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

#getting the file names
print('---------------AGGREGATED tABLE-----------')
print(df_aggregated_transaction_AllIndia.isnull().sum())
print(df_aggregated_transaction_state.isnull().sum())
print(df_aggregated_user_AllIndia.isnull().sum())
print(df_aggregated_user_state.isnull().sum())
print('---------------MAP tABLE-----------')
print(df_map_transaction_AllIndia.isnull().sum())
print(df_map_transaction_state.isnull().sum())
print(df_map_user_AllIndia.isnull().sum())
print(df_map_user_state.isnull().sum())
print('---------------TOP tABLE-----------')
print(df_top_transactionState_AllIndia.isnull().sum())
print(df_top_transactionDistrict_AllIndia.isnull().sum())
print(df_top_transactionPinCode_AllIndia.isnull().sum())
print(df_top_transactionDistrict_State.isnull().sum())
print(df_top_transactionPinCode_State.isnull().sum())
print(df_top_UserState_AllIndia.isnull().sum())
print(df_top_UserDistrict_AllIndia.isnull().sum())
print(df_top_UserPinCode_AllIndia.isnull().sum())
print(df_top_UserDistrict_state.isnull().sum())
print(df_top_UserPinCode_state.isnull().sum())