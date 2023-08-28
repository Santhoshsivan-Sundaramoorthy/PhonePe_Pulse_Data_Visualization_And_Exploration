import json

import mysql
import numpy as np
import streamlit as st
from data_transform import tableCreationandDataInsertion
import mysql.connector
import pandas as pd
import plotly.express as px

year_list = ()

replacement_dict = {
    "andaman & nicobar islands": "Andaman & Nicobar Island",
    "andhra pradesh": "Andhra Pradesh",
    "arunachal pradesh": "Arunanchal Pradesh",
    "assam": "Assam",
    "bihar": "Bihar",
    "chandigarh": "Chandigarh",
    "chhattisgarh": "Chhattisgarh",
    "dadra & nagar haveli & daman & diu": "Dadara & Nagar Havelli",
    "delhi": "NCT of Delhi",
    "goa": "Goa",
    "gujarat": "Gujarat",
    "haryana": "Haryana",
    "himachal pradesh": "Himachal Pradesh",
    "jammu & kashmir": "Jammu & Kashmir",
    "jharkhand": "Jharkhand",
    "karnataka": "Karnataka",
    "kerala": "Kerala",
    "ladakh": "Jammu & Kashmir",
    "lakshadweep": "Lakshadweep",
    "madhya pradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "manipur": "Manipur",
    "meghalaya": "Meghalaya",
    "mizoram": "Mizoram",
    "nagaland": "Nagaland",
    "odisha": "Odisha",
    "puducherry": "Puducherry",
    "punjab": "Punjab",
    "rajasthan": "Rajasthan",
    "sikkim": "Sikkim",
    "tamil nadu": "Tamil Nadu",
    "telangana": "Telangana",
    "tripura": "Tripura",
    "uttar pradesh": "Uttar Pradesh",
    "uttarakhand": "Uttarakhand",
    "west bengal": "West Bengal"
}
replacement_dict2 = {
    "andaman-&-nicobar-islands": "Andaman & Nicobar Island",
    "andhra-pradesh": "Andhra Pradesh",
    "arunachal-pradesh": "Arunanchal Pradesh",
    "assam": "Assam",
    "bihar": "Bihar",
    "chandigarh": "Chandigarh",
    "chhattisgarh": "Chhattisgarh",
    "dadra-&-nagar-haveli-&-daman-&-diu": "Dadara & Nagar Havelli",
    "delhi": "NCT of Delhi",
    "goa": "Goa",
    "gujarat": "Gujarat",
    "haryana": "Haryana",
    "himachal-pradesh": "Himachal Pradesh",
    "jammu-&-kashmir": "Jammu & Kashmir",
    "jharkhand": "Jharkhand",
    "karnataka": "Karnataka",
    "kerala": "Kerala",
    "ladakh": "Jammu & Kashmir",
    "lakshadweep": "Lakshadweep",
    "madhya-pradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "manipur": "Manipur",
    "meghalaya": "Meghalaya",
    "mizoram": "Mizoram",
    "nagaland": "Nagaland",
    "odisha": "Odisha",
    "puducherry": "Puducherry",
    "punjab": "Punjab",
    "rajasthan": "Rajasthan",
    "sikkim": "Sikkim",
    "madhya-pradesh": "Tamil Nadu",
    "telangana": "Telangana",
    "tripura": "Tripura",
    "tamil-nadu": "Tamil Nadu",
    "uttar-pradesh": "Uttar Pradesh",
    "uttarakhand": "Uttarakhand",
    "west-bengal": "West Bengal"}
st.set_page_config(
    page_title="PhonePe Pulse",
    page_icon="phonepe.png",
    layout="wide"
)

with st.container():
    image_path = "phonepe.png"
    image = st.image(image_path, width=50)
    st.subheader('PhonePe Pulse Data Visualization and Exploration')
# Text input widgets
mysql_host_input = st.text_input("MySQL Host:", "")
mysql_User_input = st.text_input("MySQL User:", "")
mysql_Password_input = st.text_input("MySQL Password:", type="password")


# Function to format state names
def format_state_name(name):
    formatted_name = name.replace('-', ' ').replace('&', '&').title()
    return formatted_name


def fetch_Query(query):
    connection = mysql.connector.connect(
        host=mysql_host_input,
        user=mysql_User_input,
        password=mysql_Password_input,
        database='phonepe'
    )
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    # Close the cursor
    cursor.close()
    # Close the database connection
    connection.close()
    return result


# Check if any of the inputs is empty
input_filled = mysql_host_input and mysql_User_input and mysql_Password_input
if "counter" not in st.session_state:
    st.session_state.counter = 0
# Connect to MySQL button
if input_filled and st.button('Connect to MySQL'):
    st.session_state.counter += 1
    st.warning(tableCreationandDataInsertion(mysql_host_input, mysql_User_input, mysql_Password_input))


elif not input_filled:
    st.warning("Please fill in all input fields before connecting.")
input_filled2 = mysql_host_input and mysql_User_input and mysql_Password_input and st.session_state.counter > 0

try:
    if input_filled2:
        col1, col2, col3 = st.columns(3)
        with col1:
            fetch_year = 'SELECT DISTINCT Year FROM Year_Quarter;'
            year_list = (row[0] for row in fetch_Query(fetch_year))
            selected_Year = st.selectbox('Please select the year', year_list)
        with col2:
            fetch_Quarter = "SELECT DISTINCT Quarter FROM Year_Quarter;"
            Quarter_list = (row[0] for row in fetch_Query(fetch_Quarter))
            selected_Quarter = st.selectbox('Please select the Quarter', Quarter_list)
            type_data = ['Transaction', 'User']
            selected_type = st.selectbox('Please select the type', type_data)
        with col3:
            fetch_states = 'SELECT DISTINCT State FROM Registered_User_All_States;'
            state_list = [row[0] for row in fetch_Query(fetch_states)]
            print(state_list)
            default_state = "All India"
            state_list = [default_state] + state_list
            selected_state = st.selectbox('Please select the State', state_list, index=state_list.index(default_state))

            # All india
        if selected_state == "All India":
            if selected_type == "Transaction":
                coll1, coll2 = st.columns(2, gap='large')
                querry = (f'select Transaction_type, Transaction_count, Transaction_amount from '
                          f'Transaction_Type_Count_Amount_All_India WHERE Year = {selected_Year} AND '
                          f'Quarter = {selected_Quarter};')
                fun_result = fetch_Query(querry)
                column = ['Type', 'Count', 'Amount']
                df = pd.DataFrame(fun_result, columns=column)

                querry2 = (f'select State, Transaction_count, Transaction_amount from '
                           f'Transaction_Count_Amount_All_India_State_Wise WHERE Year = {selected_Year} AND '
                           f'Quarter = {selected_Quarter};')
                map_result = fetch_Query(querry2)
                column = ['State', 'Count', 'Amount']
                df2 = pd.DataFrame(map_result, columns=column)

                with coll1:
                    custom_color = ["#330067", "#4c2881", "#775bbe", "#9a6dbe", "#b284be"]
                    fig1 = px.pie(df,
                                  values="Count",
                                  names="Type",
                                  title="Count Distribution",
                                  color_discrete_sequence=custom_color,
                                  hole=0.7,  # Set the size of the hole to create a donut chart
                                  )
                    st.plotly_chart(fig1)

                    fig = px.pie(df, values="Amount", names="Type", title="Amount Distribution",
                                 color_discrete_sequence=custom_color)
                    st.plotly_chart(fig)
                with coll2:
                    # Define the CSS style
                    css_style = """
                                               <style>
                                                .centered table{
                                                    border-collapse: separate;


                                                }
                                                .centered td{
                                                    border: none;
                                                }
                                                .centered tr{
                                                    border: none;
                                                    text-align: center; 
                                                }
                                                .centered th {
                                                    text-align: center;
                                                    background-color: #330067;
                                                    border: none;
                                                    border-radius: 20px; 
                                                    width: 100%;
                                                    color: white;  
                                                }
                                                </style>
                                                """
                    # Apply the CSS style to the Streamlit app
                    st.markdown(css_style, unsafe_allow_html=True)

                    # Generate the HTML table
                    html_table = df.to_html(index=False, escape=False, classes='centered', border=False)

                    # Display the HTML table using st.write()
                    st.write(html_table, unsafe_allow_html=True)

                    # Map
                    df2["State"] = [replacement_dict.get(value, value) for value in df2["State"]]
                    india_states = json.load(open('states_india.geojson', 'r'))
                    state_id_map = {}
                    for feature in india_states["features"]:
                        feature["id"] = feature["properties"]["state_code"]
                        state_id_map[feature["properties"]["st_nm"]] = feature["id"]
                    df2['Id'] = df2["State"].apply(lambda x: state_id_map[x])
                    fig3 = px.choropleth(df2, locations='Id', geojson=india_states, color='Count', hover_name="State",
                                         hover_data=['Count', 'Amount'], title="PhonePe Transaction",
                                         color_continuous_scale=custom_color)
                    fig3.update_geos(fitbounds="locations", visible=False)
                    st.plotly_chart(fig3)
                st.write("Top 10 Transaction")
                colll1, colll2, colll3 = st.columns(3)

                with colll1:

                    querry3 = (f'select State, Transaction_amount from '
                               f' Top_10_Transaction WHERE Year = {selected_Year} AND '
                               f'Quarter = {selected_Quarter};')
                    top_result_state = fetch_Query(querry3)
                    column3 = ['State', 'Amount']
                    df3 = pd.DataFrame(top_result_state, columns=column3)
                    df3["State"] = [replacement_dict.get(value, value) for value in df3["State"]]
                    crores_conversion = lambda x: f"{x / 10000000:.2f} Cr"
                    df3["Amount"] = [crores_conversion(value) for value in df3["Amount"]]
                    html_table = df3.to_html(index=False, escape=False, classes='centered', border=False)
                    st.write(html_table, unsafe_allow_html=True)
                with colll2:
                    querry4 = (f'select District, Transaction_amount from '
                               f' Top_10_Transaction_District WHERE Year = {selected_Year} AND '
                               f'Quarter = {selected_Quarter};')
                    top_result_District = fetch_Query(querry4)
                    column4 = ['District', 'Amount']
                    df4 = pd.DataFrame(top_result_District, columns=column4)
                    crores_conversion = lambda x: f"{x / 10000000:.2f} Cr"
                    df4["Amount"] = [crores_conversion(value) for value in df4["Amount"]]
                    df4["District"] = [place.title() for place in df4["District"]]
                    html_table = df4.to_html(index=False, escape=False, classes='centered', border=False)
                    st.write(html_table, unsafe_allow_html=True)

                with colll3:
                    querry5 = (f'select Pincode, Transaction_amount from '
                               f' Top_10_Transaction_Pincode WHERE Year = {selected_Year} AND '
                               f'Quarter = {selected_Quarter};')
                    top_result_Pincode = fetch_Query(querry5)
                    column5 = ['Pincode', 'Amount']
                    df5 = pd.DataFrame(top_result_Pincode, columns=column5)
                    crores_conversion = lambda x: f"{x / 10000000:.2f} Cr"
                    df5["Amount"] = [crores_conversion(value) for value in df5["Amount"]]
                    html_table = df5.to_html(index=False, escape=False, classes='centered', border=False)
                    st.write(html_table, unsafe_allow_html=True)
            if selected_type == "User":
                coll1, coll2 = st.columns(2, gap='large')
                querry = (f'select Brands, User_Count  from '
                          f'Brand_Wise_User_Count_All_India WHERE Year = {selected_Year} AND '
                          f'Quarter = {selected_Quarter};')
                fun_result = fetch_Query(querry)
                column = ['Brand', 'User Count']
                df = pd.DataFrame(fun_result, columns=column)
                with coll1:
                    custom_color = ["#330067", "#4c2881", "#775bbe", "#9a6dbe", "#b284be"]
                    fig1 = px.pie(df,
                                  values="User Count",
                                  names="Brand",
                                  title="Brand Distribution",
                                  color_discrete_sequence=custom_color,
                                  )
                    st.plotly_chart(fig1)

                with coll2:
                    # Define the CSS style
                    css_style = """
                                               <style>
                                                .centered table{
                                                    border-collapse: separate;


                                                }
                                                .centered td{
                                                    border: none;
                                                }
                                                .centered tr{
                                                    border: none;
                                                    text-align: center; 
                                                }
                                                .centered th {
                                                    text-align: center;
                                                    background-color: #330067;
                                                    border: none;

                                                    width: 100%;
                                                    color: white;  
                                                }
                                                </style>
                                                """
                    # Apply the CSS style to the Streamlit app
                    st.markdown(css_style, unsafe_allow_html=True)
                    # Generate the HTML table
                    html_table = df.to_html(index=False, escape=False, classes='centered', border=False)
                    # Display the HTML table using st.write()
                    st.write(html_table, unsafe_allow_html=True)

                # Map

                querry2 = (f'select State, Registered_User from '
                           f'Registered_User_All_India WHERE Year = {selected_Year} AND '
                           f'Quarter = {selected_Quarter};')
                map_result = fetch_Query(querry2)
                column = ['State', 'Registered User']
                df2 = pd.DataFrame(map_result, columns=column)

                df2["State"] = [replacement_dict.get(value, value) for value in df2["State"]]
                india_states = json.load(open('states_india.geojson', 'r'))
                state_id_map = {}
                for feature in india_states["features"]:
                    feature["id"] = feature["properties"]["state_code"]
                    state_id_map[feature["properties"]["st_nm"]] = feature["id"]
                df2['Id'] = df2["State"].apply(lambda x: state_id_map[x])
                fig3 = px.choropleth(df2, locations='Id', geojson=india_states, color='Registered User',
                                     hover_name="State", title="PhonePe Users",
                                     color_continuous_scale=custom_color)
                fig3.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig3)

                st.write("Top 10 Registered User")
                colll1, colll2, colll3 = st.columns(3)

                with colll1:

                    querry3 = (f'select State, Registered_User from '
                               f' Top_10_Registered_Users_All_India_State WHERE Year = {selected_Year} AND '
                               f'Quarter = {selected_Quarter};')
                    top_result_state = fetch_Query(querry3)
                    column3 = ['State', 'Registered User']
                    df3 = pd.DataFrame(top_result_state, columns=column3)
                    df3["State"] = [replacement_dict.get(value, value) for value in df3["State"]]
                    crores_conversion = lambda x: f"{x / 1000:.2f} K"
                    df3["Registered User"] = [crores_conversion(value) for value in df3["Registered User"]]
                    html_table = df3.to_html(index=False, escape=False, classes='centered', border=False)
                    st.write(html_table, unsafe_allow_html=True)
                with colll2:
                    querry4 = (f'select District, Registered_User from '
                               f' Top_10_Registered_Users_All_India_District WHERE Year = {selected_Year} AND '
                               f'Quarter = {selected_Quarter};')
                    top_result_District = fetch_Query(querry4)
                    column4 = ['District', 'Registered User']
                    df4 = pd.DataFrame(top_result_District, columns=column4)
                    crores_conversion = lambda x: f"{x / 1000:.2f} K"
                    df4["Registered User"] = [crores_conversion(value) for value in df4["Registered User"]]
                    df4["District"] = [place.title() for place in df4["District"]]
                    html_table = df4.to_html(index=False, escape=False, classes='centered', border=False)
                    st.write(html_table, unsafe_allow_html=True)

                with colll3:
                    querry5 = (f'select Pincode, Registered_User from '
                               f' Top_10_Registered_Users_All_India_Pincode WHERE Year = {selected_Year} AND '
                               f'Quarter = {selected_Quarter};')
                    top_result_Pincode = fetch_Query(querry5)
                    column5 = ['Pincode', 'Registered User']
                    df5 = pd.DataFrame(top_result_Pincode, columns=column5)
                    crores_conversion = lambda x: f"{x / 1000:.2f} K"
                    df5["Registered User"] = [crores_conversion(value) for value in df5["Registered User"]]
                    html_table = df5.to_html(index=False, escape=False, classes='centered', border=False)
                    st.write(html_table, unsafe_allow_html=True)
        elif selected_state:
            if selected_type == "Transaction":
                querry = (f'select Transaction_type, Transaction_count, Transaction_amount from '
                          f'Transaction_Type_Count_Amount_State WHERE Year = {selected_Year} AND '
                          f'Quarter = {selected_Quarter} AND State = "{selected_state}";')
                fun_result = fetch_Query(querry)
                column = ['Type', 'Count', 'Amount']
                df = pd.DataFrame(fun_result, columns=column)

                st.header(f"Transaction Data of the State {selected_state}")

                css_style = """
                                                           <style>
                                                            .centered table{
                                                                border-collapse: separate;


                                                            }
                                                            .centered td{
                                                                border: none;
                                                            }
                                                            .centered tr{
                                                                border: none;
                                                                text-align: center; 
                                                            }
                                                            .centered th {
                                                                text-align: center;
                                                                background-color: #330067;
                                                                border: none;

                                                                width: 100%;
                                                                color: white;  
                                                            }
                                                            </style>
                                                            """
                # Apply the CSS style to the Streamlit app
                st.markdown(css_style, unsafe_allow_html=True)
                # Generate the HTML table
                html_table = df.to_html(index=False, escape=False, classes='centered', border=False)
                # Display the HTML table using st.write()
                st.write(html_table, unsafe_allow_html=True)

                col1, col2 = st.columns(2)

                with col1:
                    querry2 = (f'select District, Transaction_count, Transaction_amount from '
                               f'Transaction_Count_Amount_Each_State_District_Wise WHERE Year = {selected_Year} AND '
                               f'Quarter = {selected_Quarter} AND State = "{selected_state}";')
                    map_result = fetch_Query(querry2)
                    column = ['District', 'Count', 'Amount']
                    df2 = pd.DataFrame(map_result, columns=column)
                    fig = px.bar(df2, x='District', y='Count', title='District Wise Bar Chart',
                                 labels={'District': 'District', 'value': 'Value'},
                                 hover_data={'Count': ':d', 'Amount': ':$,.2f'})

                    st.plotly_chart(fig)

                with col2:
                    st.write('Top 10 Transaction')
                    coll1, coll2 = st.columns(2)
                    with coll1:
                        querry4 = (f'select District, Transaction_amount from '
                                   f' Top_10_Transaction_State_District WHERE Year = {selected_Year} AND '
                                   f'Quarter = {selected_Quarter} AND State = "{selected_state}";')
                        top_result_District = fetch_Query(querry4)
                        column4 = ['District', 'Amount']
                        df4 = pd.DataFrame(top_result_District, columns=column4)
                        crores_conversion = lambda x: f"{x / 10000000:.2f} Cr"
                        df4["Amount"] = [crores_conversion(value) for value in df4["Amount"]]
                        df4["District"] = [place.title() for place in df4["District"]]
                        html_table = df4.to_html(index=False, escape=False, classes='centered', border=False)
                        st.write(html_table, unsafe_allow_html=True)

                    with coll2:
                        querry5 = (f'select Pincode, Transaction_amount from '
                                   f' Top_10_Transaction_State_Pincode WHERE Year = {selected_Year} AND '
                                   f'Quarter = {selected_Quarter} AND State = "{selected_state}";')
                        top_result_Pincode = fetch_Query(querry5)
                        column5 = ['Pincode', 'Amount']
                        df5 = pd.DataFrame(top_result_Pincode, columns=column5)
                        crores_conversion = lambda x: f"{x / 10000000:.2f} Cr"
                        df5["Amount"] = [crores_conversion(value) for value in df5["Amount"]]
                        html_table = df5.to_html(index=False, escape=False, classes='centered', border=False)
                        st.write(html_table, unsafe_allow_html=True)

            if selected_type == "User":
                st.header(f"User Data of the State {selected_state}")
                coll1, coll2 = st.columns(2, gap='large')
                querry = (f'select Brands, User_Count  from '
                          f'Brand_Wise_User_Count_State_Wise WHERE Year = {selected_Year} AND '
                          f'Quarter = {selected_Quarter} AND State = "{selected_state}";')
                fun_result = fetch_Query(querry)
                column = ['Brand', 'User Count']
                df = pd.DataFrame(fun_result, columns=column)
                with coll1:
                    custom_color = ["#330067", "#4c2881", "#775bbe", "#9a6dbe", "#b284be"]
                    fig1 = px.pie(df,
                                  values="User Count",
                                  names="Brand",
                                  title="Brand Distribution",
                                  color_discrete_sequence=custom_color,
                                  )
                    st.plotly_chart(fig1)

                with coll2:
                    # Define the CSS style
                    css_style = """
                                                           <style>
                                                            .centered table{
                                                                border-collapse: separate;


                                                            }
                                                            .centered td{
                                                                border: none;
                                                            }
                                                            .centered tr{
                                                                border: none;
                                                                text-align: center; 
                                                            }
                                                            .centered th {
                                                                text-align: center;
                                                                background-color: #330067;
                                                                border: none;

                                                                width: 100%;
                                                                color: white;  
                                                            }
                                                            </style>
                                                            """
                    # Apply the CSS style to the Streamlit app
                    st.markdown(css_style, unsafe_allow_html=True)
                    # Generate the HTML table
                    html_table = df.to_html(index=False, escape=False, classes='centered', border=False)
                    # Display the HTML table using st.write()
                    st.write(html_table, unsafe_allow_html=True)

                # Map

                querry2 = (f'select District, Registered_User from '
                           f'Registered_User_All_States WHERE Year = {selected_Year} AND '
                           f'Quarter = {selected_Quarter} AND State = "{selected_state}";')
                map_result = fetch_Query(querry2)
                column = ['District', 'Registered User']
                df2 = pd.DataFrame(map_result, columns=column)

                fig = px.bar(df2, x='District', y='Registered User', title='Registered Users')

                st.plotly_chart(fig)

                st.write("Top 10 Registered User")
                colll2, colll3 = st.columns(2)

                with colll2:
                    querry4 = (f'select District, Registered_User from '
                               f' Top_10_Registered_Users_State_District WHERE Year = {selected_Year} AND '
                               f'Quarter = {selected_Quarter} AND State = "{selected_state}";')
                    top_result_District = fetch_Query(querry4)
                    column4 = ['District', 'Registered User']
                    df4 = pd.DataFrame(top_result_District, columns=column4)
                    crores_conversion = lambda x: f"{x / 1000:.2f} K"
                    df4["Registered User"] = [crores_conversion(value) for value in df4["Registered User"]]
                    df4["District"] = [place.title() for place in df4["District"]]
                    html_table = df4.to_html(index=False, escape=False, classes='centered', border=False)
                    st.write(html_table, unsafe_allow_html=True)

                with colll3:
                    querry5 = (f'select Pincode, Registered_User from '
                               f' Top_10_Registered_Users_State_Pincode WHERE Year = {selected_Year} AND '
                               f'Quarter = {selected_Quarter} AND State = "{selected_state}";')
                    top_result_Pincode = fetch_Query(querry5)
                    column5 = ['Pincode', 'Registered User']
                    df5 = pd.DataFrame(top_result_Pincode, columns=column5)
                    crores_conversion = lambda x: f"{x / 1000:.2f} K"
                    df5["Registered User"] = [crores_conversion(value) for value in df5["Registered User"]]
                    html_table = df5.to_html(index=False, escape=False, classes='centered', border=False)
                    st.write(html_table, unsafe_allow_html=True)
except:
    st.warning('Please check the Input')
















