import mysql
import streamlit as st
from data_transform import tableCreationandDataInsertion
import mysql.connector
import pandas as pd
import plotly.express as px

year_list = ()

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

# Connect to MySQL button
if input_filled and st.button('Connect to MySQL'):
    st.warning(tableCreationandDataInsertion(mysql_host_input, mysql_User_input, mysql_Password_input))


elif not input_filled:
    st.warning("Please fill in all input fields before connecting.")

if input_filled:
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
            with coll1:
                custom_color = ["#330067","#4c2881","#775bbe","#9a6dbe","#b284be"]
                fig1 = px.pie(df,
                             values="Count",
                             names="Type",
                             title="Count Distribution",
                             color_discrete_sequence=custom_color,
                             hole=0.7,  # Set the size of the hole to create a donut chart
                             )
                st.plotly_chart(fig1)

                fig = px.pie(df, values="Amount", names="Type", title="Amount Distribution", color_discrete_sequence=custom_color)
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
                                                width: 100%  
                                            }
                                            </style>
                                            """
                # Apply the CSS style to the Streamlit app
                st.markdown(css_style, unsafe_allow_html=True)

                # Generate the HTML table
                html_table = df.to_html(index=False, escape=False, classes='centered', border=False)

                # Display the HTML table using st.write()
                st.write(html_table, unsafe_allow_html=True)

