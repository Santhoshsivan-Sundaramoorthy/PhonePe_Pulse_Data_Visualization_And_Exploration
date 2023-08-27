import mysql
import streamlit as st
from data_transform import tableCreationandDataInsertion
import mysql.connector

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
        print(state_list)
        default_state = "All India"
        state_list = [default_state] + state_list
        selected_state = st.selectbox('Please select the State', state_list, index=state_list.index(default_state))

        #All india
    if selected_state == "All India":
        if selected_type == "Transaction":
            querry = f'select * from Transaction_Type_Count_Amount_State WHERE Year = {selected_Year} AND Quarter = {selected_Quarter};'
            fun_result = fetch_Query(querry)
            st.dataframe(fun_result)




