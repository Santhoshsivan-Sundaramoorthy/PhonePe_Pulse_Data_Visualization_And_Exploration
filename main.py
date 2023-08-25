import streamlit as st
from data_transform import tableCreationandDataInsertion

st.set_page_config(
    page_title="PhonePe Pulse",
    page_icon="phonepe.png",
    layout="wide"
)

col1, col2, col3 = st.columns(3)

with col2:
    with st.container():
        image_path = "phonepe.png"
        image = st.image(image_path, width=50)
        st.subheader('PhonePe Pulse Data Visualization and Exploration')
    # Text input widgets
    mysql_host_input = st.text_input("MySQL Host:", "")
    mysql_User_input = st.text_input("MySQL User:", "")
    mysql_Password_input = st.text_input("MySQL Password:", type="password")

    # Check if any of the inputs is empty
    input_filled = mysql_host_input and mysql_User_input and mysql_Password_input

    # Connect to MySQL button
    if input_filled and st.button('Connect to MySQL'):
        st.warning(tableCreationandDataInsertion(mysql_host_input, mysql_User_input, mysql_Password_input))
    elif not input_filled:
        st.warning("Please fill in all input fields before connecting.")

