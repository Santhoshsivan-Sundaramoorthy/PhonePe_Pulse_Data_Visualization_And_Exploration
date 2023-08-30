# PhonePe_Pulse_Data_Visualization_And_Exploration

This project aims to extract, transform, and visualize data from the Phonepe Pulse GitHub repository. The repository contains a vast collection of metrics and statistics related to various domains. The goal is to process this data and present it in an interactive and user-friendly dashboard using a combination of technologies and tools. The project falls under the domain of Fintech.

## Technologies Used

- **GitHub Cloning**: The project involves scripting to clone the Phonepe Pulse GitHub repository and fetch the data.

- **Python**: Python programming language is the core of this project, used for scripting, data manipulation, and dashboard creation.

- **Pandas**: The Pandas library is employed for data manipulation, preprocessing, and transformation.

- **MySQL**: MySQL database is utilized for efficient data storage and retrieval.

- **mysql-connector-python**: This Python library is used to establish a connection between the Python application and the MySQL database for data insertion and retrieval.

- **Streamlit**: Streamlit is a web application framework for creating interactive dashboards with Python.

- **Plotly**: Plotly is used to create interactive and visually appealing visualizations, including geo maps.

## Problem Statement

The Phonepe Pulse GitHub repository holds a wealth of data encompassing various metrics and statistics. The project's objective is to extract, preprocess, and visualize this data in a user-friendly manner. The solution encompasses the following steps:

1. **Data Extraction**: Employ scripting to clone the Phonepe Pulse GitHub repository, extracting the data for further processing.

2. **Data Transformation**: Utilize Python and Pandas to preprocess and clean the data, preparing it for analysis and visualization.

3. **Database Insertion**: Use the "mysql-connector-python" library to insert the transformed data into a MySQL database, ensuring efficient storage and retrieval.

4. **Dashboard Creation**: Leverage Streamlit and Plotly libraries to create an interactive and visually captivating dashboard. Plotly's geo mapping functionalities and Streamlit's dropdown options are used for visualization.

5. **Data Retrieval**: Establish a connection to the MySQL database using "mysql-connector-python" and fetch data into Pandas dataframes. This data is used to dynamically update the dashboard.

6. **Deployment**: Ensure the solution is secure, efficient, and user-friendly. Thoroughly test the solution and deploy the dashboard for public access.

## Approach

1. **Data Extraction**: Using scripting, clone the Phonepe Pulse GitHub repository and store the data in a suitable format (CSV, JSON).

2. **Data Transformation**: Utilize Python and Pandas to preprocess and clean the data, handling missing values and converting it into an analysis-friendly format.

3. **Database Insertion**: Connect to the MySQL database using "mysql-connector-python" and insert the transformed data through SQL commands.

4. **Dashboard Creation**: Leverage Streamlit and Plotly to create an interactive dashboard. Utilize Plotly for geo visualizations and Streamlit for dropdown options.

5. **Data Retrieval**: Connect to the MySQL database using "mysql-connector-python," fetch data into Pandas dataframes, and update the dashboard dynamically.

6. **Deployment**: Thoroughly test the solution for security and efficiency, then deploy the interactive dashboard for public access.

## Results

The project's outcome is a live geo visualization dashboard that presents insights and information derived from the Phonepe Pulse GitHub repository. This dashboard provides an interactive and visually engaging experience, allowing users to explore the data through different dropdown options. Data is stored in a MySQL database for efficient retrieval, ensuring that the dashboard always reflects the latest information.

Users can access the dashboard via a web browser, intuitively navigating through various visualizations and facts. The dashboard serves as a valuable tool for data analysis and decision-making, offering meaningful insights into the Phonepe Pulse data repository.

In conclusion, this project delivers a comprehensive and user-friendly solution for extracting, transforming, and visualizing data from the Phonepe Pulse GitHub repository.
