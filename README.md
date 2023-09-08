# Behavior_for_360_customer_service
This project is designed to analyze customers's searching behavior in the first 2 weeks of 06/2022 and 07/2023

## About the data 
Users's search is stored at ```parquet``` format and from the first 2 weeks of 06/2022 and 07/2022.

## Procedure 
Data is processed using Pyspark, SparkSQL and then stored into a MySQL database. After that, the processed data is analyzed using Matplotlib 

### Reading data
In the code below, i used os library to read all the folders in the given directory and list all the parquet file for spark to read. After reading data of 1 day, i put it in a FOR loop to read all the remainings and used union() to create the full data set.
![image](https://github.com/Thang285/Behavior_for_360_customer_service/assets/116457922/ac02ad28-f8c5-4da8-b604-3615d30f0ff2)

### Processing data
I used this fuction to process data individually. I created a dataframe based on ```user_id``,```key_word```, ```Month```. After that, i used window fuction to find what is the most-searched keyword for each user and than retured the dataframe 
![image](https://github.com/Thang285/Behavior_for_360_customer_service/assets/116457922/4dedb084-5bb0-4cc0-be4e-80b2ce2bb4cf)

#### Mapping data
After having the data of June and July, i create an Excel file of 1000 keywords and map its categories manually and then join it back to the initial dataframe 
![image](https://github.com/Thang285/Behavior_for_360_customer_service/assets/116457922/90f578f8-202c-4521-96d2-aace727f4b91)

#### Final data
Final data is combination of both dataframe in June and July and another column indicating the change in users's search 
![image](https://github.com/Thang285/Behavior_for_360_customer_service/assets/116457922/5643ad16-d669-4213-8be6-c264efb7eaa9)

### Save the data 
Final data is stored in MySQL database for further in-depth analysis
![image](https://github.com/Thang285/Behavior_for_360_customer_service/assets/116457922/b591abc7-e9c2-4bcc-92df-9016754e46bb)

### Simple analysis
See the provided pfd file






