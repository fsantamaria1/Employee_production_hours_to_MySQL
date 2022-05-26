# Employee_production_hours_to_MySQL

This program does the following:

Reads a .csv file from a specific location
Copies its data
Creates a dataframe with the copied data
Adds new headers to the dataframe : batch_id, original_file_name, etc
Inserts the data into to the local MySQL database
Moves the original file to a different location


Notes: 
The MySQLClass needs the correct database credentials
The MySQLClass object in the main() method needs the correct table name
