# AdvancedPython-WorkingWithDatabases
This repositories shows how you can work with the databases and execute SQL commands using python.
This repo contains two python scripts with each scripts showing the setup required to have your environment ready to run the scripts
The scripts let the user know what it is doing at every step of the way via messages to the console.

This worked with the amazon rating data from any review category that the user would choose. The categories include ratings for Wireless, Books, Gift Card, among others. And the link to each category data is found in this txt file:
https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt

What this codes do is to go through this txt file seeking the link to the dataset of a specified category. When the link is found, the script download the data from the link and the data is downloaded as a gzipped tsv file. 

After that, the scripts executes some SQL commands defined within itself and load the data into a table in a certain database whether on remote server or localhost.

The script load_data.py gives the flexibility to create new table within the SQL database, define datatypes, and insert the data from the data frame row by row. Its strength is customized SQL design experiences but its drawback is the limited speed for large datasets:

Run the script:

                $ python ./load_data.py --category CategoryName 

For the script usage information/help:

                $ python ./load_data.py -h
                
The script load_data_with_sqlalchemy.py is faster and easy to work with. It inserts the whole dataframe directly from pandas to sql database. However, it is doesn't customize the datatypes of the table columns.

Run the script:

                $ python ./load_data_with_sqlalchemy.py --category CategoryName 

For the script usage information/help:

                $ python ./load_data_with_sqlalchemy.py -h
