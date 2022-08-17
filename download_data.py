# Import all the necessary libraries
import pandas as pd
import sqlalchemy
import argparse

# Function to manage arguments parsed by the user

def argumentsHandler(arguments:argparse.Namespace) ->argparse.Namespace:
    print("Validating the arguments...")
    # Handle an empty/missing input for table name
    while(arguments.tablename ==None or args.tablename==''):
        tablename_input = input("Please Enter the Valid Tablename: ")
        args.tablename = tablename_input

    # Handle an empty/missing input for file name
    while(arguments.filename ==None or arguments.filename==''):
        filename_input = input("Please Enter the Valid Filename: ")
        arguments.filename = filename_input
    print("You parsed the following arguments:\n Filename: ",
            arguments.tablename, "\n Filename: ", arguments.filename)
    return arguments

""" 
    Function that connect to the database and load the SQL table into pandas data frame. 
    This function returns the pandas dataframe
"""
def sqlToPandas(username='JNSHIMY',
                password='',
                hostname='msba-bootcamp-prod.cneftpdd0l3q.us-east-1.rds.amazonaws.com',
                port=3306,
                database='MSBA_Team13',
                tablename='reviews_raw')->pd.DataFrame:

    # Create a SQL alchemy engine that connect to teh SQL server
    print("Creating the database engine...")
    port = str(port)
    my_engine = sqlalchemy.create_engine(
        f"mysql://{username}:{password}@{hostname}:{port}/{database}")

    my_connection = my_engine.connect()
    metadata = sqlalchemy.MetaData()
    table_name = sqlalchemy.Table(tablename, metadata, autoload=True, autoload_with=my_engine)
    #Equivalent to 'SELECT * FROM table_name'
    query = sqlalchemy.select([table_name]) 
    query_result = my_connection.execute(query)
    queryResultSet = query_result.fetchall()
    # Convert the query output into the pandas dataframe
    df = pd.DataFrame(queryResultSet)
    df.columns = queryResultSet[0].keys()
    return df


if __name__ == '__main__':
    """ Create arguments parsing to allow the users to specify the 
        tablename of the data they want """
    parser = argparse.ArgumentParser()
    parser.add_argument('--tablename')
    parser.add_argument('--filename', 
                        help='python ./download_data.py --tablename Table_Name -filename Full_FilePath')
    args = parser.parse_args()
    # Handle missing/empty arguments
    args = argumentsHandler(args)
    # Define the database credentials
    user = 'your_username'
    my_password = 'your_password'
    host_server = 'msba-bootcamp-prod.cneftpdd0l3q.us-east-1.rds.amazonaws.com'
    port_number = 3306
    database_name = 'MSBA_Team13'
    table_to_download = args.tablename
    # Save dataframe into the csv file
    print("Loading the data from SQL table to Pandas Dataframe...")
    df = sqlToPandas(username = user,password = my_password, hostname= host_server,
                        port = port_number, database = database_name, tablename = table_to_download)
    print(f"Saving the dataframe into the file named {args.filename}...")
    df.to_csv(args.filename, index=False)
    print("Done!")   
