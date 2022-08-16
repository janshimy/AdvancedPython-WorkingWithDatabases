""" INSTALL THE FOLLOWING LIBRARIES IF YOU THINK YOU DON'T HAVE THEM INSTALLED ALREADY"""
# !pip install pandas 
# !pip install sqlalchemy 
# !pip install argparse
# !pip install requests
# !pip install gzip
# !pip install mysqlclient

# Import all the necessary libraries
import pandas as pd
import sqlalchemy
import argparse
import requests
import gzip
import argparse

""" Create arguments parsing to allow the users to specify the category of the data they want """
parser = argparse.ArgumentParser()
parser.add_argument('--category', help='python ./load_data.py --category Category_Name')
args = parser.parse_args()
while(args.category ==None or args.category==''):
    category_input = input("Please Enter the Valid Category: ")
    args = parser.parse_args()
    args.category = category_input
print("You parsed the following arguments: ", args.category) # Debugging line, to be dropped in the final script


"""get the gzipped file name from the website link."""
# Get the category from the parsed argument
category = args.category
# Get the web link to the dataset of the specified category
def getCategoryURL(category: str) ->str:
    url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt"
    filename = "index.txt"
    request_file = requests.get(url, allow_redirects=True)
    open(filename, 'wb').write(request_file.content)
    with open(filename,'r') as f:
        lines = f.readlines()
    for line in lines:
        try:
            line.upper().index(category.upper())
        except ValueError:
            # print("Not found!")
            continue
        else:
            print(category + " Category is Found!")
            print("The full URL is: "+line)
            return line
            break

# Download the data file from the web

url = getCategoryURL(category)

#Get/formulate a file name from the url
def getDataFilename_fromLink(weblink: str) -> str:
    filename = weblink.split('/')[-1]
    # drop the \n that comes in through readlines method
    filename = filename.split('\n')[0]
    return filename

# Download the data file and save it on local machine
filename = getDataFilename_fromLink(url)
print(filename)
request = requests.get(url.strip('\n'), allow_redirects=True)
open(filename, 'wb').write(request.content)
print("The gzipped file saved under the name: ", filename)

# Unzip the gz file and write the contents into pandas dataframe
with gzip.open(filename, 'rb') as f:
    dataFrame1 = pd.read_csv(f, sep = '\t')

my_conn = sqlalchemy.create_engine(
    "mysql://username:password@hostname:port_number/database_name")

# write the dataframe to the table named "reviews_raw" in the databse named "database_name"
print("Writing the data into the Database! Please wait...")
dataFrame1.to_sql(con=my_conn,name='reviews_raw',if_exists='append',index=False)
print("Done!")
