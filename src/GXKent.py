"""
GXKent.py 
Kent was the city featured in the Charles Dickens classic, and is therefore the sensible name for a container of expectations
The central issue that Kent resolves is to ensure that pandas dataframes are available and populated with data
in both of our data contexts: the DataWarehouse and VRDC. 

TODO: 
* Command Line Support: There should actually be 2 ways to run this script against our data warehouse. One is in collab scripts.. and the other is from the command line. 
If used from the command line.. the script should seek out database credentials in the standard .env file (which should be exlcuded in .gitignore)
In fact.. the script should simply first try to find credentials from a .env file before trying to use a google drive spreadsheet to store credentials..
This will allow us to use one and only one basic "holder" for our expectations and this "holder" will hide all of the complexity of connecting..
and just allow us to think about expectations abstractly.

* Using the command line version of this script, write unit tests for this code. 
* Test should include: connecting the the DB from .env, running simple expectations that will never fail, running expectations that will always fails.. using randomly created data
to create 100 different tables that should sometimes pass and sometimes fail and then making sure that they sometimes pass and sometimes fail. 
make sure you correctly handle the case where someone tries to run the unit test without having setup .env. It should say "you need to have .env file to connect to DW" etc etc.

* Printing have easy to read colored text results. Passing tests should take no more than 1 line of text. Failing tests should take no more than three.



* Figure out how to load this as a class at the top of Google Collab Notebooks. It would suck to need this class to be included before the actual expectation tests 
while using it against the DW. The simplest way to get this to work reliably may be to "Open Source" this class and put it up on pypi so that we can just say "import kent" in google collab.
But see if there is a different way to do this consistently. It is possible to have the class just sitting next to the script in google drive... but I am afraid that 
might not be clean to support in the long term. Perhaps this should be something we contribute back to Great Expectations? I doubt they would accept it..
but we might learn alot about how GX is supposed to work when they tell us "why" they will not accept it. If we do this, we need to remove mentions of VRDC 
and replace them with "Spark"

The statements that I expect to see in the expectation notebooks will look like this: 

Kent = GXKent()
Kent.is_print_on_success = False

  sql_text = 
SELECT count(DISTINCT a.npi) AS new_npi_cnt
FROM default_npi_setting_count.{table_name} a
WHERE a.npi NOT IN (
    SELECT DISTINCT b.npi
    FROM default_npi_setting_count.persetting_2021_12 b
    );

gxDF = Kent.gx_df_from_sql(sql_text)

Kent.capture_expectation(
    expectation_name='Between year comparision {this_year} {that_year}', 
    expectation_result=gxDF.expect_column_max_to_be_between('new_npi_cnt',112671,253511)
)

Kent.capture_expectation(
    expectation_name='Between year comparision {this_year} {that_year}', 
    expectation_result=gxDF.expect_column_min_to_be_between('new_npi_cnt',11671,23511)
)

Kent.capture_expectation(
    expectation_name='Between year comparision {this_year} {that_year}', 
    expectation_result=gxDF.expect_column_avg_to_be_between('new_npi_cnt',50000,60000)
)




"""

__author__ = 'Fred Trotter'
__version__ = '0.1'
__date__ = '2023-12-05'


from pdb import line_prefix
import sys
sys.path.append("/dbfs/mnt/dua/dua_027654/FTR820/") 
from importlib import util as iutil
import great_expectations as gx
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine, text


class GXKent(object):

    password_worksheet_default = 'CollabDWAccess'
    is_print_on_success = True

    #Returns a great expecations dataframe from raw SQL text
    def gx_df_from_sql(sql):
        #the hard work is done here.. to get a pandas dataframe
        pandas_df = self.pd_df_from_sql(sql)
        #now we convert it to great expectations
        gx_df = gx.from_pandas(pandas_df)
        #and return it.
        return(gx_df)


    #returns a pandas dataframe from raw SQL text. This is the place where VRDC vs Datawarehouse really matters
    def pd_df_from_sql(sql):
        sql_text = text(sql)
        if(self.is_DW):
            #Then we need to use the db_connection that we created when we initialized
            #to create the pandas dataframe. 
            pandas_df = pd.read_sql_query(sql_text,self.db_connection)
        else:
            if(self.is_VRDC):
                #when we initialized we found our spark connection and now we use it to run the sql
                spark_df = self.spark.sql(sql_text)
                pandas_df = spark_df.toPandas()
            else:
                #This means we are not in VRDC and we are not in the datawarehouse. This should be unreachable.
                raise Exception("Kent.py: it should not be possible to be neither in the DataWarehouse or in VRDC")
        return(pandas_df)    


    def init_DW_connection(self, *, password_worksheet = None, gx_context_name = 'default_gx_context') -> None:
        #First we have a bunch of Google Collab specific libraries to import
        import gspread
        import getpass
        from google.auth import default #autenticating to google
        from google.colab import auth

        if password_worksheet is None:
            password_worksheet = self.password_worksheet_default
        

        #First we use google to access a worksheet that contains your database password
        # You should NEVER save a password directly into a script, either in Colab
        # or in github.
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        worksheet = gc.open(name_of_my_password_worksheet).sheet1 #get_all_values gives a list of rows
        rows = worksheet.get_all_values() #Convert to a DataFrame
        df = pd.DataFrame(rows)

        # Convert column first row into data column labels
        df.columns = df.iloc[0]
        df = df.iloc[1:]

        #assumes that the second row of the spreadsheet (the first row of data)
        #has the username and password, etc
        username = df.iat[0,0]
        password = df.iat[0,1]
        server = df.iat[0,2]
        port = str(df.iat[0,3])
        db = df.iat[0,4]

        # Now we have the credentials and other details we need to connect the
        # database server.

        sql_url = f"mysql+pymysql://{username}:{password}@{server}:{port}/{db}"

        engine = create_engine(sql_url)
        self.db_connection = engine.connect()
        


    def __init__(self, *, password_worksheet = None ) -> None:

        self.expectation_dict = {}

        #This is where we determine if we are in a databricks or MariaDB context. 
        #The short answer is that if pyspark is available then we are in VRDC
        if (spec := iutil.find_spec('pyspark')) is not None:

            self.is_VRDC = True
            self.is_DW = False

            # Then we have pyspark and we are in VRDC or VRDC simulation.
            # Lets save the spark session...  
            from pyspark.context import SparkContext
            from pyspark.sql.session import SparkSession
            sc = SparkContext.getOrCreate();

            self.spark = SparkSession(sc)
        else:
            
            # TODO This is where we sort out if we are in Google Collab or Command Line

            self.is_VRDC = False
            self.is_DW = True

            #Here we are in the mariadb context. 
            self.init_DW_connection(password_worksheet = password_worksheet)



    #this adds the results of an expectation to our list of expectation results
    def capture_expectation(*, expectation_name, expectation_result):
        #Is there anything else this function should be doing?
        self.expectation_list[expectation_name] = expectation_result

    #loops over all of the expectations and prints the results
    def print_all_expectation_results(self):
        for this_name, this_result in self.expectation_list:          
            self.print_one_expectation_results(
                                        expectation_name=this_name,
                                        gx_results=this_result
            )


    #this is our expectation result printer.. and it should be greatly improved. 
    def print_one_expectation_results(*, expectation_name, gx_result, is_print_success: bool):

        if """"success": false""" in gx_result.__str__():
            print(f"\tFAIL: {expectation_name}")
            print(gx_result)
        else:
            if self.is_print_on_success:
                print(f"\tSUCCESS: {expectation_name}")
        
