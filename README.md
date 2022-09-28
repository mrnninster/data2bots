# Project Documentation
This project requires the use of 2 data sources, the s3 bucket and the PostgresDB, based on
this I created modules for the actions i would need to carry out while interacting with each data source.
I also created log files for the s3,database and data processing sections.

## Project Breakdown
- DATA2BOTS
    - database
        - [db.py](database/db.py)
            - database (class)
                - conn (method) -> creates database connection
                - create_cursor (method) -> creates connection cursor.
                - create_table (method) -> create a table
                - fetch_data (method) -> fetches data from database
                - write_data (method) -> write data to the database using threads
                - close (method) closes database connection
    - s3
        - [s3.py](s3/s3.py)
            - bucket (class) -> creates bucket connection
                - fetch -> fetches bucket content
                - download -> downloads files from bucket
                - upload -> uploads files to buckets
    - downloads
        - contains files downloaded from the s3 bucket
    - exports
        - contains files to be uploaded to the s3 bucket
    - log_files
        - contains the log files for the s3,database and processing sections
    - [main.py](main.py)
        - functions
            - add_dataframe_to_table -> Since it is easier to preview csv files as pandas dataframes and results of
            sql quries can also be converted to dataframes whn properlt formatted, I found the dataframe an appropriate 
            middle ground for data exploration and viewing, enabled by the power of pandas. So with this function I am able 
            to upload a dataframe into the database
            - best_performing_product -> this function isused to get some of the properties of the best performing product
            using the review data
        - data_processing
            - I basically carry out the data processing as it was outlined in the project milestones
                - connect to s3 bucket
                - download files
                - load files as pandas dataframes
                - create tables in schema
                - write dataframes to database to create orders, reviews and shipment_deliveries tables
                - find total numbers of public holiday for pas year and write to database
                - find total number of late and undelivered shipments and write to database
                - find best perforing product, carry out product analytics, write to database
    - [LICENSE](LICENSE.md)
    - [requirements](requirements.txt)