# imports
import os
import math
import logging
import pandas as pd


from s3.s3 import bucket
from dotenv import load_dotenv
from database.db import database
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

load_dotenv(".env")

# Setup Logging
main_logger = logging.getLogger("main")
main_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
file_handler = logging.FileHandler("log_files/main.log")
file_handler.setFormatter(formatter)
main_logger.addHandler(file_handler)


# functions
def add_dataframe_to_table(db,dataframe,db_cursor,db_conn,tablename,schemaname,check=True):
    try:
        column_names = tuple(dataframe.columns)
        data = dataframe.to_dict()
        data_list = list()
        for i in range(len(dataframe)):
            dict_data = {f"{j}":f"{data[j][i]}" for j in column_names}
            data_list.append(dict_data)
        if check == True:
            db.write_data(db_cursor,db_conn,data_list,tablename,schemaname)
        else:
            db.write_data(db_cursor,db_conn,data_list,tablename,schemaname,check=False)
    except Exception as e:
        main_logger.critical(f"AddDataframeToTableError: failed to add dataframe to {tablename}, {e}")

def best_performing_product():
    # Fetch review data
    review_data = db.fetch_data(db_cursor,"*",f"{staging}.reviews")

    review_count_dict = {}
    review_points_sum = {}
    review_points_distribution = {}

    for review in review_data:
        if f"id_{review[2]}" in review_count_dict.keys():
            review_count_dict[f"id_{review[2]}"] = review_count_dict[f"id_{review[2]}"] + 1
        else:
            review_count_dict[f"id_{review[2]}"] = 1

        if f"id_{review[2]}" in review_points_sum.keys():
            review_points_sum[f"id_{review[2]}"] = review_points_sum[f"id_{review[2]}"] + review[1]
        else:
            review_points_sum[f"id_{review[2]}"] = review[1]

        if f"id_{review[2]}_{review[1]}" in review_points_distribution.keys():
            review_points_distribution[f"id_{review[2]}_{review[1]}"] = review_points_distribution[f"id_{review[2]}_{review[1]}"] + 1
        else:
            review_points_distribution[f"id_{review[2]}_{review[1]}"] = 1

    # Highest Reviewed Product
    index = list(review_count_dict.values()).index(max(list(review_count_dict.values())))
    highest_reviewed = list(review_count_dict.keys())[index]
    highest_reviewed_id = highest_reviewed.split("_")[-1]
    review_sum = review_points_sum[highest_reviewed]
    pct_one_star = (review_points_distribution[f"{highest_reviewed}_1"]/review_count_dict[highest_reviewed])*100
    pct_two_star = (review_points_distribution[f"{highest_reviewed}_2"]/review_count_dict[highest_reviewed])*100
    pct_three_star = (review_points_distribution[f"{highest_reviewed}_3"]/review_count_dict[highest_reviewed])*100
    pct_four_star = (review_points_distribution[f"{highest_reviewed}_4"]/review_count_dict[highest_reviewed])*100
    pct_five_star = (review_points_distribution[f"{highest_reviewed}_5"]/review_count_dict[highest_reviewed])*100
    product_data = db.fetch_data(db_cursor,"product_name","if_common.dim_products",filtered=True,filter=f"product_id = {highest_reviewed_id}",all=False,one=True)
    product_name = product_data[0]
    return(highest_reviewed_id,product_name,review_sum,pct_one_star,pct_two_star,pct_three_star,pct_four_star,pct_five_star)


if __name__ == "__main__":

    # Connect to database
    db = database()
    db_conn = db.conn()
    db_cursor = db.create_cursor(db_conn)
    staging = os.getenv("STAGING")
    analytics = os.getenv("ANALYTICS")

    # Connect to bucket
    bk = bucket()
    os.makedirs("downloads", exist_ok=True)
    bk.download(file_category="orders",src_name="orders.csv",dst_name="downloads/orders.csv")
    bk.download(file_category="orders",src_name="reviews.csv",dst_name="downloads/reviews.csv")
    bk.download(file_category="orders",src_name="shipment_deliveries.csv",dst_name="downloads/shipment_deliveries.csv")

    # load csv
    orders_df = pd.read_csv("downloads/orders.csv")
    reviews_df = pd.read_csv("downloads/reviews.csv")
    shipments_df = pd.read_csv("downloads/shipment_deliveries.csv")

    # Create csv tables
    try:
        orders_columns = "(id SERIAL PRIMARY KEY, order_id INTEGER, customer_id INTEGER, order_date date, product_id INTEGER, unit_price INTEGER, quantity INTEGER, total_price INTEGER)"
        reviews_columns = "(id SERIAL PRIMARY KEY, review INTEGER, product_id INTEGER)"
        shipments_columns = "(id SERIAL PRIMARY KEY, shipment_id INTEGER, order_id INTEGER, shipment_date VARCHAR, delivery_date VARCHAR)"
        db.create_table(db_cursor,db_conn,orders_columns,"orders",staging)
        db.create_table(db_cursor,db_conn,reviews_columns,"reviews",staging)
        db.create_table(db_cursor,db_conn,shipments_columns,"shipment_deliveries",staging)
    except Exception as e:
        main_logger.critical(f"CreateTableError: failed to create tables, {e}")


    # Write to csv tables
    try:
        with ThreadPoolExecutor() as executor:
            executor.submit(add_dataframe_to_table,db,orders_df,db_cursor,db_conn,"orders",staging)
        with ThreadPoolExecutor() as executor:
            executor.submit(add_dataframe_to_table,db,reviews_df,db_cursor,db_conn,"reviews",staging)
        with ThreadPoolExecutor() as executor:
            executor.submit(add_dataframe_to_table,db,shipments_df,db_cursor,db_conn,"shipment_deliveries",staging)
    except Exception as e:
        main_logger.critical(f"WriteTableError: failed to write tables, {e}")

    # Total number of orders placed on a public holiday every month, for the past year.
    try:
        # Fetch Data
        data = db.fetch_data(db_cursor,"*",f"{staging}.orders",secondary_tablename="if_common.dim_dates",all=True,filtered=True,filter="if_common.dim_dates.working_day = False AND 1 <= if_common.dim_dates.day_of_the_week_num AND 5 >= if_common.dim_dates.day_of_the_week_num",join=True,join_condition=f"if_common.dim_dates.calendar_dt = {staging}.orders.order_date")

        # Create Data Columns
        data_columns = ["db_id"] + list(orders_df.columns) + ["calendar_dt","year_num","month_of_the_year_num","day_of_the_month_num","day_of_the_week_num","working_day"]

        # Create DataFrame
        data_df = pd.DataFrame(data,columns=data_columns)

        # Get last year limit
        todays_date = datetime.utcnow().date()
        last_year_limit = todays_date - timedelta(days=365)

        # Remove unwanted data
        for i in range(len(data)):
            if (data_df["order_date"][i] < last_year_limit):
                data_df.drop(i,inplace=True)
        data_df = data_df.reset_index(drop=True)

        # Aggrigate public holiday sales
        months = {1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0}
        for i in range(len(data_df)):
            monthKey = int(data_df["month_of_the_year_num"][i])
            months[monthKey] = months[data_df["month_of_the_year_num"][i]] + 1
        main_logger.debug(f"AggPublicHolidaySales: {months}")

        # Create/Update Table
        agg_pub_columns = """(
            ingestion_date date NOT NULL, 
            tt_order_hol_jan int NOT NULL, 
            tt_order_hol_feb int NOT NULL, 
            tt_order_hol_mar int NOT NULL, 
            tt_order_hol_apr int NOT NULL, 
            tt_order_hol_may int NOT NULL, 
            tt_order_hol_jun int NOT NULL, 
            tt_order_hol_jul int NOT NULL, 
            tt_order_hol_aug int NOT NULL,
            tt_order_hol_sep int NOT NULL,
            tt_order_hol_oct int NOT NULL,
            tt_order_hol_nov int NOT NULL,
            tt_order_hol_dec int NOT NULL
            )"""

        db.create_table(db_cursor,db_conn,agg_pub_columns,"agg_public_holiday",analytics)
        mnths = {
                "ingestion_date":[todays_date],
                "tt_order_hol_jan":[months[1]],
                "tt_order_hol_feb":[months[2]],
                "tt_order_hol_mar":[months[3]],
                "tt_order_hol_apr":[months[4]],
                "tt_order_hol_may":[months[5]],
                "tt_order_hol_jun":[months[6]],
                "tt_order_hol_jul":[months[7]],
                "tt_order_hol_aug":[months[8]],
                "tt_order_hol_sep":[months[9]],
                "tt_order_hol_oct":[months[10]],
                "tt_order_hol_nov":[months[11]],
                "tt_order_hol_dec":[months[12]]
                }
        
        # Create transformation dataframe
        df_dict = pd.DataFrame.from_dict(mnths)

        # Export to csv
        df_dict.to_csv("exports/agg_public_holiday.csv")

        # Write result to db
        with ThreadPoolExecutor() as executor:
            executor.submit(add_dataframe_to_table,db,df_dict,db_cursor,db_conn,"agg_public_holiday",analytics,check=False)

        main_logger.debug(f"AggPublicHolidaySales: {mnths}")
    except Exception as e:
        main_logger.critical(f"TaskError:AggPublicHolidaySales: failed to complete task, {e}")

    # Total number of late and undelivered shipments
    try:
        # Fetch data
        task2_data = db.fetch_data(db_cursor,"*",f"{staging}.orders",secondary_tablename=f"{staging}.shipment_deliveries",all=True,join=True,join_condition=f"{staging}.shipment_deliveries.order_id = {staging}.orders.order_id",filtered=True,filter=f"{staging}.shipment_deliveries.delivery_date = '-' AND {staging}.shipment_deliveries.shipment_date != '-'")
        task3_data = db.fetch_data(db_cursor,"*",f"{staging}.orders",secondary_tablename=f"{staging}.shipment_deliveries",all=True,join=True,join_condition=f"{staging}.shipment_deliveries.order_id = {staging}.orders.order_id",filtered=True,filter=f"{staging}.shipment_deliveries.delivery_date = '-' AND {staging}.shipment_deliveries.shipment_date = '-'")

        # Data columns
        data_columns = ["oid"] + list(orders_df.columns) + ["sid","shipment_id","order_id","shipment_date","delivery_date"]

        # Dataframes
        task2_data_df = pd.DataFrame(task2_data,columns=data_columns)
        task3_data_df = pd.DataFrame(task3_data,columns=data_columns)

        # Late shipment counter
        late_shipment_counter = 0
        for i in range(len(task2_data_df)):
            order_date = task2_data_df["order_date"][i]
            shipment_date_str = task2_data_df["shipment_date"][i]
            shipment_date = datetime.strptime(shipment_date_str,"%Y-%m-%d").date()
            diff = shipment_date - order_date
            if diff >= timedelta(days=6):
                late_shipment_counter += 1

        main_logger.debug(f"LateShipmentCount: {late_shipment_counter}")

        # Undelivered shipment counter
        undelivered_shipment_counter = 0
        for i in range(len(task2_data_df)):
            order_date = task2_data_df["order_date"][i]
            todays_date = datetime.utcnow().date()
            diff = todays_date - order_date
            if diff >= timedelta(days=15):
                undelivered_shipment_counter += 1

        main_logger.debug(f"UndeliveredShipmentCount: {undelivered_shipment_counter}")
        
    except Exception as e:
        main_logger.critical(f"TaskError:Late&UndeliveredShipments: failed while fetching data {e}")

    else:
        try:
            # Create table if not exists
            agg_shp_columns = """(
                ingestion_date date NOT NULL,
                tt_late_shipments int NOT NULL,
                tt_undelivered_items int NOT NULL
                )"""

            db.create_table(db_cursor,db_conn,agg_shp_columns,"agg_shipments",analytics)

            shipments_analytics = {
                "ingestion_date":[todays_date],
                "tt_late_shipments":[late_shipment_counter],
                "tt_undelivered_items":[undelivered_shipment_counter]
            }
            main_logger.debug(f"AggShipments:{shipments_analytics}")

            # Create transformation dataframe
            df_dict = pd.DataFrame.from_dict(shipments_analytics)

            # Export to csv
            df_dict.to_csv("exports/agg_shipments.csv")

            # Write late and Undelivered shipments to db
            with ThreadPoolExecutor() as executor:
                executor.submit(add_dataframe_to_table,db,df_dict,db_cursor,db_conn,"agg_shipments",analytics,check=False)

        except Exception as e:
            main_logger.critical(F"TaskError:Late&UndeliveredSipments: failed while writing data, {e}")

        # Best Performing Product
        try:
            with ThreadPoolExecutor() as executor:
                process_thread = executor.submit(best_performing_product)
            data = process_thread.result()
            
            # Fetch order data for best performing product
            highest_reviewed_id = data[0]
            order_data = db.fetch_data(db_cursor,"*",f"{staging}.orders",filtered=True,filter=f"product_id = {highest_reviewed_id}")

            # Find date of highest data
            highest_order_date = datetime.strptime("0001-01-01","%Y-%m-%d").date()
            highest_order = 0
            for order in order_data:
                if (order[6] >= highest_order) and (order[3] >= highest_order_date):
                    highest_order_date = order[3]
                    highest_order = order[6]

            # Check date is public holiday or not with dim_dates
            calender_data = db.fetch_data(db_cursor,"day_of_the_week_num,working_day","if_common.dim_dates",filtered=True,filter=f"calendar_dt = '{highest_order_date}'")
            day_of_the_week,working_day = calender_data[0]

            if (day_of_the_week in range(1,6)):
                if (working_day == True):
                    is_public_holiday = False
                else:
                    is_public_holiday = True

            # Early and late shipments
            shipments_data = db.fetch_data(
                db_cursor,
                "*",
                f"{staging}.orders",
                secondary_tablename=f"{staging}.shipment_deliveries",
                all=True,
                join=True,
                join_condition=f"{staging}.shipment_deliveries.order_id = {staging}.orders.order_id",
                filtered=True,
                filter=f"{staging}.shipment_deliveries.shipment_date != '-' AND {staging}.orders.product_id = {highest_reviewed_id}")

            data_columns = ["oid"] + list(orders_df.columns) + ["sid","shipment_id","order_id","shipment_date","delivery_date"]
            shipments_data_df = pd.DataFrame(shipments_data,columns=data_columns)

            total_shipments = len(shipments_data)
            early_shipment_count = 0
            late_shipment_count = 0

            for i in range(total_shipments):
                order_date = shipments_data_df["order_date"][i]
                shipment_date_str = shipments_data_df["shipment_date"][i]
                shipment_date = datetime.strptime(shipment_date_str,"%Y-%m-%d").date()
                diff = shipment_date - order_date
                if diff >= timedelta(days=6):
                    late_shipment_count += 1
                else:
                    early_shipment_count += 1

            pct_late_shipment = (late_shipment_count/total_shipments)*100
            pct_early_shipment = (early_shipment_count/total_shipments)*100
            todays_date = datetime.utcnow().date()

            # Create/Update Database
            best_performing_product_columns = """(
                ingestion_date date NOT NULL,
                product_name varchar NOT NULL,
                most_ordered_day date NOT NULL,
                is_public_holiday bool NOT NULL,
                tt_review_points int NOT NULL,
                pct_one_star_review float NOT NULL,
                pct_two_star_review float NOT NULL,
                pct_three_star_review float NOT NULL,
                pct_four_star_review float NOT NULL,
                pct_five_star_review float NOT NULL,
                pct_early_shipments float NOT NULL,
                pct_late_shipments float NOT NULL
            )"""

            db.create_table(db_cursor,db_conn,best_performing_product_columns,"best_performing_product",analytics)

            best_performing_product_db = {
                "ingestion_date":[todays_date],
                "product_name":[data[1]],
                "most_ordered_day":[highest_order_date],
                "is_public_holiday":[is_public_holiday],
                "tt_review_points":[data[2]],
                "pct_one_star_review":[data[3]],
                "pct_two_star_review":[data[4]],
                "pct_three_star_review":[data[5]],
                "pct_four_star_review":[data[6]],
                "pct_five_star_review":[data[7]],
                "pct_early_shipments":[pct_early_shipment],
                'pct_late_shipments':[pct_late_shipment]
            }
            main_logger.debug(f"BestPerformingProduct:{best_performing_product_db}")

            # Create transformation dataframe
            bpp_dict = pd.DataFrame.from_dict(best_performing_product_db)

            # Export to csv
            bpp_dict.to_csv("exports/best_performing_products.csv")

            # Write result to db
            with ThreadPoolExecutor() as executor:
                executor.submit(add_dataframe_to_table,db,bpp_dict,db_cursor,db_conn,"best_performing_product",analytics,check=False)

        except Exception as e:
            main_logger.critical(f"TaskError:BestPerformingProduct: failed to complete task, {e}")
    
    # Close Database Connection
    db.close(db_conn)

    # Export Data
    user_id = os.getenv("DB_USER")
    bk.upload(user_id=user_id,src_folder="exports",dst_name="analytics_export")