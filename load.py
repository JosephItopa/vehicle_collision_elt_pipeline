
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

def connect_2_db(uidd, pwdd, dserver, dport, ddb):
    return create_engine(f'postgresql://{uidd}:{pwdd}@{dserver}:{dport}/{ddb}')
# connect to database
connection = connect_2_db("sakar_", "sakar10", "localhost", 5432, "postgres")

#(secrets["API_USER"], secrets["API_PWD"], secrets["API_HOST"], 5432, secrets["API_DB"])
def load_2_database():
    def check_tbl_exist():
        try: 
            connection.execute(text("""CREATE TABLE IF NOT EXISTS vehicle_collision(
                                                                                    collision_id VARCHAR, 
                                                                                    crash_date DATE,
                                                                                    crash_time VARCHAR,
                                                                                    on_street_name VARCHAR,
                                                                                    number_of_persons_injured INT,
                                                                                    number_of_persons_killed INT,
                                                                                    number_of_pedestrians_injured INT,
                                                                                    number_of_pedestrians_killed INT,
                                                                                    number_of_cyclist_injured INT,
                                                                                    number_of_cyclist_killed INT,
                                                                                    number_of_motorist_injured INT,
                                                                                    number_of_motorist_killed INT,
                                                                                    contributing_factor_vehicle_1 VARCHAR, 
                                                                                    vehicle_type_code1 VARCHAR,
                                                                                    latitude FLOAT,
                                                                                    longitude FLOAT
                                                                                    );"""
                                   )
                              )
        except Exception as e:
            print(e) 
        return True
    
    if check_tbl_exist() is True:
        df = pd.read_parquet("./transformed/transformed_vehicle_collision_dataset.parquet")
        # load data to table
        df.to_sql("vehicle_collision", con=connection, if_exists='append', index=False)
            
    return "data is loaded into a data warehouse."