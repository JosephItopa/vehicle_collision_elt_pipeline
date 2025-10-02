import requests
import pandas as pd

def extract_from_api(no_of_request):
    # vehicle collision data [2million]
    no_of_request=50000
    url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=".format(no_of_request)
    response = requests.get(url)
    data_list = pd.read_json(response.text, typ='series').to_list()
    df = pd.DataFrame(data_list)
        #
    df = df[['collision_id', 'crash_date', 'crash_time', 'on_street_name', 'number_of_persons_injured',\
         'number_of_persons_killed', 'number_of_pedestrians_injured', 'number_of_pedestrians_killed',\
         'number_of_cyclist_injured', 'number_of_cyclist_killed','number_of_motorist_injured',\
         'number_of_motorist_killed', 'contributing_factor_vehicle_1',  'vehicle_type_code1', 'latitude','longitude'\
        ]]
    df.to_parquet("./raw/raw_vehicle_collision_dataset.parquet")
    return "extraction is done."