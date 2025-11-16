"""
Written by Nicolas Calo

Script for data mining on the Air France KLM "https://api.airfranceklm.com/opendata/flightstatus/" API.

This script requires an afklm_api_keys.txt file containing the personal API keys (one on each line) to use to query the API.

An additional df_call_parameters.csv file can be provided to set up the parameters to query. This csv should contain a dataframe specifying all the combinations of values needed to be retrieved (one query for each row of the dataframe)

The script will iterate over:
- each row of the dataframe for the parameters (or use a single set of default values if no csv provided)
- each page of the query results
- each provided API keys when a key daily allotment has been totally consumed

For each query, a .json file named according to the parameters of the query and page number will be produced in the /data folder. Upon subsequent runs of this script, it will skip any API calls for which there is already a corresponding .json file (page nb and call parameter) 

!!! define max_page_to_fetch to limit the number of pages to retrieve
!!! Always define dates otherwise it will get the current date and tracking of what has already been retrieved will not be ensured!!!
"""

### Library import
import pandas as pd
import requests
import re
import time
import json
import os
import datetime
from colorama import Fore, Style
import gzip
import base64
import pandas as pd
import requests
import re
import time
import json
import os
from io import BytesIO
import datetime
from colorama import Fore, Style
import gzip
from google.cloud import storage
from dotenv import load_dotenv
import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
import google
import logging
import google.cloud


### GCP parameters
PROJECT_ID = "trusty-anchor-473006-u9"
bucket_name = "airfrance-bucket"


pd.set_option('future.no_silent_downcasting', True)

### Script parameters
path_data_storage = "data"
path_call_parameter_file_folder = "call_parameter_lists"
skip_previously_failed_serverError = True
skip_previously_failed_flightNotFound = True
skip_previously_failed_otherErrors = True
api_key_list_folder = "api_keys"
skip_complete = True
add_new_dates_csv_parameters = True

future_days_to_retrieve = 30

max_daily_api_call = 100 # API limited to 1 call / s, 100 / day
max_page_to_fetch = 10000000000
pageNumberStart = 0
page_max = 100000  # Will auto-adjust after first page retrieved
refresh_stats = False
time_delay_query = 0 # to increase time between queries. If 0, will anyway check for 1.1 seconds between calls

non_parameters = [
    "call_parameters", "response", "message", "timestamp",
    "nb_of_pages_already_retrieved", "totalPages", "completion", "totalFlights"
]

pd.options.mode.chained_assignment = None  # suppress warnings

# configure logger


console_handler = logging.StreamHandler()
formatter = logging.Formatter(
    fmt='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler.setFormatter(formatter)
logger = logging.getLogger("extraction_app_logger")
logger.setLevel(logging.INFO)

try:
    client = google.cloud.logging.Client(project=PROJECT_ID)
    cloud_handler = CloudLoggingHandler(client)
    logger.addHandler(cloud_handler)

    # Initialisation GCS client
    client_storage = storage.Client()
    bucket = client_storage.bucket(bucket_name)

    # loading envirement variables
    load_dotenv()

    in_cloud = True


except:
    in_cloud = False
    bucket = None


logger.addHandler(console_handler)
logger.propagate = False







### general functions for GCP/local handling

def import_csv(path_folder:str,path_file:str, bucket = bucket):
    if in_cloud:
        csv_blob = bucket.blob(path_file)
        csv_data = csv_blob.download_as_bytes()         

        data =  pd.read_csv(BytesIO(csv_data),encoding="utf-8",low_memory=False)

    else:    

        data =  pd.read_csv('/'.join([path_folder,path_file]),low_memory=False)
    return data


def save_csv(df, path_folder:str,path_file:str, bucket = bucket) -> None:
    if in_cloud:

        csv_blob = bucket.blob(path_file)
        csv_buffer = BytesIO(bytes(df.to_csv(index=False), encoding='utf-8'))
        
        csv_blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
        # logger.info(f"{path_file} updated")
       
    else:    
        df.to_csv('/'.join([path_folder,path_file]),index = 0)
    return None


def append_to_csv(row, path_folder:str,path_file:str, bucket = bucket) -> None:

    if in_cloud:
        
        csv_blob = bucket.blob(path_file)
        csv_data = csv_blob.download_as_bytes()         

        data =  pd.read_csv(BytesIO(csv_data),encoding="utf-8",low_memory=False)
        df = pd.concat([data, row])

        csv_blob = bucket.blob(path_file)
        csv_buffer = BytesIO(bytes(df.to_csv(index=False), encoding='utf-8'))
        
        csv_blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")



        # logger.info(f"{path_file} updated")

    else:
        with open(f"{path_folder + "/" + path_file}","a") as f:
            row_new.to_csv(f, header=False,index = 0, lineterminator='\n')
    
    return None



def info_message(text:str, color:str=None, level_info:str=None) -> None:
    
    if color is None:
        print(Fore.RESET + text)
    else:
        print(eval(f'Fore.{color.upper()}') + text)
    '''
    if in_cloud:

        match level_info:
            case 'info':
                logger.info(text)
            case 'warning':
                logger.warning(text)
            case 'error':
                logger.error(text)
            case _:
                logger.info(text)
    '''
    return None




def list_json_files(path_data_storage:str, bucket = bucket) -> list:
    if in_cloud:
        json_list_blobs = client_storage.list_blobs(bucket, prefix=path_data_storage)
        json_list = [val.name for val in json_list_blobs if 'json' in val.name]
    
    else:
        json_list = os.listdir(path_data_storage)
        json_list = [val for val in json_list if 'json' in val]

    
    json_list.sort()

    return json_list



def save_and_compress_json(path_data_storage:str,json_to_make:str, bucket = bucket) -> None:

    if in_cloud:

        gzip_blob_name = f"{path_data_storage}/{json_to_make}.gz"    
        gzip_blob = bucket.blob(gzip_blob_name)
        buffer=BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gzip_file:
            gzip_file.write(json.dumps(data,ensure_ascii=False,indent=4).encode("utf-8"))
        
        gzip_blob.upload_from_file(BytesIO(buffer.getvalue()))
        # logger.info(f"blob:'{gzip_blob_name}' uploaded")

    else:
        with gzip.open(f"{path_data_storage}/{json_to_make}.gz", 'wt', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    return None


def open_json(path_data_storage:str,file_to_open:str, bucket = bucket) -> None:

    if in_cloud:

        gzip_blob_name = f"{path_data_storage}/{json_to_make}.gz"    
        gzip_blob = bucket.blob(gzip_blob_name)
        gzip_data = gzip_blob.download_as_bytes()
        buffer=BytesIO(gzip_data)
        with gzip.GzipFile(fileobj=buffer) as gz :
            data = json.load(gz)

    else:
        with gzip.open(f"{path_data_storage}/" + file_to_open) as json_file:
            data = json.load(json_file)
    
    return data




def list_api_files(api_key_list_folder:str, bucket = bucket) -> list:

    if in_cloud:
        api_file_list = client_storage.list_blobs(bucket,prefix=api_key_list_folder)
        api_file_list = [val.name for val in api_file_list if (len(re.findall("csv$",val.name)) > 0)]
    
    else:
        api_file_list = os.listdir(api_key_list_folder)
        api_file_list = [val for val in api_file_list if (len(re.findall("csv$",val)) > 0)]
    
    return api_file_list

    

def list_call_parameters(path_call_parameter_file_folder:str, bucket = bucket) -> list:

    if in_cloud:
        path_call_parameter_csv_list = client_storage.list_blobs(bucket,prefix=path_call_parameter_file_folder)
        call_parameter_csv_list = [val.name for val in path_call_parameter_csv_list if ('df_call_parameters'  in val.name) & (len(re.findall("csv$",val.name)) > 0)]
    
    else:
        call_parameter_csv_list = os.listdir(path_call_parameter_file_folder)
        call_parameter_csv_list = [val for val in call_parameter_csv_list if ('df_call_parameters'  in val) & (len(re.findall("csv$",val) )> 0

)]
    
    call_parameter_csv_list.sort()

    return call_parameter_csv_list

    



### Working directory adjustments

if not in_cloud:

    cwd = os.getcwd()
    if cwd.endswith("DST_DE_Airlines"):
        os.chdir("1_data_collection/afklm_api_collection")
    elif cwd.endswith("1_data_collection"):
        os.chdir("afklm_api_collection")
    else:
        script_path = os.path.dirname(os.path.realpath(__file__))
        os.chdir(script_path)
    
    ### Create folder for retrieved data
    os.makedirs(path_data_storage, exist_ok=True)

### List of already retrieved data and parameter CSV files

call_parameter_csv_list = list_call_parameters(path_call_parameter_file_folder=path_call_parameter_file_folder,bucket=bucket)
json_list = list_json_files(path_data_storage,bucket)



### To update with functions that append results


if add_new_dates_csv_parameters :
    for call_parameter_csv in call_parameter_csv_list:


    
    ### Update with new dates when all pages of current parameter file retrieved or failed
        info_message(f"adding missing dates to {call_parameter_csv}")

        df_call_parameters = import_csv(path_folder = path_call_parameter_file_folder, path_file = call_parameter_csv).fillna('').sort_values(['endRange','completion'])
        
        df_call_parameters_root = df_call_parameters.drop(non_parameters,axis =1 ,errors='ignore').drop_duplicates()
        
        params = list(df_call_parameters_root.columns)
        params.remove('startRange')
        params.remove('endRange')
        
        df_call_parameters_root = df_call_parameters_root.drop('startRange',axis=1).groupby(params).max().reset_index()
        
        for index, row in df_call_parameters_root.iterrows():
            
            endRange = str(row.endRange)
            endRange = endRange.replace('Z','')

            
            while ((datetime.datetime.fromisoformat(endRange).date() - datetime.datetime.now().date() ).days) < future_days_to_retrieve :
            
                row_new =  df_call_parameters_root.iloc[[index]].copy()
                row_new['startRange'] = (datetime.datetime.fromisoformat(endRange) + datetime.timedelta(seconds=1)).isoformat() + "Z"
                endRange = (datetime.datetime.fromisoformat(endRange) + datetime.timedelta(days=1)).isoformat()
                row_new['endRange'] = endRange + "Z"
                

                df_call_parameters = pd.concat([df_call_parameters, row_new], ignore_index=True)

                endRange = row_new['endRange'].item().replace('Z','')
                
        
        save_csv(df_call_parameters,
            path_folder = path_call_parameter_file_folder,
            path_file = call_parameter_csv)


        info_message(f"adding missing dates to {call_parameter_csv} over")
        




### Load API keys




if in_cloud:

    API_key_file_list = list_api_files(api_key_list_folder=api_key_list_folder,bucket=bucket)[0]
    API_key_list_cleaned = import_csv(path_folder = api_key_list_folder,path_file = API_key_file_list)



    api_key_list = os.getenv("API_KEYS")

    api_key_list = api_key_list_test.split(',')

    for key in api_key_list_test:
        key_desc = key.split(":")[0]
        key_value = key.split(":")[1]

        API_key_list_cleaned["api_key"] = API_key_list_cleaned.apply(
            lambda row: key_value
            if row["key_desc"] == key_desc
            else row["api_key"],
            axis=1,
        )
else:


    API_key_list_cleaned = pd.DataFrame()
    for file in os.listdir(api_key_list_folder):
        if file.endswith(".csv"):
            API_key_list = import_csv(api_key_list_folder, file)
            API_key_list_cleaned = pd.concat([API_key_list_cleaned, API_key_list], ignore_index=True)

    API_key_list_cleaned = (
        API_key_list_cleaned.sort_values("timestamp")
        .drop_duplicates(subset="key_desc", keep="last")
    )

    API_key_list_cleaned["timestamp"] = API_key_list_cleaned["timestamp"].fillna(datetime.datetime.now().isoformat())

    API_key_list_cleaned["nb_calls_today"] = API_key_list_cleaned.apply(
        lambda row: 0
        if (datetime.datetime.now().date() - datetime.datetime.fromisoformat(row["timestamp"]).date()).days > 0
        else row["nb_calls_today"],
        axis=1,
    )
    API_key_list_cleaned["timestamp"] = API_key_list_cleaned.apply(
        lambda row: datetime.datetime.now().isoformat()
        if (datetime.datetime.now().date() - datetime.datetime.fromisoformat(row["timestamp"]).date()).days > 0
        else row["timestamp"],
        axis=1,
    )






last_call_time = datetime.datetime.now()
char = " "



for index, record in API_key_list_cleaned.iterrows():
    
    
        
    API_key = record['api_key']
    nb_calls_today = record['nb_calls_today']
    key_desc = record['key_desc']

    
    
    print("")
    info_message(f"{key_desc}")
    
    if nb_calls_today == max_daily_api_call:
        info_message(f"-> Daily call quota reached. Trying next API key",color='yellow',level_info='warning')
        
        continue
    
    info_message(f"{max_daily_api_call - nb_calls_today} / 100 API calls left for today",'green')
    print("")
    for call_parameter_csv in call_parameter_csv_list:

        
        
        info_message("#"*90+ "\n"+call_parameter_csv+ "\n"+"#"*90+ "\n")

        
        ### Import query parameters
        df_call_parameters = import_csv(
            path_call_parameter_file_folder ,call_parameter_csv
        ).fillna('')

        call_parameters_list = []

        for i in range(len(df_call_parameters)):
            df_subset_parameter = df_call_parameters.iloc[[i]].to_dict(orient="list")
            call_parameters_url = "&".join(
                [key + "=" + str(val[0]) for key, val in df_subset_parameter.items()
                if val[0] != '' and val[0] != '[nan]']
            )
            call_parameters_list.append(call_parameters_url)

        df_call_parameters['call_parameters'] = call_parameters_list
        

        ### Loading API keys to use
        
        

        ### Definition of base URLs for API call
        base_url = "https://api.airfranceklm.com/opendata/flightstatus/?"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        ### Definition of default parameters for API call
        dict_call_parameters = {
            "aircraftRegistration": '',  # string Registration code of the aircraft
            "aircraftType": '',  # string Filter by a type of aircraft
            "arrivalCity": '',  # string Filter by airport code of arrival city
            "carrierCode": [],  # array[string] Airline code
            "consumerHost": '',  # string System info from which request is launched
            "departureCity": '',  # string IATA departure city code
            "destination": '',  # string Destination airport
            "flightNumber": '',  # string Filter by flight number
            "movementType": '',  # string Focus (Departure/Arrival)
            "operatingAirlineCode": [],  # array[string] Operating airline code
            "operationalSuffix": '',  # string Operational suffix
            "origin": '',  # string Departure airport
            "serviceType": [],  # array[string] IATA service type code
            "timeOriginType": '',  # string S/M/I/P
            "timeType": '',  # string U/L
            "endRange": '2025-07-23T23:59:59Z',  # string<date-time>
            "startRange": '2025-07-21T09:00:00Z',  # string<date-time>
            "call_parameters": '',  # repopulated after request
            'response': '',  # repopulated after request
            'message': '',  # repopulated after request
            'timestamp': '',  # repopulated after request
            'nb_of_pages_already_retrieved': '',  # repopulated after request
            'totalPages': '',  # repopulated after request
            'completion': ''  # repopulated after request
        }

        dict_call_parameters["carrierCode"] = ",".join(dict_call_parameters['carrierCode'])
        dict_call_parameters["operatingAirlineCode"] = ",".join(dict_call_parameters['operatingAirlineCode'])
        dict_call_parameters["serviceType"] = ",".join(dict_call_parameters['serviceType'])

        df_call_parameters_defaults = pd.DataFrame(dict_call_parameters, index=[0])  # from defaults

        try:
            df_call_parameters = import_csv(path_call_parameter_file_folder,call_parameter_csv).fillna('')
            save_csv(df_call_parameters,
            path_folder = path_call_parameter_file_folder,
            path_file = call_parameter_csv.replace(".csv",".bak"))

        except:
            try:
                df_call_parameters = import_csv(path_call_parameter_file_folder,call_parameter_csv.replace(".csv",".bak")).fillna('')
                save_csv(df_call_parameters,
                    path_folder = path_call_parameter_file_folder,
                    path_file = call_parameter_csv)
            except:
                df_call_parameters = df_call_parameters_defaults
        



        info_message( f"Max number of pages to retrieve: {max_page_to_fetch} ")


        info_message( f"Number of API call parameters to process = {len(df_call_parameters)}")

            
    
        i = 0
        df_call_parameters_new = df_call_parameters.copy(deep=True)  # to update the CSV after each query



        ### Loop over the CSV file containing the parameter list to send to the API
        for i in range(0, len(df_call_parameters)):
            to_test = float(df_call_parameters.iloc[[i]]['completion'].replace('','0').item())
            
            if skip_complete & (to_test == 100):
                continue
            print("")
            df_subset = df_call_parameters.iloc[[i]].copy(deep=True).reset_index().drop(['index'], axis=1)
            pageNumber = pageNumberStart  # first page is 1; page 0 returns same results

            ### Check if query parameter already tested and skip previously failed if chosen
            text_response = str(df_subset['response'])
            match_error = re.search("\\d\\d\\d", text_response)
            if match_error is None:
                match_error = "000"
            else:
                match_error = match_error[0]

            ### Cleaning of empty parameter calls
            parameter_list = df_subset.drop(non_parameters, axis=1, errors='ignore').columns.to_list()
            dict_call_parameters = df_subset.drop(non_parameters, axis=1, errors='ignore').to_dict(orient="list")
            call_parameters_url = "&".join([key + "=" + str(val[0])
                                            for key, val in dict_call_parameters.items()
                                            if val[0] != '' and val[0] != '[nan]'])

            info_message(f"{call_parameters_url}")

            

            if skip_previously_failed_serverError & int(match_error) > 200:
                if int(match_error) >= 500:
                    message = f"skipped because previously failed due to server error"

                if int(match_error) == 404:
                    message = f"skipped because previously obtained 'FLIGHT NOT FOUND'"
                
                if int(match_error) > 200:
                    message = f"skipped because previously obtained another error"

                info_message(f"skipped because previously failed due to server error",'magenta','warning')
                continue
                
            

            url = (base_url + call_parameters_url).replace(" ", "")

            ### Check date query coherence
            if df_subset['endRange'].item() < df_subset['startRange'].item():
                info_message("ERROR: endRange < startRange",'red','error')
                break

            ### Loop until desired number of pages or max pages reached
            while (pageNumber + 1 <= page_max) & (pageNumber + 1 <= max_page_to_fetch) & (nb_calls_today < 101):

                json_to_make_root = f"afklm_api_data_collection_{re.sub(':', '_', call_parameters_url)}"
                
                
                date_diff = (datetime.datetime.fromisoformat(df_subset['startRange'].item()).date() - datetime.datetime.date(datetime.datetime.now())).days
                
                if date_diff > 0:
                    json_to_make = json_to_make_root + f"_{pageNumber}_sched.json"
                elif date_diff == 0:
                    json_to_make = json_to_make_root + f"_{pageNumber}_updSchedD1.json"
                else:
                    json_to_make = json_to_make_root + f"_{pageNumber}.json"
                    

                df_totalPages = df_subset['totalPages']
                df_item = df_subset['totalPages'].item()
                df_values = df_subset['totalPages'].values
                time_analysis = datetime.datetime.now().isoformat()

                # Skip current query if file already exists
                if (json_to_make in json_list) | (json_to_make + '.gz' in json_list):
                    if df_item == '':
                        info_message("loading page info from already retrieved files",'blue')
                        file_to_open = [file for file in json_list if json_to_make in file][0]
                        data = open_json(path_data_storage,file_to_open,bucket)
                        page_max = data['page']['totalPages']
                        df_subset.loc[0, ['totalPages']] = page_max
                    else:
                        page_max = df_item

                    info_message(f"Page {pageNumber} : skipped because already retrieved",'blue','info')
                    if (page_max == pageNumber + 1):
                        info_message(f"All pages already retrieved",'blue','info')
                        df_subset.loc[0, ['nb_of_pages_already_retrieved']] = df_subset.loc[0, ['totalPages']].item()
                        df_subset.loc[0, ['completion']] = 100
                        df_subset.loc[0, ['timestamp']] = time_analysis

                        df_subset.loc[0, ['call_parameters']] = call_parameters_url
                        df_call_parameters_new = pd.concat([df_call_parameters_new, df_subset], ignore_index=True)
                        df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=parameter_list, keep='last')
                        df_call_parameters_new = df_call_parameters_new.fillna('').sort_values(['endRange','completion'])
                        save_csv(

                            
                            df_call_parameters_new, path_folder=path_call_parameter_file_folder,path_file=call_parameter_csv, bucket = bucket
                             
                            )
                    pageNumber += 1
                    continue

                # Main API request logic
                
                headers['API-Key'] = API_key
                url_page = (url + f"&{pageNumber=}").replace("?&", "?")
                
                
                while (datetime.datetime.now() - last_call_time).seconds < 1.1:
                    time.sleep(0.1)
                    
                time.sleep(time_delay_query)
                
                response = requests.get(url_page, headers=headers)
                
                last_call_time = datetime.datetime.now()
                
                
                nb_calls_today = nb_calls_today + 1
                
                API_key_list_cleaned['nb_calls_today'] = API_key_list_cleaned.apply(lambda row: nb_calls_today if row['key_desc'] == key_desc else row['nb_calls_today'] , axis=1)

                if in_cloud:
                    API_key_list_cleaned['api_key'] = API_key_list_cleaned.apply(lambda row: 'SECRET' , axis=1)

                save_csv(API_key_list_cleaned,
                         path_folder=api_key_list_folder,
                         path_file="afklm_api_keys.csv",
                        bucket = bucket)
                

                
                df_subset.loc[0, ['timestamp']] = time_analysis
                df_subset.loc[0, ['call_parameters']] = call_parameters_url

                if response.__bool__():
                    data = response.json()
                    page_max = data['page']['totalPages']
                    fullCount = data['page']['fullCount']

                    save_and_compress_json(path_data_storage,json_to_make, bucket)
                    
                    info_message(f"Page {pageNumber} : retrieval OK    Total: {page_max}",'green','info')

                    if df_subset['nb_of_pages_already_retrieved'].item() == '':
                        df_subset.loc[0, ['nb_of_pages_already_retrieved']] = 0

                    if int(pageNumber + 1) > int(df_subset['nb_of_pages_already_retrieved'].item()):
                        df_subset.loc[0, ['nb_of_pages_already_retrieved']] = float(f"{(pageNumber+1):.0f}")

                    df_subset.loc[0, ['response']] = str(response)
                    df_subset.loc[0, ['totalPages']] = float(f"{(page_max):.0f}")
                    df_subset.loc[0, ['totalFlights']] = float(f"{(fullCount):.0f}")
                    df_subset.loc[0, ['completion']] = float(f"{100*(pageNumber+1)/page_max:.0f}")
                    df_subset.loc[0, ['message']] = ""

                    df_call_parameters_new = pd.concat([df_call_parameters_new, df_subset], ignore_index=True)
                    df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=parameter_list, keep='last')
                    df_call_parameters_new = df_call_parameters_new.fillna('')
                    save_csv(
                        df_call_parameters_new, path_folder=path_call_parameter_file_folder,path_file=call_parameter_csv,
                        bucket = bucket)


                    pageNumber += 1

                elif ("Developer" in response.text):
                    info_message("API daily quota consumed",'red','warning')

                    nb_calls_today = 100
                    

                    API_key_list_cleaned['nb_calls_today'] = API_key_list_cleaned.apply(lambda row: 100 if row['key_desc'] == key_desc else row['nb_calls_today'] , axis=1)
                    API_key_list_cleaned['timestamp'] = API_key_list_cleaned.apply(lambda row: time_analysis if row['key_desc'] == key_desc else row['timestamp'] , axis=1)

                    API_key_list_cleaned = API_key_list_cleaned.drop_duplicates(subset='api_key',keep='last')

                    if in_cloud:
                        API_key_list_cleaned['api_key'] = API_key_list_cleaned.apply(lambda row: 'SECRET' , axis=1)

                    save_csv(API_key_list_cleaned, api_key_list_folder, "afklm_api_keys.csv")
                    

                    break

                else:
                    info_message(f"Issues with the call: {response} {response.text}",'red','warning')
                    df_subset.loc[0, ['response']] = str(response)
                    df_subset.loc[0, ['message']] = str(response.text)
                    df_call_parameters_new = pd.concat([df_call_parameters_new, df_subset], ignore_index=True)
                    df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=['call_parameters'], keep='last')
                    df_call_parameters_new = df_call_parameters_new.fillna('').sort_values(['endRange','completion'])
                    
                    save_csv(
                        df_call_parameters_new, path_folder=path_call_parameter_file_folder,path_file=call_parameter_csv,
                        bucket = bucket)
                    break
            
            if nb_calls_today == 100:
                break

        
            

