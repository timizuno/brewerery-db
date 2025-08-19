import os
import csv
import time
import logging
import requests
from datetime import datetime
from airflow.sdk import Variable

def extract_from_dbbrewery_to_bronze():
    """
    Extract data from the db brewery api, paginate the results, and save it to a CSV file in the bronze layer/folder. 
    Parameters are set in the Airflow Variables.
    """

    page_url = Variable.get("BREWERIES_API_URL")
    max_per_page = Variable.get("BREWERIES_API_PER_PAGE")
    data_dir_base = "/opt/airflow/data" 
    container_name = Variable.get("DATA_LAKE_BRONZE_CONTAINER")

    page = 1
    header_exist = False
    
    current_date = datetime.today().strftime("%Y%m%d")
    output_dir = os.path.join(data_dir_base, container_name)
    output_file = os.path.join(output_dir, f'{current_date}_extraction_breweries.csv')
    
    # Garante que o diretório de destino exista
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info(f"File will be saved in: {output_file}")

    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        while True:
            params = {
                "per_page": max_per_page,
                "page": page
            }
            
            try:
                response = requests.get(page_url, params=params, timeout=20)
                response.raise_for_status()
                data_page = response.json()
            except requests.exceptions.RequestException as e:
                logging.error(f"Error calling the api {page}: {e}")
                # Decide se deve parar ou tentar novamente (aqui vamos parar)
                raise
            
            if not data_page:
                logging.info('Não há mais páginas para extrair.')
                break
            
            if not header_exist:
                header_csv = data_page[0].keys()
                writer.writerow(header_csv)
                header_exist = True

            rows_to_write = []
            for dictionary in data_page:
                row = [dictionary.get(h) for h in header_csv]
                rows_to_write.append(row)

            writer.writerows(rows_to_write)
            if page%5==0:
                logging.info(f'Page {page} with {len(rows_to_write)} rows saved.')
            page += 1
            time.sleep(0.5)

    logging.info('Extração para a camada Bronze finalizada com sucesso.')