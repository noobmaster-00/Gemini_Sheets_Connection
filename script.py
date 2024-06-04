import os
import pickle
import gspread
import requests
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import google.generativeai as gemini
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


gemini.configure(api_key='AIzaSyAocN349OOEGoBAxrqz5ypiH1-rK8Xovjc')  

def authenticate_google_sheets():
    creds = None
    
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
   
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    client = gspread.authorize(creds)
    return client

def get_company_details(url, detail_type):
    prompt = f"Extract the {detail_type} from the website at {url}"
    try:
        response = gemini.generate_text(
            model="models/text-bison-001",
            prompt=prompt,
        )
        return response.candidates[0]['output'].strip()
    except Exception as e:
        print(f"An error occurred while querying Gemini API: {e}")
        return None

def extract_relevant_info(column, text):
    if column.lower() in ['ceo', 'founder']:
       
        match = re.search(r'\b[A-Z][a-z]* [A-Z][a-z]*\b', text)
        if match:
            return match.group()

    return text

def set_cell_color(sheet, cell, color):
    sheet.format(cell, {
        "backgroundColor": {
            "red": color[0],
            "green": color[1],
            "blue": color[2]
        }
    })

def process_record(record, headers, i):
    company_name = record['Company Name']
    website = record['Website']
    url_identification = record.get('URL Identification', '')

   
    if any(record.get(header) for header in headers[3:]):
        return i, None, []

   
    url_correct = True

    print(f"Processing {company_name} from {website}...")

    updates = []
   
    for column in headers[3:]:
        detail = get_company_details(website, column.lower())
        if detail:
            relevant_info = extract_relevant_info(column, detail)
            updates.append((i + 2, headers.index(column) + 1, relevant_info))
            print(f"Updated {column} for {company_name} with: {relevant_info}")
        else:
            url_correct = False

    if not url_correct:
        updates.append((i + 2, headers.index('URL Identification') + 1, 'Incorrect URL'))
        print(f"Marked {company_name} as Incorrect URL due to scraping failure.")

    return i, url_correct, updates

def main():
    client = authenticate_google_sheets()
    sheet = client.open('SBG - Company Categorization').sheet1  

   
    records = sheet.get_all_records()

  
    headers = sheet.row_values(1)

   
    df = pd.DataFrame(records)


    all_updates = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_record, record, headers, i) for i, record in enumerate(records)]
        for future in as_completed(futures):
            i, url_correct, updates = future.result()
            if updates:
                all_updates.extend(updates)
                if not url_correct:
                    cell = f'C{i + 2}'
                    set_cell_color(sheet, cell, (1, 0, 0)) 

    
    cell_updates = [gspread.Cell(row, col, value) for row, col, value in all_updates]
    sheet.update_cells(cell_updates)

if __name__ == '__main__':
    main()
