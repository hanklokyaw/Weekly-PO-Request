import requests
import pandas as pd

def fetch_psl_data(base_id, table_id, token):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    base_url = f'https://api.airtable.com/v0/{base_id}/{table_id}'

    records = []
    offset = None
    while True:
        url = base_url
        if offset:
            url = f'{base_url}?offset={offset}'
        response = requests.get(url, headers=headers)
        data = response.json()
        records.extend(data.get('records', []))
        offset = data.get('offset')
        if not offset:
            break

    return records

def convert_to_df(psl_records):
    # Convert Airtable data to DataFrame
    psl_data = []
    for record in psl_records:
        fields = record['fields']
        psl_data.append({
            'id': fields.get('id'),
            'PO #': fields.get('PO #'),
            'Date': fields.get('Date'),
            'Vendor ID': fields.get('Vendor ID'),
            'Vendor Name': fields.get('Vendor Name'),
            'Unit Cost': fields.get('Unit Cost'),
            'Quantity': fields.get('Quantity'),
            'Cost': fields.get('Cost'),
            'Note': fields.get('Note'),
            'Status': fields.get('Status')
        })

    psl_df = pd.DataFrame(psl_data)
    return psl_df