from airtable_fetch import fetch_psl_data, convert_to_df
import pandas as pd
import os
from datetime import datetime as dt


def group_amazon_orders(df):
    # Filter Amazon orders
    amazon_df = df[df['Vendor'].str.contains('Amazon')]

    # Aggregate Amazon orders into a single line
    amazon_aggregated = {
        'Date': amazon_df['Date'].max(),  # Use the latest date
        'Document Number': amazon_df['Document Number'].min(),  # Use the first document number
        'Vendor': '1716 Amazon',
        'Current Status': amazon_df['Current Status'].iloc[0],  # Assuming all status are the same
        'Total (Transaction)': amazon_df['Total (Transaction)'].sum(),  # Sum all transactions
        'Memo': 'Weekly Amazon orders'
    }

    # Create a DataFrame from the aggregated Amazon order
    amazon_summary_df = pd.DataFrame([amazon_aggregated])

    # Filter out non-Amazon orders
    non_amazon_df = df[~df['Vendor'].str.contains('Amazon')]

    # Combine non-Amazon orders with the aggregated Amazon order
    combined_df = pd.concat([non_amazon_df, amazon_summary_df], ignore_index=True)

    return combined_df

today = dt.today().strftime('%m.%d.%Y')

airtable_base_id = os.getenv("PO_BASE_ID")
airtable_table_id = os.getenv("PO_TABLE_ID")
airtable_token = os.getenv('AIRTABLE_PERSONAL_TOKEN')

weekly_po_requests = fetch_psl_data(airtable_base_id, airtable_table_id, airtable_token)
po_df = convert_to_df(weekly_po_requests)

# Filter pending approval requests
pending_df = po_df[(po_df['Status'] == 'Pending Approval') | (po_df['Status'] == 'Urgent')].copy()

# # Ensure the columns are in numeric format for calculations
# pending_df.loc[:, 'Unit Cost'] = pd.to_numeric(pending_df['Unit Cost'], errors='coerce')
# pending_df.loc[:, 'Quantity'] = pd.to_numeric(pending_df['Quantity'], errors='coerce')

# Calculate the cost
# pending_df.loc[:, 'Calculated Cost'] = pending_df['Unit Cost'] * pending_df['Quantity']

# Concatenate Vendor ID and Vendor Name to create a Vendor column
pending_df.loc[:, 'Vendor'] = pending_df['Vendor ID'].astype(str) + " " + pending_df['Vendor Name']

# Convert the Date column to datetime format with the correct format
pending_df.loc[:, 'Date'] = pd.to_datetime(pending_df['Date'], format='%Y-%m-%d', errors='coerce')

# Check if pending_df is empty after filtering
if pending_df.empty:
    print("No pending approvals found.")
else:
    # Group by relevant columns and aggregate costs
    grouped_df = pending_df.groupby(['PO #']).agg({'Date':'max', 'Vendor':'max', 'Status':'max', 'Note':'max', 'Cost': 'sum'}).reset_index()

    # Display the grouped data
    grouped_df.rename(columns={'PO #':'Document Number', 'Status':'Current Status', 'Cost':'Total (Transaction)', 'Note':'Memo'}, inplace=True)
    grouped_df = grouped_df[['Date','Document Number', 'Vendor','Current Status', 'Total (Transaction)', 'Memo']]
    # Convert the Date column to the desired format for display or export
    grouped_df['Date'] = grouped_df['Date'].dt.strftime('%m/%d/%Y')
    grouped_df['Current Status'] = grouped_df['Current Status'].replace('Pending Approval',
                                                                        'Pending Supervisor Approval')
    grouped_df = grouped_df.sort_values(['Date', 'Document Number'], ascending=[False, True])

    # group amazon orders
    result_df = group_amazon_orders(grouped_df)

    print(grouped_df)
    result_df.to_excel(f"ANA Weekly PO Request {today}.xlsx", index=False)