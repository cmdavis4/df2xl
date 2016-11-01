import os
import json
import httplib2
import pdb
import pandas as pd
import numpy as np

from xlsxwriter.utility import xl_rowcol_to_cell, xl_cell_to_rowcol
from apiclient import discovery
from oauth2client import client, tools
from oauth2client.file import Storage


SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

##############################
# 'SPREADSHEET' -> WORKBOOK
# 'SHEET'-> SINGLE TAB OF WORKBOOK
# I DON'T MAKE THE RULES I JUST FOLLOW THEM
##############################


try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def get_service():

    http = get_credentials().authorize(httplib2.Http())
    discovery_url = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
    service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discovery_url)
    return service

service = get_service()

def create_spreadsheet(name):

    data = {'properties': {'title': name}}
    return service.spreadsheets().create(body=data).execute()


def update_sheet(spreadsheet_id, cell_range, values):
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=cell_range,
        body={'range': cell_range, 'majorDimension': 'ROWS', 'values': values},
        valueInputOption='RAW',

    ).execute()

def add_sheet(spreadsheet_id, sheet_name):

    data = {'requests': [{'addSheet': {'properties': {'title': sheet_name}}}]}

    return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=data).execute()

def dataframe_to_sheet(spreadsheet_id, sheet_name, df, if_exists='silent'):


    try:
        res = add_sheet(spreadsheet_id, sheet_name)
        sheet_id = res['spreadsheetId']
    except Exception:
        if if_exists == 'silent':
            sheet_id = [x['properties']['sheetId']
                        for x in service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()['sheets']
                        if x['properties']['title'] == sheet_name][0]
        else:
            raise

    string_arr = np.array([x.split('|') for x in df.to_csv(sep='|').split('\n')[:-1]])

    table_dim = np.shape(string_arr)
    data_dim = np.shape(df.values)

    num_col_header_rows = table_dim[0] - data_dim[0]
    num_row_header_cols = table_dim[1] - data_dim[1]

    col_header = string_arr[:num_col_header_rows]
    row_header = string_arr[num_col_header_rows:, :num_row_header_cols]
    data = string_arr[num_col_header_rows:, num_row_header_cols:]

    if  num_col_header_rows > 0:
        col_header_tl = 'A1'
        col_header_br = xl_rowcol_to_cell(num_col_header_rows-1, table_dim[1]-1)  # -1 to account for 0-indexing
        col_header_range = '{}!{}:{}'.format(sheet_name, col_header_tl, col_header_br)
    else:
        col_header_range = None
    if num_row_header_cols > 0:
        row_header_tl = xl_rowcol_to_cell(num_col_header_rows, 0)
        row_header_br = xl_rowcol_to_cell(data_dim[0], num_row_header_cols-1)  # -1 to account for 0-indexing
        row_header_range = '{}!{}:{}'.format(sheet_name, row_header_tl, row_header_br)
    else:
        row_header_range = None
    data_tl = xl_rowcol_to_cell(num_col_header_rows, num_row_header_cols)
    data_br = xl_rowcol_to_cell(table_dim[0]-1, table_dim[1]-1)  # -1 to account for 0-indexing
    data_range = '{}!{}:{}'.format(sheet_name, data_tl, data_br)

    column_header_request = {'updateCells': {
        'fields': '*',
        'range': {
            'endRowIndex': xl_cell_to_rowcol(col_header_br)[0] + 1,  # Upper bound is exclusive
            'startRowIndex': xl_cell_to_rowcol(col_header_tl)[0],
            'startColumnIndex': xl_cell_to_rowcol(col_header_tl)[1],
            'endColumnIndex': xl_cell_to_rowcol(col_header_br)[1] + 1,  # Upper bound is exclusive
            'sheetId': sheet_id
        },
        'rows': [{
            'values': [{
                'userEnteredFormat': {
                    'backgroundColor': {
                        'blue': .6,
                        'green': .3,
                        'red': .1,
                        'alpha': 1
                    }
                }
            } for _ in col_header]
        }]
    }}

    requests = {'requests': [column_header_request]}
    # pdb.set_trace()
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=requests).execute()



if __name__ == '__main__':
    # create_spreadsheet('This is a dril')
    df = pd.DataFrame([[1,2,3],[4,5,6]], columns=['a', 'b', 'c']).set_index(['a', 'b'])
    dataframe_to_sheet('112yP5RKg-cLy3UAgHr3o46b-AmOvXfGtOzprOwNtNhY', 'wint', df)
    # result = add_sheet('112yP5RKg-cLy3UAgHr3o46b-AmOvXfGtOzprOwNtNhY', 'candles')
    # pdb.set_trace()
