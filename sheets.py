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

def dataframe_to_sheet(spreadsheet_id, sheet_name, df, fail_if_exists=True):


    # Try adding a sheet to the given spreadsheet; if one already exists, either update an existing sheet or fail,
    # depending on the fail_if_exists flag
    try:
        res = add_sheet(spreadsheet_id, sheet_name)
        sheet_id = res['spreadsheetId']
    except Exception:
        if fail_if_exists:
            raise
        else:
            # TODO: Need to have this clear all the data in the sheet
            sheet_id = [x['properties']['sheetId']
                        for x in service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()['sheets']
                        if x['properties']['title'] == sheet_name][0]

    # Turn the dataframe into a string (for pasteData) and an array of strings (to get shape of headers)
    df_string = df.to_csv(sep='|')[:-1]
    string_arr = np.array([x.split('|') for x in df_string.split('\n')])

    data_dim = np.shape(df.values)
    table_dim = np.shape(string_arr)
    column_header_dim = (table_dim[0]-data_dim[0], table_dim[1])
    row_header_dim = (table_dim[0]-column_header_dim[0], table_dim[1]-data_dim[1])

    column_header_range = ((0, 0), (column_header_dim[0], column_header_dim[1]))
    row_header_range = ((column_header_dim[0], 0), (table_dim[0], row_header_dim[1]))


    requests = []
    if column_header_dim[0] > 0 and column_header_dim[1] > 0:
        column_header_request = {'repeatCell': {
            'fields': '*',
            'range': {
                'startRowIndex': column_header_range[0][0],
                'startColumnIndex': column_header_range[0][1],
                'endRowIndex': column_header_range[1][0],
                'endColumnIndex': column_header_range[1][1],
                'sheetId': sheet_id
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': {
                        'blue': .33,
                        'green': .33,
                        'red': .33,
                    },
                    'horizontalAlignment': 'CENTER',
                    'textFormat': {
                        'foregroundColor': {
                            'red': 1.0,
                            'blue': 1.0,
                            'green': 1.0
                        },
                        'bold': False
                    }
                }
            }
        }}
        requests.append(column_header_request)

    if row_header_dim[0] > 0 and row_header_dim[1] > 0:
        row_header_request = {'repeatCell': {
            'fields': '*',
            'range': {
                'startRowIndex': row_header_range[0][0],
                'startColumnIndex': row_header_range[0][1],
                'endRowIndex': row_header_range[1][0],
                'endColumnIndex': row_header_range[1][1],
                'sheetId': sheet_id
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': {
                        'blue': .54,
                        'green': .54,
                        'red': .54,
                    },
                    'horizontalAlignment': 'CENTER',
                    'textFormat': {
                        'foregroundColor': {
                            'red': 1.0,
                            'blue': 1.0,
                            'green': 1.0
                        },
                        'bold': False
                    }
                }
            }
        }}
        requests.append(row_header_request)

    fill_data_request = {'pasteData': {
        'coordinate': {
            'rowIndex': 0,
            'columnIndex': 0,
            'sheetId': sheet_id
        },
        'delimiter': '|',
        'data': df_string
    }}
    requests.append(fill_data_request)

    requests = {'requests': [column_header_request, row_header_request, fill_data_request]}

    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=requests).execute()



if __name__ == '__main__':
    df = pd.DataFrame([[1,2,3,4],[5,6,7,8]], columns=['a', 'b', 'c', 'd']).set_index(['a', 'b'])
    df['e'] = ['=C2*D2', '=C3*D3']
    dataframe_to_sheet('112yP5RKg-cLy3UAgHr3o46b-AmOvXfGtOzprOwNtNhY', 'wint', df, fail_if_exists=False)
