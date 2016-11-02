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
# I DON;T MAKE THE RULES I JUST FOLLOW THEM
##############################


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

def create_spreadsheet(title):

    data = {'properties': {'title': title}}
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

def get_sheet_id(spreadsheet_id, sheet_name):

    try:
        return str([x['properties']['sheetId']
                         for x in service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()['sheets']
                         if x['properties']['title'] == sheet_name][0])
    except IndexError:
        raise ValueError("Sheet '{}' does not exists in spreadsheet {}".format(sheet_name, spreadsheet_id))

def push_dataframe_to_sheet(spreadsheet_id, sheet_name, df, execute=True, fail_if_exists=True):
    '''
    Function to push a single pandas DataFrame to a Google sheet. Note that fail_if_exists refers to the
    existence of the sheet with the given sheet_name, and not the whole spreadsheet. This function assumes that
    the spreadsheet into which the dataframe is to be inserted already exists. The dataframe is pushed using a
    (up to) three part batch request; one to format the column header, one to format the row header, and one to
    paste the data into the sheet.
    :param spreadsheet_id: str, for now must be the actual id and not the name
    :param sheet_name: str, name of the sheet into which the DataFrame should be inserted
    :param df: pd.DataFrame
    :param execute: boolean, executes the requests if True, else just returns them in a list
    :param fail_if_exists: boolean, whether to raise an error if a sheet named sheet_name already exists in this
    spreadsheet
    :return: dict (the response from Google) if execute else list[dict (request)]
    '''

    # Try adding a sheet to the given spreadsheet; if one already exists, either update an existing sheet or fail,
    # depending on the fail_if_exists flag
    try:
        res = add_sheet(spreadsheet_id, sheet_name)
        sheet_id = str(res['replies'][0]['addSheet']['properties']['sheetId'])
    except Exception:
        if fail_if_exists:
            raise
        else:
            # TODO: Need to have this clear all the data in the sheet
            sheet_id = get_sheet_id(spreadsheet_id, sheet_name)

    # Turn the dataframe into a string (for pasteData) and an array of strings (to get shape of headers)
    df_string = df.to_csv(sep='|')[:-1]
    string_arr = np.array([x.split('|') for x in df_string.split('\n')])

    # Figure out the ranges to use for the requests
    data_dim = np.shape(df.values)
    table_dim = np.shape(string_arr)
    column_header_dim = (table_dim[0]-data_dim[0], table_dim[1])
    row_header_dim = (table_dim[0]-column_header_dim[0], table_dim[1]-data_dim[1])

    column_header_range = ((0, 0), (column_header_dim[0], column_header_dim[1]))
    row_header_range = ((column_header_dim[0], 0), (table_dim[0], row_header_dim[1]))


    requests = []

    # Column header request
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

    # Row header request
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

    # Data paste request
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

    # Resize request
    resize_request = {
        'autoResizeDimensions': {
            'dimensions': {
                'sheetId': sheet_id,
                'dimension': 'COLUMNS',
                'startIndex': 0,
                'endIndex': table_dim[1]
            }
        }
    }
    requests.append(resize_request)

    if execute:
        # Send it off
        return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()

    else:
        # if not execute, just return the list of requests (presumably so that they can be consolidated with requests
        # from other calls to this function)
        return requests


def push_analysis_to_sheets(df_list, title=None, spreadsheet_id=None, execute=True):
    '''
    Function to push multiple DataFrames into the same Google spreadsheet.
    :param df_list: list[(str, pd.DataFrame)], where str is the title of the sheet and pd.DataFrame is the content
    :param title: str, the name of the spreadsheet to create. ONLY INCLUDE THIS if you want to create a new
    spreadsheet. If both title and spreadsheet_id are passed, spreadsheet_id will be ignored, and a new spreadsheet
    will be created.
    :param spreadsheet_id: str, the id of the spreadsheet into which this data should be pushed
    :param execute: boolean, executes the requests if True, else just returns a list of requests
    :return: dict (the response from Google) if execute else list[dict (request)]
    '''

    if title:
        res = create_spreadsheet(title)
        spreadsheet_id = str(res['spreadsheetId'])

    requests = []
    for (name, df) in df_list:
        requests += push_dataframe_to_sheet(spreadsheet_id, name, df, fail_if_exists=True, execute=False)

    # If creating a new workbook, delete 'Sheet1'
    if title:
        delete_request = {'deleteSheet': {'sheetId': get_sheet_id(spreadsheet_id, 'Sheet1')}}
        requests.append(delete_request)

    if execute:
        requests = {'requests': requests}
        return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=requests).execute()

    else:
        return requests



if __name__ == '__main__':
    df1 = pd.DataFrame([[1,2,3,4], [6,7,8,9]], columns=['a', 'b', 'c', 'd']).set_index(['a', 'b'])
    df3 = pd.DataFrame({
        'expense category': ['Food', 'Data', 'Rent', 'Candles', 'Utility'],
        'expense amount': ['$200', '$150', '$800', '$3,600', '$150']
    })
    df3 = df3[['expense category', 'expense amount']]
    df4 = pd.DataFrame({'proposal': ['spend less on candles'], 'response': ['no']})
    l = [('someone who is good at the economy', df3), ('please help me budget this', df1), ('my family is dying', df4)]
    res = push_analysis_to_sheets(l, title='spend less on candles')
