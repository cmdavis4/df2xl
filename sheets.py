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

# Cell background colors
TITLE_COLOR = {'blue': .66, 'green': .46, 'red': 0}
ROW_HEADER_COLOR = {'blue': 1., 'green': .69, 'red': .33}
COLUMN_HEADER_COLOR = {'blue': .84, 'green': .59, 'red': .33}
BORDER_STYLE = {'style': 'DASHED', 'width': 1, 'color': ROW_HEADER_COLOR}

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
                         for x in get_sheet_by_name(spreadsheet_id, sheet_name)['sheets']
                         if x['properties']['title'] == sheet_name][0])
    except IndexError:
        raise ValueError("Sheet '{}' does not exists in spreadsheet {}".format(sheet_name, spreadsheet_id))

def get_sheet_by_name(spreadsheet_id, sheet_name):

    try:
        return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    except IndexError:
        raise ValueError("Sheet '{}' does not exists in spreadsheet {}".format(sheet_name, spreadsheet_id))

def dataframe_to_requests(df, name, sheet_id, top_left=(0, 0)):

    # Turn the dataframe into a string (for pasteData) and an array of strings (to get shape of headers)
    df_string = df.to_csv(sep='|')[:-1]
    df_string = '|'.join([name.upper() for _ in range(len(df_string.split('\n')[0].split('|')))]) + '\n' + df_string
    string_arr = np.array([x.split('|') for x in df_string.split('\n')])
    # title_arr = np.array([name] * len(string_arr[0]))
    # string_arr = np.vstack((title_arr, string_arr))

    # Figure out the ranges to use for the requests
    data_dim = np.shape(df.values)
    table_dim = np.shape(string_arr)
    column_header_dim = (table_dim[0]-data_dim[0]-1, table_dim[1])
    row_header_dim = (table_dim[0]-column_header_dim[0]-1, table_dim[1]-data_dim[1])

    title_range = ((top_left[0], top_left[1]),
                   (top_left[0]+1, top_left[1]+table_dim[1]))

    # +1's are to offset the title row
    column_header_range = ((top_left[0]+1, top_left[1]),
                           (top_left[0] + column_header_dim[0] + 1, top_left[1] + column_header_dim[1]))
    row_header_range = ((top_left[0] + column_header_dim[0] + 1, top_left[1]),
                        (top_left[0] + table_dim[0], top_left[1] + row_header_dim[1]))
    table_range = ((top_left[0], top_left[1]),
                   (top_left[0]+table_dim[0], top_left[1]+table_dim[1]))
    data_range = ((column_header_range[1][0], row_header_range[1][1]),
                 (table_range[1][0], table_range[1][1]))

    requests = []

    # Title request
    title_merge_request = {'mergeCells': {
        'range': {
            'startRowIndex': title_range[0][0],
            'startColumnIndex': title_range[0][1],
            'endRowIndex': title_range[1][0],
            'endColumnIndex': title_range[1][1],
            'sheetId': sheet_id
        },
        'mergeType': 'MERGE_ALL'
    }}
    title_format_request = {'repeatCell': {
            'fields': '*',
            'range': {
                'startRowIndex': title_range[0][0],
                'startColumnIndex': title_range[0][1],
                'endRowIndex': title_range[1][0],
                'endColumnIndex': title_range[1][1],
                'sheetId': sheet_id
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': TITLE_COLOR,
                    'horizontalAlignment': 'CENTER',
                    'textFormat': {
                        'foregroundColor': {
                            'red': 1.0,
                            'blue': 1.0,
                            'green': 1.0
                        },
                        'bold': True
                    }
                }
            }
        }}
    # These need to be in a specific order relative to the others, so we add them on just before executing

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
                    'backgroundColor': COLUMN_HEADER_COLOR,
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
                    'backgroundColor': ROW_HEADER_COLOR,
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
            'rowIndex': top_left[0],
            'columnIndex': top_left[1],
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

    # Named range request
    named_range_request = {
        'addNamedRange': {
            'namedRange': {
                'range': {
                    'startRowIndex': table_range[0][0],
                    'startColumnIndex': table_range[0][1],
                    'endRowIndex': table_range[1][0],
                    'endColumnIndex': table_range[1][1],
                    'sheetId': sheet_id
                },
                'name': name.replace(' ', '_')
            }
        }
    }
    requests.append(named_range_request)

    requests = [title_format_request] + requests + [title_merge_request]

    return (data_range, requests)

def add_chart(sheet_id, table_range, title, type='growth', top_left=None):

    implemented_types = ['growth']

    if type not in implemented_types:
        raise NotImplementedError('Acceptable type arguments are {}'.format(implemented_types))

    top_left = top_left if top_left is not None else (table_range[0][0], table_range[1][1]+1)

    if type == 'growth':

        request = {
          "addChart": {
            "chart": {
              "position": {
                "overlayPosition": {
                  "anchorCell": {
                    "rowIndex": top_left[0],
                    "columnIndex": top_left[1],
                    "sheetId": sheet_id
                  }
                }
              },
              "spec": {
                "title": title,
                "basicChart": {
                  "chartType": "LINE",
                  "legendPosition": "BOTTOM_LEGEND",
                  "axis": [
                    {
                      "position": "BOTTOM_AXIS",
                      "title": "Date"
                    },
                    {
                      "position": "LEFT_AXIS",
                      "title": "New Adds"
                    }
                  ],
                  "domains": [
                    {
                      "domain": {
                        "sourceRange": {
                          "sources": [
                            {
                              "sheetId": sheet_id,
                              "startRowIndex": table_range[0][0]+2,
                              "endRowIndex": table_range[1][0]+1,
                              "startColumnIndex": table_range[0][1],
                              "endColumnIndex": table_range[0][1]+1
                            }
                          ]
                        }
                      }
                    }
                  ],
                  "series": [
                    {
                      "series": {
                        "sourceRange": {
                          "sources": [
                            {
                              "sheetId": sheet_id,
                              "startRowIndex": table_range[0][0]+2,
                              "endRowIndex": table_range[1][0]+1,
                              "startColumnIndex": table_range[0][1]+1,
                              "endColumnIndex": table_range[1][1]
                            }
                          ]
                        }
                      },
                      "targetAxis": "LEFT_AXIS"
                    }
                  ],
                  "headerCount": 1
                }
              }
            }
          }
        }

    return request

def create_analysis_workbook(df_list, title=None, spreadsheet_id=None, execute=True, overwrite=True):
    '''
    Function to push multiple DataFrames into the same Google spreadsheet.
    :param df_list: list[(str, pd.DataFrame, dict)],where str is the title of the sheet and pd.DataFrame is the
    content. The dict is a list of flags specifying charts to be created, extra tables to create, etc.
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

    try:
        sheet_id = add_sheet(spreadsheet_id, 'Outputs')['replies'][0]['addSheet']['properties']['sheetId']
    except Exception:
        if not overwrite:
            raise
        else:
            # If sheet already exists, clear it
            sheet_id = get_sheet_id(spreadsheet_id, 'Outputs')
            requests.append({'updateCells': {
                    'range': {
                        'sheetId': sheet_id
                    },
                    'fields': '*'
                }
            })

            # Delete named ranges
            try:
                nr_ids = [x['namedRangeId'] for x in get_sheet_by_name(spreadsheet_id, 'Outputs')['namedRanges']]
                for x in nr_ids:
                    requests.append({'deleteNamedRange': {'namedRangeId': x}})
            except KeyError:  # If no named ranges
                pass

            # Delete Charts
            try:
                chart_ids = [x['chartId'] for x in get_sheet_by_name(spreadsheet_id, 'Outputs')['sheets'][0]['charts']]
                for x in chart_ids:
                    requests.append({'deleteEmbeddedObject': {'objectId': x}})
            except KeyError:  # If no charts
                pass

            # Unmerge merged cells
            unmerge_request = {'unmergeCells': {
                'range': {
                    'sheetId': sheet_id
                }
            }}
            requests.append(unmerge_request)

    top_left=(0,0)
    for t in df_list:

        # Border request
        if top_left[0] > 0:
            requests.append({
                'updateBorders': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': top_left[0]-1,
                        'endRowIndex': top_left[0]
                    },
                    'top': BORDER_STYLE
                }
            })

        name, df = t[0], t[1]
        try:
            d = t[2]
        except IndexError:
            d = {}
        dr, r = dataframe_to_requests(df, name, sheet_id, top_left)
        requests += r

        if d.get('triangle', False) and d.get('growth', False):
            diagonals = np.diag(df)
            x_labels = df.index[:len(diagonals)]
            s = pd.Series(diagonals, index=x_labels)
            diagonals_df = pd.DataFrame(s, columns=['New adds'])
            diag_dr, diag_r = dataframe_to_requests(diagonals_df, name + ' new adds', sheet_id,
                                                    top_left=(top_left[0], dr[1][1]+1))
            requests += diag_r

            chart_tr = ((top_left[0], dr[1][1]+1), (diag_dr[1][0], diag_dr[1][1]))  # chart table (data source) range
            if d.get('chart', False):
                requests.append(add_chart(sheet_id, chart_tr, 'New adds'))
                top_left = (max(dr[0][0]+17, diag_dr[1][0]+2, dr[1][0]+2), 0)  # 17 is arbitrary, pls change if needed
            else:
                top_left = (max(dr[1][0], diag_dr[1][0])+2, 0)
        else:
            top_left = (dr[1][0]+2, 0)


    # If creating a new workbook, delete 'Sheet1'; we do this as part of the batch request and not on its own
    # because I'm not sure what would happen if you tried to delete the only sheet in a workbook.
    if title:
        delete_request = {'deleteSheet': {'sheetId': get_sheet_id(spreadsheet_id, 'Sheet1')}}
        requests.append(delete_request)

    if execute:
        return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()
    else:
        return requests


if __name__ == '__main__':
    df1 = pd.DataFrame([[1,2,10000,4], [6,7,8,9]], columns=['a', 'b', 'c', 'd']).set_index(['a', 'b'])
    df3 = pd.DataFrame({
        'expense category': ['Food', 'Data', 'Rent', 'Candles', 'Utility'],
        'expense amount': ['$200', '$150', '$800', '$3,600', '$150']
    })
    df3 = df3[['expense category', 'expense amount']]
    df4 = pd.DataFrame({'proposal': ['spend less on candles'], 'response': ['no']})
    df5 = pd.DataFrame.from_dict({
        'a': [5, 4, 3, 2, 1],
        'b': [np.nan, 6, 3, 2, 1],
        'c': [np.nan, np.nan, 7, 4, 1],
        'd': [np.nan, np.nan, np.nan, 8, 1],
        'e': [np.nan, np.nan, np.nan, np.nan, 9]
    }, orient='index')
    df5 = df5.loc[['a', 'b', 'c', 'd', 'e']]
    df5.columns = pd.date_range('2010-01-01', freq='AS', periods=5)
    df5.index = pd.date_range('2010-01-01', freq='AS', periods=5)
    df6 = pd.DataFrame([1,2,3],[4,5,6])
    l = [
        ('someone who is good at the economy', df3),
        ('please help me budget this', df1),
        ('my family is dying', df4),
        ('real data', df5, {'triangle': True, 'growth': True, 'chart': True}),
        ('blah', df6)
    ]
    res = create_analysis_workbook(l, spreadsheet_id='1V6CJ7wcyuESwfR8w4T7SbltE9H3TYcGEY_9apW1ebCM')
