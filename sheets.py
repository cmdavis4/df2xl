import sys
sys.path.insert(0, '/home/vagrant/twosixcapital/gspread')

import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client import tools
import pdb

SCOPE = 'https://spreadsheets.google.com/feeds'

def get_credentials():
    return ServiceAccountCredentials.from_json_keyfile_name('test-spreadsheet.json', SCOPE)

def df_to_sheets(df, spreadsheet_id, worksheet_id, column_header=True, row_header=True):

    gc = gspread.authorize(get_credentials())
    wks = getattr(gc.open(spreadsheet_id), worksheet_id.lower())

    to_write = [x.split('|') for x in df.to_csv(sep='|').split('\n')[:-1]]
    max_cell = wks.get_addr_int(len(to_write), len(to_write[0]))
    cell_range = wks.range('A1:{}'.format(max_cell))
    cell_arr = np.array(cell_range).reshape(len(to_write), len(to_write[0]))

    for row in range(len(cell_arr)):
        for col in range(len(cell_arr[0])):
            cell_arr[row, col].value = to_write[row][col]

    wks.update_cells(list(cell_arr.flatten()))


if __name__ == '__main__':
    df = pd.DataFrame([[1,2,3], [4,5,6]], columns=['a', 'b', 'c']).set_index(['a', 'b'])
    df.columns = [['d'], df.columns]

    df_to_sheets(df, 'test', 'Sheet1')