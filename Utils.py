import numpy as np
import pandas as pd
import datetime as dt
from lmfit import Parameters
from sqlalchemy import create_engine
import pymysql
import json
import pandas.io.sql as pdsql
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from io import StringIO
import os

DATETIME_LIKE = (dt.datetime, dt.date, np.datetime64, pd.Timestamp)

def isIntegerLike(val):
    return type(val) == int or issubclass(type(val), np.integer)

def isNumerical(val):
    return type(val) == int or type(val) == float or issubclass(type(val), np.integer) or issubclass(type(val), np.float)

def isDatetimeLike(val):
    return type(val) in DATETIME_LIKE

def assertType(value, allowed_types):
    """
    Assert type of value
    :param value:
    :param allowed_types: tuple of types or type
    :return:
    """
    if isinstance(allowed_types, type):
        allowed_types = (allowed_types, )
    assert type(value) in allowed_types, "The only valid values for type are %s." % str(allowed_types)

def getDatabaseAuth(filepath, quiet=False):
    """Read MySQL database authentication information from json file."""
    if quiet is False:
        print('Reading authentication file.')
    try:
        auth = open(filepath, 'r')
        auth_json = json.load(auth)
        auth.close()
        return auth_json
    except IOError as IOErr:
        print('Could not open auth file: %r' % IOErr)
        exit(1)
    except Exception as Err:
        print('Unknown Exception: %r' % Err)
        exit(1)

def pushParametersDictToSQL(authfile, param_dict, table_name, if_exists='append', index=False):
    assertType(param_dict, dict)
    param_dict = {i: param_dict[i].valuesdict() for i in param_dict}
    param_df = pd.DataFrame(param_dict).transpose()
    new_dicts = []
    for row in param_df.iterrows():
        new_dicts.append({
            'id': '|'.join(row[0]),
            'param_name': row[1].index.values,
            'param_value': row[1].values
        })
    df = pd.concat([pd.DataFrame(x) for x in new_dicts])
    df['runtime'] = dt.datetime.now()
    pushDataFrameUsingAlchemy(authfile, df, table_name, if_exists=if_exists, index=index)


def readParametersDictFromSQL(authfile, query, results=True, quiet=False, datetime_filter='recent', exact_datetimes=None):
    passable_datetime_filters = ['recent', 'all', 'exact']
    assert datetime_filter in passable_datetime_filters, (
        'Acceptable datetime_filters are %s' % passable_datetime_filters)
    df = runDatabaseQuery(authfile, query, results=results, quiet=quiet)
    # assert 'ParameterSetName' in df.columns, (
    #     'Derived table from query passed must have a column named "ParameterSetName"')
    if datetime_filter == 'exact':
        for i in exact_datetimes:
            uv.assertDatetimeLike(exact_datetimes)
        df = df[df['runtime'].apply(lambda x: x in exact_datetimes)]
    if datetime_filter == 'recent':
        df = df[df['runtime'] == max(df['runtime'])]
    if datetime_filter == 'all':
        pass
    param_dict = {}
    df = df.set_index('id')
    for ind in df.index.values:
        sub_df = df.ix[ind]
        p = Parameters()
        for row in sub_df.iterrows():
            p.add(name=row[1]['param_name'], value=row[1]['param_value'])
        param_dict[tuple(ind.split('|'))] = p
    return param_dict

def pushDataFrameUsingAlchemy(authfile, df, table_name, if_exists='fail', index=True, message=None, quiet=False):
    """Run query on database and return results as a data frame."""
    if message is not None:
        print(message)

    # Connect to the MySQL database server
    auth_json = getDatabaseAuth(authfile, quiet=quiet)
    if quiet is False:
        print(' Attempting to connect to server %s' % auth_json["Host"])
    try:
        engine = create_engine('mysql+mysqlconnector://%s:%s@%s:%s/%s' % (auth_json["Username"], auth_json["Password"],
                                                           auth_json["Host"], auth_json["Port"], auth_json["Database"]))
        if quiet is False:
            print(' Connection established to server %s' % auth_json["Host"])

        # Read data into pandas dataframe
        if quiet is False:
            print(' Pushing data.')

        try:
            df.to_sql(table_name, engine, if_exists=if_exists, index=index, chunksize=1000)
            if quiet is False:
                print(' Push complete.')
        except Exception as Err:
            if message is None:
                print(' Unable to complete push: %r' % Err)
            else:
                print(' Unable to complete push: %r. %s' % (Err, message))
            engine.dispose()
            return 'Failed'
        finally:
            # Close cursor and connection
            if quiet is False:
                print('Closing database connections.\n')
            engine.dispose()
            return 'Success'

    except Exception as Err:
        # TODO: Add specific db connection exceptions and actions
        print(' Unable to connect to database: %r' % Err)
        exit(1)

def runDatabaseQuery(authfile, sql_query, results=True, quiet=False):
    """Run query on database and return results as a data frame."""
    if quiet is False:
        print('Running query: %s' % sql_query)

    # Connect to the MySQL database server
    auth_json = getDatabaseAuth(authfile, quiet=quiet)
    if quiet is False:
        print(' Attempting to connect to server %s' % auth_json["Host"])
    try:
        conn = pymysql.connect(
            host = auth_json["Host"],
            user = auth_json["Username"],
            passwd = auth_json["Password"],
            db = auth_json["Database"],
            port = int(auth_json["Port"]))
        if quiet is False:
            print(' Connection established to server %s' % auth_json["Host"])
        # Get a cursor and review server info
        cur = conn.cursor()

        # Read data into pandas dataframe
        if quiet is False:
            print(' Running query.')
        if results:
            try:
                result_df = pdsql.read_sql(sql_query, con=conn)
                if quiet is False:
                    print(' Data retrieval completed.')
                return result_df
            except Exception as Err:
                print(' Unable to retrieve data as requested: %r' % Err)
            finally:
                # Close cursor and connection
                if quiet is False:
                    print('Closing database connections.\n')
                cur.close()
                conn.close()
        else:
            try:
                cur.execute(sql_query)
                if quiet is False:
                    print(' Query worked.')
            except Exception as Err:
                print(' Query failed: %r' % Err)
            finally:
                if quiet is False:
                    print(' Closing DB connection.\n')
                cur.close()
                conn.close()

    except Exception as Err:
        # TODO: Add specific db connection exceptions and actions
        print(' Unable to connect to database: %r' % Err)
        exit(1)


def getDataFrameFromS3(auth_file_path, bucket_name, s3_file_path):
    """
    Reads a csv from S3, returns a DataFrame.
    :param auth_file_path: str, path to auth file
    :param bucket_name: str, name of S3 bucket
    :param s3_file_path: str, path to S3 file
    :return: pd.DataFrame
    """
    auth_json = getDatabaseAuth(auth_file_path)
    conn = S3Connection(auth_json['key'], auth_json['secret'])
    bucket = conn.get_bucket(bucket_name)
    key = Key(bucket)
    key.key = s3_file_path
    string = key.get_contents_as_string()
    string = string.decode('utf-8')
    stringio = StringIO(string)
    df = pd.DataFrame.from_csv(stringio)
    conn.close()
    return df


def pushDataFrameToS3(df, auth_file_path, bucket_name, s3_file_path, overwrite=False):
    """
    Takes a DataFrame and saves to s3
    :param df: pd.DataFrame
    :param auth_file_path: str, path to auth file
    :param bucket_name: str, name of S3 bucket
    :param s3_file_path: str, path to S3 file
    :return:
    """
    auth_json = getDatabaseAuth(auth_file_path)
    conn = S3Connection(auth_json['key'], auth_json['secret'])
    bucket = conn.get_bucket(bucket_name)
    key = bucket.get_key(s3_file_path)
    if key is None:
        key = Key(bucket)
        key.key = s3_file_path
    elif not overwrite:
        print('Nothing was pushed because %s already exists. To overwrite, use overwrite=True' % s3_file_path)
        return
    stringio = StringIO()
    df.to_csv(stringio)
    key.set_contents_from_string(stringio.getvalue())
    conn.close()


def saveFileToS3(file, auth_file_path, bucket_name, s3_file_path, overwrite=False, delete_local_version=False):
    auth_json = getDatabaseAuth(auth_file_path)
    conn = S3Connection(auth_json['key'], auth_json['secret'])
    bucket = conn.get_bucket(bucket_name)
    key = bucket.get_key(s3_file_path)
    if key is None:
        key = Key(bucket)
        key.key = s3_file_path
    elif not overwrite:
        print('Nothing was pushed because %s already exists. To overwrite, use overwrite=True' % s3_file_path)
        return
    key.set_contents_from_filename(file)
    conn.close()
    if delete_local_version:
        os.remove(file)
