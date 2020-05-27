
def read_excel(pd, file_name, sheet_name, extract_cols):
    """
    Read Excel file using Pandas
    :param pd: Pandas Object
    :param file_name: Excel filename
    :param sheet_name: Sheetname of excel file
    :param extract_cols: Extraction column names
    :return: dataframe
    """
    try:
        df = pd.read_excel(
            file_name, sheet_name=sheet_name, usecols=extract_cols)
        return df
    except Exception as error:
        logger.error(error)



def write_dataframes_excel(pd, result, filename, sheet_name=None):
    """
    Write results to an excel sheet
    :param pd: Pandas object
    :param result: Results
    :param filename: Excel filename
    :param sheet_names: Excel sheetname
    :return: None
    """
    try:
        # write to excel
        writer = pd.ExcelWriter(filename)
        result.to_excel(writer, sheet_name)
        writer.save()
    except Exception as error:
        logger.error(error)

def multiple_dfs_into_single_sheet(pd, df_list, sheets, file_name, spaces):
    # Usage:
    # list of dataframes
    # dfs = [df, df1, df2]
    #
    # # run function
    # multiple_dfs_into_single_sheet(dfs, 'Validation', 'test1.xlsx', 1)
    """
    Multiple dataframes pushed to a single sheet in excel workbook
    :param pd:
    :param df_list:
    :param sheets:
    :param file_name:
    :param spaces:
    :return:
    """

    try:
        writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
        row = 0
        col = 0
        for dataframe in df_list:
            dataframe.to_excel(
                writer,
                sheet_name=sheets,
                startrow=row,
                startcol=col,
                index=False)

            row = row + len(dataframe.index) + spaces + 1
        writer.save()
    except Exception as error:
        logger.error(error)

def dfs_different_tabs(pd, df_list, sheet_list, file_name):
    # Usage:
    # list of dataframes and sheet names
    # dfs = [df, df1, df2]
    # sheets = ['df', 'df1', 'df2']
    #
    # # run function
    # dfs_different_tabs(dfs, sheets, 'multi-test.xlsx')
    """
    Write multiple dataframes to different sheets in workbook
    :param pd:
    :param df_list:
    :param sheet_list:
    :param file_name:
    :return:
    """
    try:
        writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
        for dataframe, sheet in zip(df_list, sheet_list):
            dataframe.to_excel(
                writer, sheet_name=sheet, startrow=0, startcol=0)
        writer.save()
    except Exception as error:
        logger.error(error)

def write_dataframes_csv(results,
                         target_csv_file,
                         index,
                         header=True,
                         write_mode='w',
                         quoting=None):
    """
    Write dataframes to CSV files
    :param results:
    :param target_csv_file:
    :return:
    """
    try:
        if quoting == 1:
            quoting = csv.QUOTE_ALL
        elif quoting == 2 or not quoting:
            quoting = csv.QUOTE_NONE
        elif quoting == 3:
            quoting = csv.QUOTE_NONNUMERIC

        with open(target_csv_file, write_mode) as f:
            results.to_csv(
                f,
                encoding='utf-8',
                index=index,
                header=header,
                quoting=quoting)
    except UnicodeDecodeError as ue:
        error_msg = "Was unable to write to the file due to Unicode Error: " \
                    "`{}` for encoding type: `{}`".format(ue, 'utf-8')
        logger.warning(error_msg)
        encoding_types = ['ascii', 'cp1252', 'iso-8859-1']
        for encoding_type in encoding_types:
            try:
                with open(target_csv_file, write_mode) as f:
                    results.to_csv(
                        f,
                        encoding=encoding_type,
                        index=index,
                        header=header,
                        quoting=quoting)
            except UnicodeDecodeError as ue:
                error_msg = "Was unable to write to the file due to Unicode Error: " \
                            "`{}` for encoding type: `{}`".format(ue,
                                                                   encoding_type)
                logger.warning(error_msg)
                continue
    except Exception as error:
        logger.error(error)


def drop_unnecessary_columns_pandas(df, drop_col='unnamed', retrieve_col=6):

    if not df.empty:
        # Drop extra null columns on the right
        try:
            # Drop '' on the right column side, axis = 1 denotes columns
            df.drop('', axis=1, inplace=True)
        #except ValueError as error:
        except (ValueError, KeyError) as error:
            pass
            #error_msg = "Error {} on dropping null fields in Pandas".format(error)
            #logger.warning(error_msg)

        try:
            if len(df.columns) > retrieve_col:
                headers_unknown = df.columns.str.contains(
                    drop_col, case=False).tolist()
                headers_unknown_index = [
                    index for index, h_row in enumerate(headers_unknown)
                    if h_row
                ]
                if headers_unknown_index:
                    headers_keys = {
                        1: 'Date',
                        2: 'Description',
                        3: 'Debit',
                        4: 'Credit',
                        5: 'Balance',
                        6: 'classification_trans'
                    }
                #df.drop(df.columns[df.columns.str.contains(drop_col, case = False)],axis = 1, inplace=True)
            if len(df.columns) > retrieve_col:
                df = df.iloc[:, 0:retrieve_col]
                df.columns = list(headers_keys.values())
            data = tools.clean_data_rows(
                data,
                data_to_clean=["data to be cleaned"],
                junk_line_remover=3)
            df = pd.DataFrame(data)

        except Exception as error:
            pass
    return df


def check_value_dataframe(df, column1, column_value, column2):
    """
    Check if the particular value has the string and try to match in the
    particular column
    :param pd:
    :param local_filename:
    :param column1:
    :param column_value:
    :param column2:
    :return:
    """
    try:
        if column1 and not column2:
            return df[column1]

        elif column1 and column_value and column2:
            column_value_result = df.loc[df[column1].isin(
                [column_value])][column2]
            if not column_value_result.empty:
                return column_value_result.values[0]
            return column_value_result

    except Exception as error:
        logger.error(error)

def write_csv_as_excel(pd, csv_files, target_excel_file, sheet_name):
    """
    Convert all CSV files into a single excel file
    :param pd: Pandas object
    :param csv_files: List of CSV files
    :param target_excel_file: Output Excel file
    :return: None
    """
    try:
        with pd.ExcelWriter(target_excel_file) as ew:
            if isinstance(csv_files, list):
                for csv_file in csv_files:
                    with open(csv_file) as f:
                        pd.read_csv(f).to_excel(
                            ew, sheet_name=sheet_name, encoding='cp1252')
            elif isinstance(csv_files, str):
                with open(csv_files) as f:
                    pd.read_csv(
                        f, encoding='cp1252').to_excel(
                            ew, sheet_name=sheet_name, encoding='cp1252')
    except Exception as error:
        logger.error(error)

def nth_day_aggregate_month(pd, resampled_df, nth_day=1,
                            aggregate_func="mean"):
    """
    Find the nth day aggregate functions using pandas
    :param pd: Pandas object
    :param resampled_df: Re-sampled data frame
    :param nth_day: nth day of data to be passed to an aggregate function
    :param aggregate_func: Mean/std/Average/Covariance
    :return: aggregate_func_result
    """
    logger.info("Into `{}`() . .".format(whoami()))
    try:
        nth_day_result = resampled_df.groupby(
            pd.Grouper(freq='M')).nth(nth_day - 1)
        if aggregate_func == 'mean' or 'average':
            aggregate_func_result = nth_day_result.mean()

        logger.info("The {} of {} nth day of month is : {}".format(
            aggregate_func, nth_day, aggregate_func_result["balance"]))
    except Exception as error:
        logger.error(error)

    return aggregate_func_result

def autofill_dataframe(pd, filename, df, resample_by='D', fill_type="ffill"):
    """
    Autofill for the dataframe, Using Forward fill by default
    :param pd: Pandas Object
    :param filename: Bank statement filename
    :param df: Pandas dataframe
    :param resample_by: 'day=D/month=M/year=Y'
    :param fill_type: fill_type can be ffill/bfill
    :return: filled_dataframe
    """
    try:
        if fill_type == 'ffill':
            filled_dataframe = df.resample(resample_by).ffill()
        elif fill_type == 'bfill':
            filled_dataframe = df.resample(resample_by).bfill()
        return filled_dataframe

    except Exception as error:
        error_msg = "Error: {} while forward auto-filling for `{}`".format(
            error, filename)
        logger.error(error_msg)
        return


def convert_df_to_json(filename, orient_type):
    data = pd.read_csv(filename).to_json(orient=orient_type)
    #data_json = data.to_json(orient=orient_type)
    dataframe_json = json.loads(data)
    return dataframe_json




def remove_columns_dataframe(df, df_columns, axis=1):
   """
   df: dataframe
   df_columns: list of column indexes to be removed from the dataframes
   axis: By default will be 1 axis which denotes (column), axis=0 denotes row
   """
   try:
       df = df.drop(df.columns[df_columns], axis=axis)
       return df
   except Exception as error:
       logger.error(error)
