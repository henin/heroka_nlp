
def get_db_connection_client_info(client,
                                  read_only_connection=None,
                                  reporting_connection=None):
    try:
        client_data = get_client_info(client)
        dynamodb_region = str(client_data.get("dynamodb_region"))
        dynamodb = get_service('dynamodb', service_region_name=dynamodb_region)

        host = client_data.get('nc_db_hostname')
        read_host = client_data.get('nc_db_read_hostname')
        reporting_host = client_data.get('nc_db_reporting_hostname')
        username = client_data.get('nc_db_username')
        password = client_data.get('nc_db_password')
        database = client_data.get('nc_database')
        project_path = client_data.get('project_path')
        charset = "utf8mb4"
        cursorclass = pymysql.cursors.DictCursor
        AWS_ACCESS_KEY_ID = client_data.get('AWS_ACCESS_KEY_ID')
        AWS_ACCESS_KEY_SECRET = client_data.get('AWS_ACCESS_KEY_SECRET')

        read_connection = pymysql.connect(
            host=read_host,
            user=username,
            password=password,
            db=database,
            charset=charset,
            cursorclass=cursorclass)

        if read_only_connection:
            results = {
            'read_connection': read_connection,
            'client_data': client_data
        }
            return results

        write_connection = pymysql.connect(
            host=host,
            user=username,
            password=password,
            db=database,
            charset=charset,
            cursorclass=cursorclass)

        results = {
            'read_connection': read_connection,
            'write_connection': write_connection,
            'client_data': client_data
        }

        if reporting_connection:
            report_connection = pymysql.connect(
            host=reporting_host,
            user=username,
            password=password,
            db=database,
            charset=charset,
            cursorclass=cursorclass)
            results['reporting_connection'] = report_connection

        return results

    except Exception as error:
        logger.error(error)


def create_database_connection(hostname,
                               username,
                               password,
                               database,
                               charset=None):
    # Connect to the database
    try:
        connection = pymysql.connect(
            host=hostname,
            user=username,
            password=password,
            db=database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        return connection
    except Exception as error:
        logger.error(error)

def execute_mysql_query(connection, sql, fetch_type=None,
                        keep_connection=None):
    """
    connection: Connection details
    sql: Query to execute, can be insert, update, drop"
    fetch_type: if sql is select( fetch_type=all/one), fetch_type for insert, update,drop is None
    keep_connection : If you dont want to close connections, then None
    """
    try:
        cursor = connection.cursor()
        # Create a new record
        cursor.execute(sql)
        if not fetch_type:
            # connection commits
            connection.commit()
        if fetch_type == 'all':
            result = cursor.fetchall()
            return result
        elif fetch_type == 'one':
            result = cursor.fetchone()
            return result
        if not keep_connection:
            connection.close()
    except Exception as error:
        logger.error(error)
        connection.close()




