import base64
import pymysql
import boto3
import json
from botocore.exceptions import ClientError

def get_secret():
    """Retrieve secret from AWS Secrets Manager."""
    secret_name = "LicenseDB"  # Name of the secret
    region_name = "us-east-1"  # Replace with your AWS region

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print("ERROR: Could not retrieve secret -", e)
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(decoded_binary_secret)

def get_db_connection():
    """
    Creates a connection to the RDS:MySQLDB database using credentials from Secrets Manager.
    """
    #secret = get_secret()
    host="chello-insights.cb8gwiek2g7m.us-east-2.rds.amazonaws.com"
    username="admin123"
    password="Mychello08012024*"
    dbname="DB_Chello"

    try:
        connection = (pymysql
                      .connect(host=host,user=username,password=password,db=dbname,charset='utf8mb4'))
        return connection
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
        raise

def execute_query(sql, params=None, fetch_one=False):
    """
    Executes a given SQL query with optional parameters and returns the result.
    """
    connection = get_db_connection()
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        if fetch_one:
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()
    connection.close()
    return result

def insert_record(sql, params):
    """
    Inserts a record into the database.
    """
    connection = get_db_connection()
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        connection.commit()
    connection.close()

def update_record(sql, params):
    """
    Updates a record in the database.
    """
    connection = get_db_connection()
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        connection.commit()
    connection.close()

def delete_record(sql, params):
    """
    Deletes a record from the database.
    """
    connection = get_db_connection()
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        connection.commit()
    connection.close()

# Additional utility functions can be added here