import boto3
import pandas as pd
import psycopg2
import io
import csv
from io import StringIO

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    bucket_name = 'apprentice-training-data-bipin-cleaned-data'
    object_key = 'filtered_data.csv'

    
    '''
    # This part of code was the first assignment to dump the raw data to the cleaded data bucket.
    
    # Define the source and destination bucket and key
    source_bucket = 'apprentice-training-data-bipin-raw-data'
    source_key = 'football_data.csv'

    destination_bucket = 'apprentice-training-data-bipin-cleaned-data'
    destination_key = 'filtered_data1.csv'

    # Download CSV from source bucket
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    csv_content = response['Body'].read()

    # Perform data cleaning using pandas
    df = pd.read_csv(io.BytesIO(csv_content))
    
    df = df.drop(columns=['prediction','start_date','last_update_at','odds'])
    
    column_to_modify = "competition_name"
    df[column_to_modify] = df[column_to_modify].str.replace(r'^\d+\s*', '', regex=True)
    
    result_to_remove = "0 - 0"
    filtered_data = df[df["result"] != result_to_remove]
 
    # Perform your data cleaning operations on the dataframe

    # Convert the cleaned dataframe back to CSV content
    cleaned_csv_content = filtered_data.to_csv(index=False)

    # Upload cleaned CSV to destination bucket
    s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=cleaned_csv_content)
    
    '''
    
    
    #Connecting database to postgresql part of code starts here
    db_params = {
        "host": "apprentice-training-2023-rds.cth7tqaptja4.us-west-1.rds.amazonaws.com",
        "database": "postgres",
        "user": "postgres",
        "password": "hello123"
    }
    '''
    create_table_query = """
        CREATE TABLE IF NOT EXISTS etl_assignment_bipin (
            home_team VARCHAR,
            away_team VARCHAR,
            id int,
            market VARCHAR,
            competition_name VARCHAR,
            competition_cluster VARCHAR,
            status VARCHAR,
            federation varchar,
            is_expired varchar,
            season varchar,
            result varchar
        );
        """
    
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    
    # Commit changes and close connections
    conn.commit()
    cursor.close()
    conn.close()
    '''
    
    
    
    #rdb part of code starts here 

    
    conn = psycopg2.connect(**db_params)
    
    cursor = conn.cursor()
    
    
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = response['Body'].read().decode('utf-8')

    # Parse data as CSV
    csv_data = StringIO(data)
    csv_reader = csv.reader(csv_data)
    next(csv_reader)  
    
    cursor = conn.cursor()

    values_to_insert = []
    for row in csv_reader:
        home_team = row[0]
        away_team = row[1]
        id = row[2]
        market = row[3]
        competition_name = row[4]
        competition_cluster = row[5]
        status = row[6]
        federation = row[7]
        is_expired = row[8]
        season = row[9]
        result = row[10]
        
        values_to_insert.append((home_team, away_team,id,market,competition_name,competition_cluster,status,federation,is_expired,season,result))
        
    placeholders = ', '.join(['%s'] * len(values_to_insert[0]))
    sql = f"INSERT INTO etl_assignment_bipin VALUES ({placeholders})"
    
    cursor.executemany(sql, values_to_insert)
    
    conn.commit()
    cursor.close()
    conn.close()



    return {
        'statusCode': 200,
        'body': 'Data cleaned and stored successfully!'
    }

