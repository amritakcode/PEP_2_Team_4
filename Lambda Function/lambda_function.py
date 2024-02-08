import json
import pandas as pd
import mysql.connector
import boto3
import traceback
import io

# Function to connect to the RDS instance
def connect_to_rds_instance(region_name, db_instance_identifier):
    try:
        # Create a boto3 RDS client
        rds_client = boto3.client('rds', region_name=region_name)
        
        # Describe the specified RDS instance
        response = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
        
        # Extract endpoint information
        endpoint = response['DBInstances'][0]['Endpoint']
        host = endpoint['Address']
        port = endpoint['Port']
        username = response['DBInstances'][0]['MasterUsername']
        password = "hdMIcfoYeYNsbkm43Vhk"  # Replace with your actual password
        
        # Print connection details
        print("Successfully connected to RDS instance:")
        print(f"Host: {host}")
        print(f"Port: {port}")
        print(f"Username: {username}")
        print("Password: ***")  # Do not print password for security reasons
        
        return host, port, username, password

    except Exception as e:
        print("Error connecting to RDS instance:", e)
        return None

# Function to import data from S3 to MySQL RDS
def import_data_to_rds(connection, s3_bucket, s3_key):
    try:
        # Read CSV data from S3
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), delimiter=';')

        # Convert data types
        df['ap_hi'] = df['ap_hi'].astype(int)
        df['ap_lo'] = df['ap_lo'].astype(int)
        df['cholesterol'] = df['cholesterol'].astype(int)
        df['gluc'] = df['gluc'].astype(int)
        df['smoke'] = df['smoke'].astype(int)
        df['alco'] = df['alco'].astype(int)
        df['active'] = df['active'].astype(int)
        df['cardio'] = df['cardio'].astype(int)

        # Data Cleaning
        df.drop(df[(df['height'] > df['height'].quantile(0.975)) | (df['height'] < df['height'].quantile(0.025))].index,inplace=True)
        df.drop(df[(df['weight'] > df['weight'].quantile(0.975)) | (df['weight'] < df['weight'].quantile(0.025))].index,inplace=True)
        df.drop(df[(df['ap_hi'] > df['ap_hi'].quantile(0.975)) | (df['ap_hi'] < df['ap_hi'].quantile(0.025))].index,inplace=True)
        df.drop(df[(df['ap_lo'] > df['ap_lo'].quantile(0.975)) | (df['ap_lo'] < df['ap_lo'].quantile(0.025))].index,inplace=True)
        df['age'] = (df['age'] / 365).round().astype('int')
        df.dropna(inplace=True)
        df.drop_duplicates()

        # Open a cursor
        cursor = connection.cursor()

        # Insert data into tables
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO habits (habit_id, smoke, alcohol, activ, cardio) VALUES (%s, %s, %s, %s, %s)", (index, int(row['smoke']), int(row['alco']), int(row['active']), int(row['cardio'])))

            cursor.execute("INSERT INTO blood_info (blood_id, ap_hi, ap_low, cholesterol, glucose) VALUES (%s, %s, %s, %s, %s)", (index, int(row['ap_hi']), int(row['ap_lo']), int(row['cholesterol']), int(row['gluc'])))

            cursor.execute("INSERT INTO patient (patient_id, age, gender, height, weight, habit_id, blood_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", (int(row['id']), int(row['age']), int(row['gender']), int(row['height']), float(row['weight']), index, index))

        # Commit changes
        connection.commit()

        # Close cursor
        cursor.close()

        print("Data imported successfully.")

    except Exception as e:
        print("Error importing data to MySQL:", e)
        
def lambda_handler(event, context):
    try:
        # Establish connection to MySQL RDS instance
        conn = mysql.connector.connect(
            database="cardiovascular",
            user="mavila0045",
            password="hdMIcfoYeYNsbkm43Vhk",
            host="cardiovascular-disease.cnq8osg2c5wd.us-west-1.rds.amazonaws.com",
            port="3306"
        )

        # Get S3 bucket and key from event
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']

        # Import data from S3 to RDS
        import_data_to_rds(conn, s3_bucket, s3_key)

        # Close connection
        conn.close()

    except Exception as e:
        print("Error:", e)
        traceback.print_exc()
        raise e
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
