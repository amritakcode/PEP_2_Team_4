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
        password = ""  # Replace with your actual password
        
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
        df.dropna(inplace=True)

        # Convert data types
        df['ap_hi'] = df['ap_hi'].astype(int)
        df['ap_lo'] = df['ap_lo'].astype(int)
        df['cholesterol'] = df['cholesterol'].astype(int)
        df['gluc'] = df['gluc'].astype(int)
        df['smoke'] = df['smoke'].astype(int)
        df['alco'] = df['alco'].astype(int)
        df['active'] = df['active'].astype(int)
        df['cardio'] = df['cardio'].astype(int)

        # Create blood_info DataFrame with unique blood_id
        blood_info = df[['ap_hi', 'ap_lo', 'cholesterol', 'gluc']].copy()
        blood_info.reset_index(inplace=True)
        blood_info.columns = ['blood_id', 'ap_hi', 'ap_low', 'cholesterol', 'glucose']

        # Create habits DataFrame with unique habit_id
        habits = df[['smoke', 'alco', 'active', 'cardio']].copy()
        habits.reset_index(inplace=True)
        habits.columns = ['habit_id', 'smoke', 'alcohol', 'activ', 'cardio']

        # Create patient DataFrame
        patient = df[['id', 'age', 'gender', 'height', 'weight']].copy()

        # Open a cursor
        cursor = connection.cursor()

        # Insert data into blood_info table
        for index, row in blood_info.iterrows():
            cursor.execute("""
                INSERT INTO blood_info (blood_id, ap_hi, ap_low, cholesterol, glucose)
                VALUES (%s, %s, %s, %s, %s);
            """, (index, int(row['ap_hi']), int(row['ap_low']), int(row['cholesterol']), int(row['glucose'])))

        # Insert data into habits table
        for index, row in habits.iterrows():
            cursor.execute("""
                INSERT INTO habits (habit_id, smoke, alcohol, activ, cardio)
                VALUES (%s, %s, %s, %s, %s);
            """, (index, int(row['smoke']), int(row['alcohol']), int(row['activ']), int(row['cardio'])))

        # Commit changes
        connection.commit()

        # Insert data into patient table
        for index, row in patient.iterrows():
            cursor.execute("""
                INSERT INTO patient (patient_id, age, gender, height, weight, habit_id, blood_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (int(row['id']), int(row['age']), int(row['gender']), int(row['height']), float(row['weight']), int(row.name), int(row.name)))

        # Commit changes
        connection.commit()

        # Close cursor
        cursor.close()

        print("Data imported successfully.")

    except Exception as e:
        print("Error importing data to MySQL:", e)
        
def lambda_handler(event, context):
    try:
        # Establish connection to MySQL RDS instance (Insert Database Credentials)
        conn = mysql.connector.connect(
            database="",
            user="",
            password="",
            host="",
            port=""
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
