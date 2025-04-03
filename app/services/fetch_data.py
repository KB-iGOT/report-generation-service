import pandas as pd
from io import BytesIO
from ..config.db_connection import DBConnection

class DataFetcher:
    def __init__(self):# Class-level shared connection
        # Initialize a shared database connection
        self.connection = DBConnection.get_connection()

    def fetch_data_as_map(self, table_name):
        try:
            cursor = self.connection.cursor()

            # Fetch data from the table
            query = f"SELECT * FROM {table_name};"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            # Create a list of dictionaries (map) for the response
            data_map = [dict(zip(column_names, row)) for row in rows]

            print(f"Data fetched successfully. Total records: {len(data_map)}")
            return data_map
        except Exception as e:
            print(f"Error: {e}")
            return []

    def fetch_data_as_csv_stream(self, table_name, org_id):
        try:
            cursor = self.connection.cursor()

            # Fetch all data from the table with filtering by orgId
            query = f"""
                SELECT * 
                FROM {table_name} 
                WHERE mdo_id = %s;
            """
            cursor.execute(query, (org_id,))
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            # Load data into a pandas DataFrame
            df = pd.DataFrame(rows, columns=column_names)

            # Filter DataFrame to include only the required columns
            required_columns = ['user_id', 'mdo_id', 'full_name', 'email']
            df = df[required_columns]

            # Export DataFrame to a CSV byte stream
            csv_stream = BytesIO()
            df.to_csv(csv_stream, index=False)
            csv_stream.seek(0)  # Reset stream position to the beginning

            print(f"Data fetched and converted to CSV stream successfully for orgId {org_id}. Total records: {len(df)}")
            return csv_stream
        except Exception as e:
            print(f"Error: {e}")
            return None

    def close(self):
        # Use the standalone close_connection function to close the shared connection
        close_connection(self.connection)

def close_connection(connection):
    if connection:
        connection.close()
