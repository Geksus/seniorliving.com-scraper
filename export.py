import sqlite3
import pandas as pd


def export():
    conn = sqlite3.connect('senior_living.db')
    query = 'SELECT * FROM data'
    df = pd.read_sql_query(query, conn)

    # Add column names
    df.columns = ['zipcode',
                  'care_name',
                  'type_of_care',
                  'title', 'address',
                  'description',
                  'contact_information',
                  'website',
                  'payment_type']

    # Export to CSV
    df.to_csv('data.csv', index=False, header=True)

    # Export to JSON
    df.to_json('data.json', orient='records')

    # Export to Excel
    df.to_excel('data.xlsx', index=False, header=True)

    conn.close()
