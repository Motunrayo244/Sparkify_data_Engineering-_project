import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        print("connection to postgres Database was successfull")
    except psycopg2.Error as e:
        print("Connection to the postgres database was not successful")
        print(e)
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    cur.close()
    conn.close()    
    
    # connect to sparkify database
    
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        print("connection to sparkifydb Database was successfull")
    except psycopg2.Error as e:
        print("Connection to the sparkifydb database was not successful")
        print(e)
    
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
            print("table with query below successfully executed\n", query)

        except psycopg2.Error as e:
            print("Error encountered while dropping table",query)
            print(e)

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
            print("table with query below created successfully \n", query)

        except psycopg2.Error as e:
            print("Error encountered while creating table",query)
            print(e)


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()