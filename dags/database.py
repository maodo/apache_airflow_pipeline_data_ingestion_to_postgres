import psycopg2

class Database():
    def __init__(self,host,port,dbname,user,password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
    def connect(self):
        try:
            self.conn = psycopg2.connect(host=self.host,port=self.port,dbname=self.dbname,user=self.user,password=self.password)
            print("Connected to the database successfully!")
            # Create a cursor object
            cursor = self.conn.cursor()
            # Example: Execute a query
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print(f"PostgreSQL version: {version[0]}. You DB is ready to receive connections")
            cursor.close()
        except Exception as e:
            cursor.close()
            self.conn.close()
            print(f"Error connecting to database : {e}")
    def close(self):
        try:
            self.conn.close()
            print(f"Connection closed successfully !")
        except Exception as e:
            print(f"Error closing connection : {e}")