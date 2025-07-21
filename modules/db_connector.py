import oracledb

class OracleDBConnector:
    """
    Handles Oracle DB connections using oracledb. Use as a context manager.
    """
    def __init__(self, db_config):
        self.user = db_config['user']
        self.password = db_config['password']
        self.dsn = db_config['dsn']
        self.conn = None

    def __enter__(self):
        self.conn = oracledb.connect(user=self.user, password=self.password, dsn=self.dsn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def get_cursor(self):
        if not self.conn:
            raise Exception("Connection not established. Use as a context manager.")
        return self.conn.cursor() 