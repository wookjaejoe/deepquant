import pymysql

from config import config
import sqlalchemy

_config_maria = config['maria']
_host = _config_maria['host']
_username = _config_maria['username']
_password = _config_maria['password']
_db = "finance"


def maria_home() -> sqlalchemy.engine.Engine:
    return sqlalchemy.create_engine(f"mysql+pymysql://{_username}:{_password}@{_host}/{_db}")


class MariaConnection:
    def __enter__(self):
        self.conn = pymysql.connect(
            host=_host,
            user=_username,
            password=_password,
            db=_db,
            charset='utf8'
        )
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
