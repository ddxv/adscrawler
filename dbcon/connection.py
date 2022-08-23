from sqlalchemy import create_engine
from config import CONFIG, get_logger
from sshtunnel import SSHTunnelForwarder

logger = get_logger(__name__)


def get_db_connection(is_local_db):
    server_name = "madrone"
    if is_local_db:
        local_port = 5432
    else:
        server = OpenSSHTunnel(server_name)
        server.start()
        local_port = str(server.local_bind_port)
    conn = PostgresCon(server_name, "127.0.0.1", local_port)
    return conn


def OpenSSHTunnel(server_name):
    with SSHTunnelForwarder(
        (CONFIG[server_name]["host"], 22),  # Remote server IP and SSH port
        ssh_username=CONFIG[server_name]["os_user"],
        # ssh_pkey=CONFIG["ssh"]["pkey"],
        # ssh_private_key_password=CONFIG["ssh"]["pkey_password"],
        remote_bind_address=("127.0.0.1", 5432),
    ) as server:  # PostgreSQL server IP and sever port on remote machine
        server.start()  # start ssh sever
        logger.info("Connecting via SSH")
        # connect to PostgreSQL
    return server


class PostgresCon:
    """Class for managing the connection to postgres
    Parameters:
    ----------------
        my_db: String, passed on init, string name of db
    """

    engine = None
    db_name = None
    db_pass = None
    db_uri = None
    db_user = None
    db_ip = None
    ssh_pkey_password = None
    ssh_pkey = None
    ssh_username = None

    def __init__(self, my_db, db_ip, db_port):
        self.db_name = my_db
        self.db_ip = db_ip
        self.db_port = db_port
        try:
            self.db_pass = CONFIG["db"][self.db_name]["db_password"]
            self.db_user = CONFIG["db"][self.db_name]["db_user"]
        except Exception as error:
            logger.error(f"Loading db_auth for {self.db_name}, error: {error}")

    def set_engine(self):
        try:
            db_login = f"postgresql://{self.db_user}:{self.db_pass}"
            db_uri = f"{db_login}@{self.db_ip}:{self.db_port}/{self.db_name}"
            logger.info(f"Connecting to PostgreSQL {self.db_name}")
            self.engine = create_engine(db_uri, connect_args={"connect_timeout": 10})
        except Exception as error:
            logger.error(
                f"Failed to connect {self.db_name} @ {self.db_ip}, error: {error}"
            )
            self.db_name = None
