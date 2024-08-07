import sqlalchemy
from sshtunnel import SSHTunnelForwarder

from adscrawler.config import CONFIG, get_logger

logger = get_logger(__name__)


class PostgresCon:
    """Class for managing the connection to postgres

    Parameters
    ----------
        my_db: String, passed on init, string name of db
    """

    engine = sqlalchemy.engine
    db_name = None
    db_pass = None
    db_uri = None
    db_user = None
    db_ip = None

    def __init__(self, my_db: str, db_ip: str, db_port: str) -> None:
        self.db_name = my_db
        self.db_ip = db_ip
        self.db_port = db_port
        try:
            self.db_pass = CONFIG[self.db_name]["db_password"]
            self.db_user = CONFIG[self.db_name]["db_user"]
        except Exception as error:
            logger.error(f"Loading db_auth for {self.db_name}, error: {error}")

    def set_engine(self) -> None:
        try:
            db_login = f"postgresql://{self.db_user}:{self.db_pass}"
            db_uri = f"{db_login}@{self.db_ip}:{self.db_port}/{self.db_name}"
            logger.info(f"Connecting to PostgreSQL {self.db_name}")
            self.engine = sqlalchemy.create_engine(
                db_uri,
                connect_args={"connect_timeout": 10, "application_name": "adscrawler"},
            )
        except Exception as error:
            logger.error(
                f"Failed to connect {self.db_name} @ {self.db_ip}, error: {error}",
            )
            self.db_name = None


def get_db_connection(use_ssh_tunnel: bool = False) -> PostgresCon:
    server_name = "madrone"
    host = CONFIG[server_name]["host"]  # Remote server
    if use_ssh_tunnel:
        server = open_ssh_tunnel(host, server_name)
        server.start()
        db_port = str(server.local_bind_port)
        host = "127.0.0.1"
    else:
        db_port = str(5432)
    conn = PostgresCon(server_name, host, db_port)
    return conn


def open_ssh_tunnel(remote_host: str, server_name: str) -> SSHTunnelForwarder:
    with SSHTunnelForwarder(
        (remote_host, 22),  # Remote server IP and SSH port
        ssh_username=CONFIG[server_name]["os_user"],
        remote_bind_address=("127.0.0.1", 5432),
        ssh_pkey=CONFIG[server_name].get("ssh_pkey", None),
        ssh_private_key_password=CONFIG[server_name].get("ssh_pkey_password", None),
    ) as server:  # PostgreSQL server IP and sever port on remote machine
        server.start()  # start ssh sever
        logger.info("Connecting via SSH")
        # connect to PostgreSQL
    return server
