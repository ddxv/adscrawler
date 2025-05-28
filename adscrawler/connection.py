from contextlib import contextmanager
from socket import gethostbyname

import sqlalchemy
from sqlalchemy.engine import Engine
from sshtunnel import SSHTunnelForwarder

from .config import CONFIG, get_logger

logger = get_logger(__name__)


class PostgresCon:
    """Class for managing the connection to PostgreSQL with extended database operations."""

    def __init__(self, db_name: str, db_ip: str, db_port: str) -> None:
        """
        Initialize the PostgreSQL connection.
        Args:
            db_name (str): Name of the database.
            db_ip (str): IP address of the database server.
            db_port (str): Port number of the database server.
        """
        self.db_name = db_name
        self.db_ip = db_ip
        self.db_port = db_port
        self.engine: Engine

        try:
            self.db_pass = CONFIG[self.db_name]["db_password"]
            self.db_user = CONFIG[self.db_name]["db_user"]
        except KeyError as error:
            logger.error(f"Loading db_auth for {self.db_name}, error: {error}")
            raise

    def set_engine(self) -> None:
        """Set up the SQLAlchemy engine."""
        try:
            db_login = f"postgresql+psycopg://{self.db_user}:{self.db_pass}"
            db_uri = f"{db_login}@{self.db_ip}:{self.db_port}/{self.db_name}"
            logger.info(f"Adscrawler connecting to PostgreSQL {self.db_name}")
            self.engine = sqlalchemy.create_engine(
                db_uri,
                connect_args={"connect_timeout": 10, "application_name": "adscrawler"},
            )
        except Exception as error:
            logger.error(
                f"Failed to connect {self.db_name} @ {self.db_ip}, error: {error}",
            )
            raise

    @contextmanager
    def get_cursor(self) -> None:
        """Context manager for database connection and cursor."""
        conn = self.engine.raw_connection()
        try:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation error: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()


def get_db_connection(
    use_ssh_tunnel: bool = False, server_name: str = "madrone"
) -> PostgresCon:
    """
    Get a database connection, optionally using an SSH tunnel.

    Args:
        use_ssh_tunnel (bool): Whether to use an SSH tunnel for the connection.

    Returns:
        PostgresCon: A PostgreSQL connection object.
    """
    host = get_host_ip(CONFIG[server_name]["host"])

    if use_ssh_tunnel:
        ssh_port = CONFIG[server_name].get("ssh_port", 22)
        remote_port = CONFIG[server_name].get("remote_port", 5432)
        server = SSHTunnelForwarder(
            (host, ssh_port),
            ssh_username=CONFIG[server_name]["os_user"],
            remote_bind_address=("127.0.0.1", remote_port),
            ssh_pkey=CONFIG[server_name].get("ssh_pkey"),
            ssh_private_key_password=CONFIG[server_name].get("ssh_pkey_password"),
        )
        server.start()
        db_port = str(server.local_bind_port)
        host = "127.0.0.1"
    else:
        db_port = "5432"

    conn = PostgresCon(server_name, host, db_port)
    conn.set_engine()
    return conn


def get_host_ip(hostname: str) -> str:
    """Convert hostname to IPv4 address if needed."""
    # Check if hostname is already an IPv4 address
    if all(part.isdigit() and 0 <= int(part) <= 255 for part in hostname.split(".")):  # noqa: PLR2004
        return hostname
    ip_address = gethostbyname(hostname)
    logger.info(f"Resolved {hostname} to {ip_address}")
    return ip_address
