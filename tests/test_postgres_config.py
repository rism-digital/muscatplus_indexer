import pytest
from psycopg.conninfo import conninfo_to_dict

from indexer.helpers.db import postgres_conninfo, project_connection_info


@pytest.fixture
def index_config() -> dict:
    return {
        "postgres": {
            "server": "127.0.0.1",
            "muscat": {
                "username": "muscat_user",
                "password": "muscat password",
                "database": "muscat_db",
            },
            "diamm": {
                "username": "diamm_user",
                "password": "diamm password",
                "database": "diamm_db",
            },
            "cantus": {
                "username": "cantus_user",
                "password": "cantus password",
                "database": "cantus_db",
            },
        }
    }


@pytest.mark.parametrize(
    ("project", "username", "database"),
    [
        ("muscat", "muscat_user", "muscat_db"),
        ("diamm", "diamm_user", "diamm_db"),
        ("cantus", "cantus_user", "cantus_db"),
    ],
)
def test_project_connection_info_is_project_specific(
    index_config: dict, project: str, username: str, database: str
) -> None:
    connection = project_connection_info(project, index_config)

    assert connection == {
        "server": "127.0.0.1",
        "username": username,
        "password": f"{project} password",
        "database": database,
    }


def test_postgres_conninfo_uses_shared_server(index_config: dict) -> None:
    parameters = conninfo_to_dict(postgres_conninfo("diamm", index_config))

    assert parameters == {
        "hostaddr": "127.0.0.1",
        "dbname": "diamm_db",
        "user": "diamm_user",
        "password": "diamm password",
    }


def test_postgres_conninfo_uses_local_socket_for_empty_server(
    index_config: dict,
) -> None:
    index_config["postgres"]["server"] = ""

    parameters = conninfo_to_dict(postgres_conninfo("cantus", index_config))

    assert "hostaddr" not in parameters
    assert parameters["dbname"] == "cantus_db"
