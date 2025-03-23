from prefect import flow, task
from prefect_sqlalchemy import (ConnectionComponents, SqlAlchemyConnector,
                                SyncDriver)

connector = SqlAlchemyConnector(
    connection_info=ConnectionComponents(
        driver=SyncDriver.SQLITE_PYSQLITE, database="db_test.db"
    )
)

connector.save("testdb", overwrite=True)
