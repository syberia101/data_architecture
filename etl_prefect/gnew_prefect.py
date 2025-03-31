from datetime import datetime, timedelta

import httpx
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver


connector = SqlAlchemyConnector(
    connection_info=ConnectionComponents(
        driver=SyncDriver.SQLITE_PYSQLITE, database="db_test.db"
    )
)

connector.save("testdb", overwrite=True)


@task(name="Setup Table")
def setup_table(block_name: str) -> None:
    """Create the table if it does not exist
    
    Args:
        block_name (str): The name of the block
    """
    
    with SqlAlchemyConnector.load(block_name) as connector:
        connector.execute(
            "CREATE TABLE IF NOT EXISTS Article (article_id INTEGER PRIMARY KEY,title varchar, description varchar,url varchar UNIQUE, publisherAt, source_id, FOREIGN KEY (source_id) REFERENCES Source(source_id));"
        )
        connector.execute(
            "CREATE TABLE IF NOT EXISTS Source (source_id INTEGER PRIMARY KEY,name varchar UNIQUE, url varchar UNIQUE);"
        )


@task(name="Insert Article and source")
def insert_article(block_name: str, json_response: dict):
    """Insert the articles and source into the database if the source does not exist
        
    Args:
        block_name (str): The name of the block
        json_response (dict): The json response from the API
        """
    with SqlAlchemyConnector.load(block_name) as connector:
        for article in json_response["articles"]:
            title = article["title"].lower()
            description = article["description"]
            url_article = article["url"].lower()
            published_at = article["publishedAt"]
            source_name = article["source"]["name"].lower().strip()
            url_source = article["source"]["url"].lower().strip()
            print(
                f"""
                      - title {title} 
                      - description {description} 
                      - url article {url_article} 
                      - published_at {published_at}
                      - source_name:{source_name} 
                      - url_source {url_source}"""
            )

            # using mysqlalchemy select the source_id from the source table where the name is equal to source_name
            connector.execute(
                "INSERT OR IGNORE INTO Source (name, url) VALUES (:name, :url);",
                parameters={"name": source_name, "url": url_source},
            )
            source_id = connector.execute(
                "SELECT source_id FROM Source WHERE url = :url;",
                parameters={"url": url_source},
            ).fetchone()

            print("SOURCE id ", source_id)

            if source_id:
                connector.execute(
                    "INSERT or IGNORE INTO Article (title, description, url, publisherAt, source_id) VALUES (:title, :description, :url, :publisherAt, :source_id);",
                    parameters={
                        "title": title,
                        "description": description,
                        "url": url_article,
                        "publisherAt": published_at,
                        "source_id": source_id[0],
                    },
                )
            else:
                print("SHOULD HAVE QUERY THE SOURCE TABLE ", url_source)


@task (name="Get Google News")
def get_gnews(subject:str, api_secret:str) -> dict:
    """Get the latest news from Google News
    return dict: The json response from the API
    """

    now = datetime.now() - timedelta(days=1)
    print("FORMATED DATE", now.strftime("%Y-%m-%dT%H:%M:%SZ"))
    """Get the latest news from Google News"""
    params = {
        "q": subject,
        "country": "ch",
        "apikey": api_secret,
        "max": 10,
        "lang": "fr",
        "from": now,
    }
    try:
        response = httpx.get("https://gnews.io/api/v4/search", params=params)   
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print("Error fetching data from Google News", e)
        return None

@flow(name="News Flows")
def news_flows(block_name: str, subject:str, api_secret:str) -> list:
    """Flow to get the latest news from Google News and insert it into the database
    args:
        block_name (str): The name of the block to connect to the database.
    """
    setup_table(block_name)
    news = get_gnews(subject=subject,api_secret=api_secret)
    print("NEWS articles  ",news["totalArticles"])
    insert_article(block_name, news)


if __name__ == "__main__":
        
        news_flows.serve(
        name="Google News",
        tags=["Gnews"],
        parameters={"block_name": "testdb","subject":"Europe IA","api_secret":"WRITE YOUR OWN API KEY"},    
        interval=timedelta(days=1),
    )
     
