from datetime import datetime, timedelta

import httpx
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="Setup Table")
def setup_table(block_name: str) -> None:
    with SqlAlchemyConnector.load(block_name) as connector:
        connector.execute(
            "CREATE TABLE IF NOT EXISTS Article (article_id INTEGER PRIMARY KEY,title varchar, description varchar,url varchar UNIQUE, publisherAt, source_id, FOREIGN KEY (source_id) REFERENCES Source(source_id));"
        )
        connector.execute(
            "CREATE TABLE IF NOT EXISTS Source (source_id INTEGER PRIMARY KEY,name varchar UNIQUE, url varchar UNIQUE);"
        )


@task(name="Insert Article and source")
def insert_article(block_name: str, json_response: dict):
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
def get_gnews():
    now = datetime.now() - timedelta(days=1)
    print("FORMATED DATE", now.strftime("%Y-%m-%dT%H:%M:%SZ"))
    """Get the latest news from Google News"""
    params = {
        "q": "trump diversity",
        "country": "ch",
        "apikey": "11609155901ff8946d84c2c9aff210e7",
        "max": 10,
        "lang": "fr",
        "from": now,
    }
    return httpx.get("https://gnews.io/api/v4/search", params=params).json()


@flow(name="News Flows")
def news_flows(block_name: str) -> list:
    setup_table(block_name)
    news = get_gnews()
    insert_article(block_name, news)


if __name__ == "__main__":
        news_flows.serve(
        name="Google News",
        tags=["Gnews"],
        parameters={"block_name": "testdb"},
        interval=timedelta(days=1),
    )

