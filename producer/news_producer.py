import feedparser
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import os

FEED_URL = "https://feeds.bloomberg.com/markets/news.rss"
KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "news"
CACHE_FILE = "sent_news.json"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    with open(CACHE_FILE, "r") as f:
        sent_links = set(json.load(f))
except (json.JSONDecodeError, FileNotFoundError):
    sent_links = set()

etag = None
modified = None

print("Bắt đầu RSS → Kafka producer...")

while True:
    try:
        feed = feedparser.parse(FEED_URL, etag=etag, modified=modified)

        etag = feed.get("etag")
        modified = feed.get("modified")

        if feed.status == 304:
            print("No new entries.")
        else:
            new_sent = False
            for entry in feed.entries:
                link = entry.get("link", "")
                if not link or link in sent_links:
                    continue  # bỏ qua tin trùng

                published_parsed = getattr(entry, "published_parsed", None)
                if published_parsed:
                    formatted_date = time.strftime("%d-%m-%Y %H:%M:%S", published_parsed)
                else:
                    formatted_date = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

                news_json = {
                    "title": entry.get("title", ""),
                    "link": link,
                    "summary": entry.get("summary", ""),
                    "published": formatted_date
                }

                producer.send(KAFKA_TOPIC, news_json)
                sent_links.add(link)
                new_sent = True
                print(f"✅ Sent to Kafka: {news_json['title']}")

            # Lưu cache nếu có tin mới
            if new_sent:
                with open(CACHE_FILE, "w") as f:
                    json.dump(list(sent_links), f)
        time.sleep(300)

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(60)
