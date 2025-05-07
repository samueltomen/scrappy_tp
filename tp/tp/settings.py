BOT_NAME = "tp"

SPIDER_MODULES = ["tp.spiders"]
NEWSPIDER_MODULE = "tp.spiders"

ITEM_PIPELINES = {
    "tp.pipelines.MongoPipeline": 300,
}

MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "scrapy_tp"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

ROBOTSTXT_OBEY = False
