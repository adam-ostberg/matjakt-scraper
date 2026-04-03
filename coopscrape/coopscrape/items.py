import scrapy


class ProductItem(scrapy.Item):
    name = scrapy.Field()
    price = scrapy.Field()
