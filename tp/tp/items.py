import scrapy


class CompanyItem(scrapy.Item):
    enterprise_number = scrapy.Field()
    kbo_data = scrapy.Field()
    ejustice_publications = scrapy.Field()
    consult_deposits = scrapy.Field()


class KboItem(scrapy.Item):
    enterprise_number = scrapy.Field()
    general_info = scrapy.Field()
    functions = scrapy.Field()
    entrepreneurial_capacities = scrapy.Field()
    qualities = scrapy.Field()
    authorizations = scrapy.Field()
    nace_codes = scrapy.Field()
    financial_data = scrapy.Field()
    entity_links = scrapy.Field()
    external_links = scrapy.Field()


class EjusticeItem(scrapy.Item):
    enterprise_number = scrapy.Field()
    publications = scrapy.Field()
