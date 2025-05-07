import scrapy
import csv
import os
from tp.items import EjusticeItem
from scrapy.http import Request


class EjusticeSpider(scrapy.Spider):
    name = "ejustice"
    allowed_domains = ["ejustice.just.fgov.be"]

    def __init__(self, *args, **kwargs):
        super(EjusticeSpider, self).__init__(*args, **kwargs)

    def start_requests(self, limit=5):
        base_dir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        csv_path = os.path.join(base_dir, "enterprise.csv")
        count = 0

        try:
            with open(csv_path, "r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if count >= limit:
                        break

                    enterprise_number = row["EnterpriseNumber"]
                    enterprise_number_clean = enterprise_number.replace(".", "")
                    url = f"https://www.ejustice.just.fgov.be/cgi_tsv/list.pl?btw={enterprise_number_clean}"
                    print(url)
                    yield Request(
                        url=url,
                        callback=self.parse,
                        meta={"enterprise_number": enterprise_number},
                    )
                    count += 1
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture du CSV: {str(e)}")

    def parse(self, response):
        self.logger.info(f"Traitement de la page: {response.url}")

        if "list.pl?btw=" in response.url:
            self.logger.info("Page de publications correcte détectée")

            self.logger.debug(
                f"Response body (first 1000 chars): {response.text[:1000]}"
            )

            try:
                item = self.parse_publications(response)
                yield item

            except Exception as e:
                self.logger.error(f"Erreur lors du traitement des données: {str(e)}")
                import traceback

                self.logger.error(traceback.format_exc())
        else:
            self.logger.error(f"Page incorrecte: {response.url}")

    def parse_publications(self, response):
        item = EjusticeItem()
        item["enterprise_number"] = response.meta["enterprise_number"]
        item["publications"] = []

        publication_entries = response.xpath(
            "//*[contains(@class, 'publication') or contains(@class, 'list-item')]"
        )

        if not publication_entries:
            publication_entries = response.xpath("//table//tr")[1:]  # Skip header row

        for entry in publication_entries:
            publication = {
                "number": "",
                "title_and_code": "",
                "address": "",
                "type": "",
                "date": "",
                "reference": "",
                "image_url": "",
            }

            number_text = entry.xpath(
                ".//*[contains(text(), 'Numéro') or contains(text(), 'Number')]/following-sibling::text() | .//*[contains(text(), 'Numéro') or contains(text(), 'Number')]/following-sibling::*/text()"
            ).get(default="")
            title_text = entry.xpath(
                ".//*[contains(text(), 'Titre') or contains(text(), 'Title')]/following-sibling::text() | .//*[contains(text(), 'Titre') or contains(text(), 'Title')]/following-sibling::*/text()"
            ).get(default="")
            address_text = entry.xpath(
                ".//*[contains(text(), 'Adresse') or contains(text(), 'Address')]/following-sibling::text() | .//*[contains(text(), 'Adresse') or contains(text(), 'Address')]/following-sibling::*/text()"
            ).get(default="")
            type_text = entry.xpath(
                ".//*[contains(text(), 'Type')]/following-sibling::text() | .//*[contains(text(), 'Type')]/following-sibling::*/text()"
            ).get(default="")
            date_text = entry.xpath(
                ".//*[contains(text(), 'Date')]/following-sibling::text() | .//*[contains(text(), 'Date')]/following-sibling::*/text()"
            ).get(default="")
            reference_text = entry.xpath(
                ".//*[contains(text(), 'Référence') or contains(text(), 'Reference')]/following-sibling::text() | .//*[contains(text(), 'Référence') or contains(text(), 'Reference')]/following-sibling::*/text()"
            ).get(default="")
            image_url = entry.xpath(
                ".//*[contains(text(), 'Image') or contains(text(), 'PDF')]/following-sibling::a/@href | .//a[contains(@href, '.pdf')]/@href"
            ).get(default="")

            publication["number"] = number_text.strip()
            publication["title_and_code"] = title_text.strip()
            publication["address"] = address_text.strip()
            publication["type"] = type_text.strip()
            publication["date"] = date_text.strip()
            publication["reference"] = reference_text.strip()
            publication["image_url"] = image_url.strip()

            if not any(publication.values()):
                publication["number"] = (
                    entry.xpath("./td[1]//text()").get(default="").strip()
                )
                publication["title_and_code"] = (
                    entry.xpath("./td[2]//text()").get(default="").strip()
                )
                publication["address"] = (
                    entry.xpath("./td[3]//text()").get(default="").strip()
                )
                publication["type"] = (
                    entry.xpath("./td[4]//text()").get(default="").strip()
                )
                publication["date"] = (
                    entry.xpath("./td[5]//text()").get(default="").strip()
                )
                publication["reference"] = (
                    entry.xpath("./td[6]//text()").get(default="").strip()
                )
                publication["image_url"] = (
                    entry.xpath("./td[7]//a/@href").get(default="").strip()
                )

            if any(publication.values()):
                item["publications"].append(publication)
                self.logger.info(f"Publication extraite: {publication}")

        self.logger.info(f"Total publications extraites: {len(item['publications'])}")
        return item
