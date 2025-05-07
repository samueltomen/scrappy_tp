import scrapy
import csv
import os
from tp.items import KboItem
from scrapy.http import Request
import time


class KboSpider(scrapy.Spider):
    name = "kbo"
    allowed_domains = ["kbopub.economie.fgov.be"]

    def __init__(self, *args, **kwargs):
        super(KboSpider, self).__init__(*args, **kwargs)

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
                    url = f"https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={enterprise_number_clean}&lang=fr"

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

        if "toonondernemingps" in response.url:
            self.logger.info("Page d'entreprise correcte détectée")

            try:
                self.logger.info("Début de l'extraction des données...")
                item = self.parse_enterprise(response)

                self.logger.info(f"Fonctions extraites: {len(item['functions'])}")

                # Process the item directly or yield it
                yield item

            except Exception as e:
                self.logger.error(f"Erreur lors du traitement des données: {str(e)}")
                import traceback

                self.logger.error(traceback.format_exc())
        else:
            self.logger.error(f"Page incorrecte: {response.url}")

    def parse_enterprise(self, response):
        item = KboItem()
        item["enterprise_number"] = response.meta["enterprise_number"]

        item["general_info"] = self.extract_general_info(response)
        self.logger.info(f"Informations générales: {item['general_info']}")

        item["functions"] = self.extract_functions(response)
        self.logger.info(f"Fonctions: {len(item['functions'])} trouvées")

        item["entrepreneurial_capacities"] = self.extract_capacities(response)
        self.logger.info(
            f"Capacités entrepreneuriales: {item['entrepreneurial_capacities']}"
        )

        item["qualities"] = self.extract_qualities(response)
        self.logger.info(f"Qualités: {len(item['qualities'])} trouvées")

        item["authorizations"] = self.extract_authorizations(response)
        self.logger.info(f"Autorisations: {len(item['authorizations'])} trouvées")

        item["nace_codes"] = self.extract_nace_codes(response)
        self.logger.info(f"Codes NACE 2025: {len(item['nace_codes']['2025'])} trouvés")
        self.logger.info(f"Codes NACE 2008: {len(item['nace_codes']['2008'])} trouvés")
        self.logger.info(f"Codes NACE 2003: {len(item['nace_codes']['2003'])} trouvés")

        item["financial_data"] = self.extract_financial_data(response)
        self.logger.info(f"Données financières: {item['financial_data']}")

        item["entity_links"] = self.extract_entity_links(response)
        self.logger.info(f"Liens entre entités: {len(item['entity_links'])}")

        item["external_links"] = self.extract_external_links(response)
        self.logger.info(f"Liens externes: {len(item['external_links'])} trouvés")

        return item

    def extract_general_info(self, response):
        general_info = {}

        is_french = "Généralités" in response.text

        if is_french:
            section_title = "Généralités"
        else:
            section_title = "Algemeen"

        self.logger.info(
            f"Page en français: {is_french}, recherche de la section: {section_title}"
        )

        rows = response.xpath(
            '//tr[td/h2[contains(text(), "Généralités")]]/following-sibling::tr'
        )

        for row in rows:
            if row.xpath("./td/h2").get():
                break

            label = row.xpath("./td[1]/text()").get()
            if label:
                label = label.strip().replace(":", "")
                value_parts = row.xpath("./td[position()>1]//text()").getall()
                value = " ".join([part.strip() for part in value_parts if part.strip()])

                if label and value:
                    general_info[label] = value
                    self.logger.info(f"Trouvé: {label} = {value}")

        return general_info

    def extract_functions(self, response):
        functions = []

        functions_table = response.xpath('//table[@id="toonfctie"]//tr')
        self.logger.info(
            f"Nombre de lignes dans le tableau des fonctions: {len(functions_table)}"
        )

        for row in functions_table:
            cells = row.xpath("./td")
            if len(cells) >= 3:
                function = {
                    "title": "".join(cells[0].xpath(".//text()").getall()).strip(),
                    "name": "".join(cells[1].xpath(".//text()").getall()).strip(),
                    "date": "".join(cells[2].xpath(".//text()").getall())
                    .strip()
                    .replace("Depuis le ", ""),
                }
                if function["title"] and function["name"]:
                    functions.append(function)
                    self.logger.info(f"Fonction trouvée: {function}")

        return functions

    def extract_capacities(self, response):
        capacities = {}

        capacities_section = response.xpath(
            '//tr[td/h2[contains(text(), "Capacités entrepreneuriales")]]/following-sibling::tr[1]'
        )

        if capacities_section:
            capacity_texts = capacities_section.xpath(
                './td[contains(@class, "QL")]//text()'
            ).getall()
            text = " ".join([t.strip() for t in capacity_texts if t.strip()])

            if text:
                capacities["info"] = text
                self.logger.info(f"Capacité entrepreneuriale trouvée: {text}")

        return capacities

    def extract_qualities(self, response):
        qualities = []

        qualities_section = response.xpath(
            '//tr[td/h2[contains(text(), "Qualités")]]/following-sibling::tr'
        )

        for row in qualities_section:
            if row.xpath("./td/h2").get():
                break

            quality_texts = row.xpath(
                './td[contains(@class, "QL") or contains(@class, "RL")]//text()'
            ).getall()
            if quality_texts:
                text = " ".join([t.strip() for t in quality_texts if t.strip()])
                if text:
                    qualities.append(text)
                    self.logger.info(f"Qualité trouvée: {text}")

        return qualities

    def extract_authorizations(self, response):
        authorizations = []

        auth_section = response.xpath(
            '//tr[td/h2[contains(text(), "Autorisations")]]/following-sibling::tr'
        )

        for row in auth_section:
            if row.xpath("./td/h2").get():
                break

            auth_texts = row.xpath('./td[contains(@class, "QL")]//text()').getall()
            if auth_texts:
                text = " ".join([t.strip() for t in auth_texts if t.strip()])
                if text:
                    authorizations.append(text)
                    self.logger.info(f"Autorisation trouvée: {text}")

        return authorizations

    def extract_nace_codes(self, response):
        nace_codes = {"2025": [], "2008": [], "2003": []}

        # Extract NACE 2025 TVA codes
        nace_2025_tva_section = response.xpath(
            '//tr[td/h2[contains(text(), "Activités TVA Code Nacebel version 2025")]]/following-sibling::tr'
        )
        for row in nace_2025_tva_section:
            if (
                row.xpath("./td/h2").get()
                or row.xpath('.//span[@id="klikbtw2008"]').get()
            ):
                break

            code_texts = row.xpath('./td[contains(@class, "QL")]//text()').getall()
            if code_texts:
                code_text = " ".join([t.strip() for t in code_texts if t.strip()])
                if "TVA 2025" in code_text:
                    parts = code_text.split("-", 1)
                    if len(parts) > 1:
                        code_part = parts[0].strip()
                        code_number = (
                            code_part.split()[-1].strip()
                            if len(code_part.split()) > 1
                            else ""
                        )
                        description = parts[1].strip()
                        nace_codes["2025"].append(
                            {
                                "type": "TVA",
                                "code": code_number,
                                "description": description,
                                "date": self._extract_date_from_text(code_text),
                            }
                        )
                        self.logger.info(f"Code NACE TVA 2025 trouvé: {code_text}")

        # Extract NACE 2025 ONSS codes
        nace_2025_onss_section = response.xpath(
            '//tr[td/h2[contains(text(), "Activités ONSS Code Nacebel version 2025")]]/following-sibling::tr'
        )
        for row in nace_2025_onss_section:
            if (
                row.xpath("./td/h2").get()
                or row.xpath('.//span[@id="klikbtw2008"]').get()
            ):
                break

            code_texts = row.xpath('./td[contains(@class, "QL")]//text()').getall()
            if code_texts:
                code_text = " ".join([t.strip() for t in code_texts if t.strip()])
                if "ONSS2025" in code_text:
                    parts = code_text.split("-", 1)
                    if len(parts) > 1:
                        code_part = parts[0].strip()
                        code_number = (
                            code_part.split()[-1].strip()
                            if len(code_part.split()) > 1
                            else ""
                        )
                        description = parts[1].strip()
                        nace_codes["2025"].append(
                            {
                                "type": "ONSS",
                                "code": code_number,
                                "description": description,
                                "date": self._extract_date_from_text(code_text),
                            }
                        )
                        self.logger.info(f"Code NACE ONSS 2025 trouvé: {code_text}")

        # Note: NACE 2008 and 2003 codes would require JavaScript interaction
        # which is not possible with standard Scrapy requests
        self.logger.info(
            "Les codes NACE 2008 et 2003 ne sont pas extraits sans Selenium"
        )

        return nace_codes

    def extract_financial_data(self, response):
        financial_data = {}

        financial_section = response.xpath(
            '//tr[td/h2[contains(text(), "Données financières")]]/following-sibling::tr'
        )

        for row in financial_section:
            if row.xpath("./td/h2").get():
                break

            label = row.xpath("./td[1]/text()").get()
            if label:
                label = label.strip().replace(":", "")
                value = "".join(row.xpath("./td[2]//text()").getall()).strip()

                if label and value:
                    financial_data[label] = value
                    self.logger.info(f"Donnée financière trouvée: {label} = {value}")

        return financial_data

    def extract_entity_links(self, response):
        links_section = response.xpath(
            '//tr[td/h2[contains(text(), "Liens entre entités")]]/following-sibling::tr'
        )

        no_data = links_section.xpath(
            './td[contains(text(), "Pas de données reprises dans la BCE")]'
        ).get()
        if no_data:
            self.logger.info("Pas de liens entre entités disponibles")
            return [{"info": "Pas de données reprises dans la BCE"}]

        entity_links = []

        for row in links_section:
            if row.xpath("./td/h2").get():
                break

            all_text = row.xpath("./td//text()").getall()
            text = " ".join([t.strip() for t in all_text if t.strip()])

            entity_number = row.xpath("./td//a/text()").get("")

            if text and not "Pas de données" in text:
                link_info = {"enterprise_number": entity_number, "relation": text}
                entity_links.append(link_info)
                self.logger.info(f"Lien entre entités trouvé: {link_info}")

        if not entity_links:
            return [{"info": "Aucun lien entre entités trouvé"}]

        return entity_links

    def extract_external_links(self, response):
        external_links = []

        links_section = response.xpath(
            '//tr[td/h2[contains(text(), "Liens externes")]]/following-sibling::tr'
        )

        for row in links_section:
            if row.xpath("./td/h2").get():
                break

            links = row.xpath("./td//a")

            for link in links:
                link_text = link.xpath("./text()").get("").strip()
                link_url = link.xpath("./@href").get("")

                if link_text and link_url:
                    external_link = {"text": link_text, "url": link_url}
                    external_links.append(external_link)
                    self.logger.info(f"Lien externe trouvé: {link_text} -> {link_url}")

        return external_links

    def _extract_date_from_text(self, text):
        if "Depuis le" in text:
            date_part = text.split("Depuis le")[1].strip()
            return date_part
        return ""
