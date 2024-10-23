import datetime
import hashlib
import json
import os
import random
import uuid
import pymysql
import pandas as pd
import scrapy
from curl_cffi import requests
from scrapy.cmdline import execute
from datetime import datetime,date
from zepto_.items import Zepto_comp


class ZeptoCompetitorSpider(scrapy.Spider):
    name = "zepto_Competitor"
    # allowed_domains = ["zeptonow.com"]
    # start_urls = ["https://zeptonow.com"]
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }
    def __init__(self):
        try:
            self.conn = pymysql.Connect(host='localhost',
                                        user='root',
                                        password='actowiz',
                                        database='zepto_')
            self.cur = self.conn.cursor()

        except Exception as e:
            print(e)

    def start_requests(self):
        query = 'select *from zepto_links_comp where status="pending"'
        self.cur.execute(query)
        rows = self.cur.fetchall()
        today_date = str(date.today()).replace('-', '_')
        folder_loc = f'C:/Shalu/PageSave/Zepto_weekly/comp/{today_date}/'
        if not os.path.exists(folder_loc):
            os.makedirs(folder_loc,exist_ok=True)

        for row in rows:
            city = row[1]
            zipcode = row[0]

            storeid = row[2]
            longitude = row[4]
            latitude = row[3]
            brand_name=row[5]
            Brand_Url=row[6]
            unique_id=row[8]
            prod_id = Brand_Url.split('/')
            prod_id = prod_id[-1]
            # unique_id = hashlib.sha256((str(zipcode) + city + Brand_Url).encode()).hexdigest()
            # update_query = """
            #                    UPDATE zepto_links_comp
            #                    SET unique_id = %s
            #                    WHERE pincode = %s AND city = %s AND url = %s
            #                    """
            # self.cur.execute(update_query, (unique_id,  zipcode, city, Brand_Url))
            # self.conn.commit()
            # print("Row updated successfully.")
            main_loc = folder_loc + f"{unique_id}.html"

            if not os.path.isfile(main_loc):
                meta = {}
                browsers = [
                    "chrome110",
                    "edge99",
                    "safari15_5"
                ]
                meta["impersonate"] = random.choice(browsers)
                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'accept-language': 'en-US,en;q=0.9',
                    'appversion': '11.7.0-WEB',
                    'cache-control': 'no-cache',
                    'compatible_components': 'CONVENIENCE_FEE,RAIN_FEE,EXTERNAL_COUPONS,STANDSTILL,BUNDLE,MULTI_SELLER_ENABLED,PIP_V1,NEW_FEE_STRUCTURE,NEW_BILL_INFO,RE_PROMISE_ETA_ORDER_SCREEN_ENABLED,MANUALLY_APPLIED_DELIVERY_FEE_RECEIVABLE,MARKETPLACE_REPLACEMENT,ZEPTO_PASS,ZEPTO_PASS:1,ZEPTO_PASS:2,ZEPTO_PASS_RENEWAL',
                    # 'deviceid': '4595536922659838',
                    'origin': 'https://www.zeptonow.com',
                    'platform': 'WEB',
                    'pragma': 'no-cache',
                    'priority': 'u=1, i',
                    'referer': 'https://www.zeptonow.com/',
                    'requestid': '691278856699332',
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'cross-site',
                    'sessionid': '6096416937665581',
                    'store_id': f'{storeid}',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                }

                r4 = requests.get(f'https://api.zepto.co.in/api/v1/inventory/store/{storeid}/detailed-info',
                                  headers=headers).json()
                name = r4['name']
                city_all = r4['city']
                sellerInfo = r4['sellerInfo']
                cityId = r4['cityId']
                openTime = r4['openTime']
                closeTime = r4['closeTime']
                id = r4['id']

                store_info = {
                    "storeServiceableResponseV2": [
                        {
                            "serviceable": True,
                            "storeId": storeid,
                            "storeConstruct": "PRIMARY_STORE"
                        }
                    ],
                    "storeServiceableResponse": {
                        "serviceable": True,
                        "storeId": storeid,
                        "userId": None,
                        "etaServiceability": None,
                        "distance": None,
                        "storeLatitude": None,
                        "storeLongitude": None,
                        "secondaryStoreIds": None
                    },
                    "primaryStoreInfo": {
                        "name": name,
                        "isOnline": True,
                        "openTime": openTime,
                        "closeTime": closeTime,
                        "id": id,
                        "cityId": cityId,
                        "city": city_all,
                        "sellerInfo": sellerInfo,
                        "isOtofEnabled": False,
                        "noBagDeliveryEnabled": True,
                        "isNoBagDeliveryNew": True,
                        "noBagDeliveryDefaultOptStatus": True,
                        "standStillMode": False,
                        "raining": False,
                        "isFullNightDeliveryEnabled": False,
                        "issueAtStore": False,
                        "takingOrders": True,
                        "cartV3Enabled": True,
                        "cartV2Enabled": False
                    },
                    "etaServiceableInfo": [
                        {
                            "storeId": storeid,
                            "etaInMinutes": "12",
                            "deliverableType": "OPEN",
                            "deliverableSubtype": "ETA_NORMAL",
                            "isDeliverable": True
                        }
                    ]
                }
                cookies = {
                    'unique_browser_id': '4595536922659838',
                    '_gcl_au': '1.1.760092518.1721989156',
                    '_fbp': 'fb.1.1721989156461.627741631865033537',
                    '_ga': 'GA1.1.631977533.1721989157',
                    'maxWeightLimitCart': '25000',
                    'csrfSecret': 'EDb1KN0AGiNrD3wSdb0hICnA',
                    'mp_dcc8757645c1c32f4481b555710c7039_mixpanel': '%7B%22distinct_id%22%3A%20%22%24device%3A19107c7b48aef8-042e5ed5d9db76-26001e51-e1000-19107c7b48bef9%22%2C%22%24device_id%22%3A%20%2219107c7b48aef8-042e5ed5d9db76-26001e51-e1000-19107c7b48bef9%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D',
                    'XSRF-TOKEN': 'lubVmQA6-3B8jYtlEsL3jfH3PynFZH_Sohf4.PDskDHfMdPHQf5%2FBQQHev%2B8nlWRzgf8D00qr7Ww25Cc',
                    '_ga_37QQVCR1ZS': 'GS1.1.1722952192.12.1.1722952445.60.0.0',
                    'latitude': f'{latitude}',
                    'longitude': f'{longitude}',
                    'store-info': json.dumps(store_info),
                    '_ga_52LKG2B3L1': 'GS1.1.1722952192.40.1.1722952446.60.0.1265190564',
                }

                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'no-cache',
                    'pragma': 'no-cache',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                }

                response2 = requests.request('GET', url=f'{Brand_Url}',
                                             cookies=cookies,
                                             headers=headers, impersonate=random.choice(browsers)
                                             )

                if response2.status_code == 200:
                    with open(main_loc, 'wb') as file:
                        file.write(response2.text.encode('utf-8'))

                yield scrapy.Request(url="file://" + main_loc, callback=self.parse, meta = {"zipcode": zipcode,
                        "city": city,
                        "unique_id": unique_id,
                        "Brand": brand_name,
                        "Brand_Url": Brand_Url,
                        "prod_id": prod_id})

            else:

                yield scrapy.Request(url="file://" + main_loc, callback=self.parse,meta = {"zipcode": zipcode,
                        "city": city,
                        "unique_id": unique_id,
                        "Brand": brand_name,
                        "Brand_Url": Brand_Url,
                        "prod_id": prod_id})


    def parse(self, response):
        item = Zepto_comp()

        if response.meta['Brand_Url']:
            try:
                data = response.xpath('//div[@class="flex items-center"]')

                # Extract selling price, if not found set to empty string
                brand_selling_price = data.xpath('./h4/text()').extract_first()
                brand_selling_price = brand_selling_price.replace('₹', "") if brand_selling_price else ' '

                # Extract MRP, if not found set to empty string
                brand_mrp = data.xpath('./p/text()').extract_first()
                brand_mrp = brand_mrp.replace('₹', "") if brand_mrp else ' '

                # Extract discount, if not found set to empty string
                brand_discount = data.xpath('./div/text()').extract_first()
                brand_discount = brand_discount.replace('Off', "") if brand_discount else ' '

                # Calculate discount amount, if prices are not available, set to empty string
                if brand_mrp != ' ' and brand_selling_price != ' ':
                    brand_discount_amount = int(brand_mrp) - int(brand_selling_price)
                else:
                    brand_discount_amount = ' '

                # Extract quantity, handle missing data and set to empty string
                quantity = response.xpath('//div[@data-testid="pdp-product-qty"]/p/text()').extract_first()
                quantity = quantity.replace('1 Pack (', '').replace('Tea Bags)', "").replace(' g', "").replace("piece",
                                                                                                               '').strip() if quantity else ' '

                # Calculate unit price, handle missing quantity and price
                if quantity != ' ' and brand_selling_price != ' ':
                    brand_unit_price = round(int(brand_selling_price) / int(quantity), 2)
                else:
                    brand_unit_price = ' '

                # Populating item fields, insert empty string for missing values
                item['platform'] = 'Zepto'
                item['date'] = datetime.now().strftime('%d-%m-%Y')
                item['pincode'] = response.meta.get('zipcode', ' ')
                item['city'] = response.meta.get('city', ' ')
                item['competitor_brand_name'] = response.meta.get('Brand', ' ')

                # Extract competitor SKU name, set to empty string if not found
                item['competitor_sku_name'] = response.xpath("//h1/text()").get() or ' '

                item['competitor_sku'] = response.meta.get('prod_id', ' ')
                item['competitor_url'] = response.meta.get('Brand_Url', ' ')

                # Determine if product is in stock, set to 0 if SKU name is empty
                item['comp_instock'] = 0 if 'Out of Stock' in response.text or item['competitor_sku_name'] == ' ' else 1

                item['competitor_mrp'] = brand_mrp
                item['competitor_selling_price'] = brand_selling_price
                item['competitor_unit_price'] = brand_unit_price
                item['competitor_discount'] = brand_discount
                item['competitor_discount_amount'] = brand_discount_amount
                item['unique_id'] = response.meta.get('unique_id', ' ')

                yield item


            except Exception as e:
                self.logger.error(f"Error processing response: {e}")


if __name__ == '__main__':
    execute('scrapy crawl zepto_Competitor'.split())

