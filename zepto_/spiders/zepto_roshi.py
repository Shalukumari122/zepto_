import datetime
import hashlib
import json
import os
import random
import time
import uuid
from time import sleep

import pymysql
import pandas as pd
import scrapy
from curl_cffi import requests
from scrapy.cmdline import execute
from datetime import datetime,date
from zepto_.items import Zepto_roshi


class ZeptoCompetitorSpider(scrapy.Spider):
    name = "zepto_roshi"

    # custom_settings = {
    #     "DOWNLOAD_HANDLERS": {
    #         "http": "scrapy_impersonate.ImpersonateDownloadHandler",
    #         "https": "scrapy_impersonate.ImpersonateDownloadHandler",
    #     },
    #     "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    # }

    def __init__(self,start=0,end=0):
        try:
            self.conn = pymysql.Connect(host='localhost',
                                        user='root',
                                        password='actowiz',
                                        database='zepto_')
            self.cur = self.conn.cursor()
            self.start=start
            self.end=end

        except Exception as e:
            print(e)

    def start_requests(self):

        today_date = str(date.today()).replace('-', '_')

        query = f"SELECT * FROM zepto_links_roshi WHERE status ='pending' and  servical=1 and id BETWEEN {self.start} AND {self.end}"

        self.cur.execute(query)
        rows = self.cur.fetchall()

        folder_loc = f'C:/Shalu/PageSave/Zepto_weekly/roshi/{today_date}/'
        if not os.path.exists(folder_loc):
            os.makedirs(folder_loc,exist_ok=True)


        for row in rows:

            city = row[2]
            zipcode = row[1]
            area=row[0]
            storeid = row[3]
            longitude = row[6]
            latitude = row[4]
            brand_name = "Roshi"

            Roshi_Wellness_SKUs = row[7]
            Brand_Url = row[8]
            unique_id=row[9]

            prod_id = Brand_Url.split('/')
            prod_id = prod_id[-1]
            # unique_id = hashlib.sha256((area + city + brand_name +prod_id).encode()).hexdigest()
            # unique_id = hashlib.sha256((area + city + str(zipcode) + Roshi_Wellness_SKUs).encode()).hexdigest()

            # update_query = """
            #         UPDATE zepto_links_roshi
            #         SET unique_id = %s
            #         WHERE areas = %s AND City = %s AND pincode = %s AND sku_name=%s
            #         """
            # self.cur.execute(update_query, (unique_id, area, city,zipcode, Roshi_Wellness_SKUs))
            # self.conn.commit()
            # print("Row updated successfully.")
            main_loc = folder_loc + f"{unique_id}.html"

            if not os.path.isfile(main_loc):
                meta={}
                browsers = [
                    "chrome110",
                    "edge99",
                    "safari15_5"
                ]
                meta["impersonate"] = random.choice(browsers)


                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'accept-language': 'en-US,en;q=0.9',
                    'app_sub_platform': 'WEB',
                    'appversion': '12.1.0',
                    'cache-control': 'no-cache',
                    'compatible_components': 'CONVENIENCE_FEE,RAIN_FEE,EXTERNAL_COUPONS,STANDSTILL,BUNDLE,MULTI_SELLER_ENABLED,PIP_V1,NEW_FEE_STRUCTURE,NEW_BILL_INFO,RE_PROMISE_ETA_ORDER_SCREEN_ENABLED,SUPERSTORE_V1,MANUALLY_APPLIED_DELIVERY_FEE_RECEIVABLE,MARKETPLACE_REPLACEMENT,ZEPTO_PASS,ZEPTO_PASS:1,ZEPTO_PASS:2,ZEPTO_PASS_RENEWAL,CART_REDESIGN_ENABLED,SUPERSTORE_V1',
                    'deviceid': '1652933523485267',
                    'origin': 'https://www.zeptonow.com',
                    'platform': 'WEB',
                    'pragma': 'no-cache',
                    'priority': 'u=1, i',
                    # 'refe/rer': 'https://www.zeptonow.com/',
                    'requestid': '5037284344502924',
                    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'cross-site',
                    'sessionid': '7335335879449383',
                    # 'store_etas': '{"34420157-825b-4822-acc8-9f20ae822fe3":12}',
                    # 'store_ids': '34420157-825b-4822-acc8-9f20ae822fe3',
                    'storeid': f'{storeid}',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
                }

                time.sleep(1)

                r4 = requests.get(f'https://api.zepto.co.in/api/v1/inventory/store/{storeid}/detailed-info',
                                  headers=headers).json()
                name = r4['name']
                city_all = r4['city']
                sellerInfo = r4['sellerInfo']
                cityId=r4['cityId']
                openTime=r4['openTime']
                closeTime=r4['closeTime']
                id=r4['id']

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
                    'store-info':json.dumps(store_info),
                    '_ga_52LKG2B3L1': 'GS1.1.1722952192.40.1.1722952446.60.0.1265190564',
                }

                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'no-cache',
                    # 'cookie': 'unique_browser_id=4595536922659838; _gcl_au=1.1.760092518.1721989156; _fbp=fb.1.1721989156461.627741631865033537; _ga=GA1.1.631977533.1721989157; maxWeightLimitCart=25000; csrfSecret=EDb1KN0AGiNrD3wSdb0hICnA; mp_dcc8757645c1c32f4481b555710c7039_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A19107c7b48aef8-042e5ed5d9db76-26001e51-e1000-19107c7b48bef9%22%2C%22%24device_id%22%3A%20%2219107c7b48aef8-042e5ed5d9db76-26001e51-e1000-19107c7b48bef9%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; XSRF-TOKEN=lubVmQA6-3B8jYtlEsL3jfH3PynFZH_Sohf4.PDskDHfMdPHQf5%2FBQQHev%2B8nlWRzgf8D00qr7Ww25Cc; _ga_37QQVCR1ZS=GS1.1.1722952192.12.1.1722952445.60.0.0; latitude=19.1656633; longitude=73.07524769999999; store-info=%7B%22storeServiceableResponseV2%22%3A%5B%5D%2C%22storeServiceableResponse%22%3A%7B%22serviceable%22%3Afalse%2C%22storeId%22%3Anull%2C%22userId%22%3Anull%2C%22etaServiceability%22%3Anull%2C%22distance%22%3Anull%2C%22storeLatitude%22%3Anull%2C%22storeLongitude%22%3Anull%2C%22secondaryStoreIds%22%3Anull%7D%2C%22primaryStoreInfo%22%3A%7B%22name%22%3A%22BLR-JALAHALLI%22%2C%22isOnline%22%3Atrue%2C%22openTime%22%3A%2206%3A00%3A00.366115%22%2C%22closeTime%22%3A%2202%3A00%3A00.366127%22%2C%22id%22%3A%229e88e823-8755-42c9-a8f4-6b17499952a3%22%2C%22cityId%22%3A%228ed26cb7-eb7d-4b7b-8d8c-3e93d5855bdd%22%2C%22city%22%3A%7B%22name%22%3A%22Bengaluru%22%2C%22state%22%3A%22Karnataka%22%2C%22country%22%3A%22India%22%7D%2C%22sellerInfo%22%3A%7B%22name%22%3A%22Geddit%20Convenience%20Private%20Limited%22%2C%22address%22%3A%22Ground%20Floor%2C%20Rahat%20Manzil%20Building%2C%20Dr%20Ambedkar%20Road%2C%20Khar%20West%20Mumbai%2C%20Greater%20Mumbai%2C%20Maharashtra-400052%22%2C%22fssaiNo%22%3A11521998000248%2C%22showSellerInfo%22%3Afalse%2C%22juspayMerchantId%22%3A%22geddit%22%2C%22juspayAndroidClientId%22%3A%22geddit_android%22%2C%22juspayIosClientId%22%3A%22geddit_ios%22%7D%2C%22isOtofEnabled%22%3Afalse%2C%22noBagDeliveryEnabled%22%3Atrue%2C%22isNoBagDeliveryNew%22%3Atrue%2C%22noBagDeliveryDefaultOptStatus%22%3Atrue%2C%22standStillMode%22%3Afalse%2C%22raining%22%3Atrue%2C%22isFullNightDeliveryEnabled%22%3Afalse%2C%22issueAtStore%22%3Afalse%2C%22takingOrders%22%3Atrue%2C%22cartV3Enabled%22%3Atrue%2C%22cartV2Enabled%22%3Afalse%7D%2C%22etaServiceableInfo%22%3A%5B%7B%22storeId%22%3A%229e88e823-8755-42c9-a8f4-6b17499952a3%22%2C%22etaInMinutes%22%3A%2214%22%2C%22deliverableType%22%3A%22OPEN%22%2C%22deliverableSubtype%22%3A%22ETA_RAIN%22%2C%22isDeliverable%22%3Atrue%7D%5D%7D; _ga_52LKG2B3L1=GS1.1.1722952192.40.1.1722952446.60.0.1265190564',
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


                sleep(2)
                response2 = requests.request('GET', url=f'{Brand_Url}',
                                             cookies=cookies,timeout=60,
                                             headers=headers,impersonate=random.choice(browsers)
                                             )
                if response2.status_code == 429:
                    retry_after = int(response2.headers.get('Retry-After', 60))  # default to 60 seconds
                    time.sleep(retry_after)

                if response2.status_code == 200:
                    with open(main_loc, 'wb') as file:
                        file.write(response2.text.encode('utf-8'))

                yield scrapy.Request(url="file://" + main_loc, callback=self.parse, meta = {"zipcode": zipcode,
                        "city": city,
                        "Brand": brand_name,"Roshi_Wellness_SKUs":Roshi_Wellness_SKUs,
                        "unique_id":unique_id,
                        "Brand_Url": Brand_Url,
                        "prod_id": prod_id},dont_filter=True)

            else:

                yield scrapy.Request(url="file://" + main_loc, callback=self.parse,meta = {"zipcode": zipcode,
                        "city": city,
                        "Roshi_Wellness_SKUs": Roshi_Wellness_SKUs,
                        "unique_id": unique_id,
                        "Brand": brand_name,
                        "Brand_Url": Brand_Url,
                        "prod_id": prod_id},
                        dont_filter=True)

    def parse(self, response):
        item = Zepto_roshi()

        data = response.xpath('//div[@class="flex items-center"]')

        # Extract and handle missing values with ' '
        brand_selling_price = data.xpath('./h4/text()').extract_first()
        brand_selling_price = brand_selling_price.replace('₹', "") if brand_selling_price else ' '

        brand_mrp = data.xpath('./p/text()').extract_first()
        brand_mrp = brand_mrp.replace('₹', "") if brand_mrp else ' '

        brand_discount = data.xpath('./div/text()').extract_first()
        brand_discount = brand_discount.replace('Off', "") if brand_discount else ' '

        # Calculate discount amount, set to ' ' if prices are not available
        if brand_mrp != ' ' and brand_selling_price != ' ':
            brand_discount_amount = int(brand_mrp) - int(brand_selling_price)
        else:
            brand_discount_amount = ' '

        # Extract and clean quantity, set to ' ' if missing
        quantity = response.xpath('//div[@data-testid="pdp-product-qty"]/p/text()').extract_first()
        quantity = (
            quantity.replace('1 Pack (', '')
            .replace('Tea Bags)', '')
            .replace(' g', '')
            .strip()
            if quantity else ' '
        )

        # Calculate unit price, set to ' ' if either quantity or selling price is missing
        if quantity != ' ' and brand_selling_price != ' ':
            brand_unit_price = round(int(brand_selling_price) / int(quantity), 2)
        else:
            brand_unit_price = ' '

        # Populate item fields, inserting ' ' for missing values
        item['platform'] = "Zepto"
        item['date'] = datetime.now().strftime('%d-%m-%Y')
        item['pincode'] = response.meta.get('zipcode', ' ')
        item['city'] = response.meta.get('city', ' ')
        item['Brand'] = response.meta.get('Brand', ' ')
        item['brand_sku_name'] = response.meta.get('Roshi_Wellness_SKUs', ' ')

        # Determine stock status, default to 1 (in stock) if value is missing
        stock = response.xpath('//div[@class="ml-4"]/div/text()').extract_first()
        item['instock'] = 0 if stock else 1

        item['brand_sku'] = response.meta.get('prod_id', ' ')
        item['Brand_Url'] = response.meta.get('Brand_Url', ' ')
        item['brand_mrp'] = brand_mrp
        item['brand_selling_price'] = brand_selling_price
        item['brand_unit_price'] = brand_unit_price
        item['brand_discount'] = brand_discount
        item['brand_discount_amount'] = brand_discount_amount
        item['unique_id'] = response.meta.get('unique_id', ' ')

        yield item


if __name__ == '__main__':
    execute('scrapy crawl zepto_roshi  -a start=0 -a end=1000'.split())

