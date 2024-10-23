import pandas as pd
import requests

# Read your Excel file
excel_file = r'C:\Shalu\LiveProjects\zepto_\input_files\zepto_input_roshi.xlsx'
df = pd.read_excel(excel_file,)  # Adjust sheet name as necessary

# Iterate through each row in the DataFrame
for index, row in df.iterrows():
    zipcode = row['pincode']
    city = row['city']
    # area = row['Areas']

    try:
        r1 = requests.get(
            f'https://api.zepto.co.in/api/v1/maps/place/autocomplete/?place_name={zipcode},+{city}').json()

        for prediction in r1['predictions']:
            place_id = prediction['place_id']
            r2 = requests.get(f'https://api.zepto.co.in/api/v1/maps/place/details/?place_id={place_id}').json()
            latitude = r2['result']['geometry']['location']['lat']
            longitude = r2['result']['geometry']['location']['lng']

            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                'appversion': '10.9.13-WEB',
                'compatible_components': 'CONVENIENCE_FEE,RAIN_FEE,EXTERNAL_COUPONS,STANDSTILL,BUNDLE,MULTI_SELLER_ENABLED,PIP_V1,ROLLUPS,SCHEDULED_DELIVERY,SAMPLING_ENABLED,ETA_NORMAL_WITH_149_DELIVERY,HOMEPAGE_V2,NEW_ETA_BANNER,VERTICAL_FEED_PRODUCT_GRID,AUTOSUGGESTION_PAGE_ENABLED,AUTOSUGGESTION_PIP,AUTOSUGGESTION_AD_PIP,BOTTOM_NAV_FULL_ICON,NEW_FEE_STRUCTURE,NEW_BILL_INFO',
                'deviceid': '3023813566925089',
                'origin': 'https://www.zeptonow.com',
                'platform': 'WEB',
                'priority': 'u=1, i',
                'referer': 'https://www.zeptonow.com/',
                'sessionid': '6974254890094455',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            }

            params = {
                'latitude': str(latitude),
                'longitude': str(longitude),
                'page_type': 'HOME',
                'version': 'v2',
                'show_new_eta_banner': 'true',
            }

            r3 = requests.get('https://api.zepto.co.in/api/v1/config/layout/', params=params, headers=headers).json()
            store_id = r3['storeServiceableResponse']['storeId']

            if store_id:
                df.at[index, 'latitude'] = latitude
                df.at[index, 'longitude'] = longitude
                df.at[index, 'storeid'] = store_id
                print(f"{city}|{zipcode}|{store_id}|{latitude}|{longitude}")
                break
        else:
            print(f"No store found for {city} | {zipcode}")

    except Exception as e:
        print(f"Error: {e} | {city} | {zipcode}")

# Save the updated DataFrame back to Excel
df.to_excel(excel_file, index=False)
