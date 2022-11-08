from bs4 import BeautifulSoup
import requests
import json
import html

def return_site(address : str):
    return requests.get(address)

def print_site(address : str):
    print(return_site(address))

def save_site(address : str, filename : str):
    contents = return_site(address)
    f = open(filename, 'w')
    soup = BeautifulSoup(contents.text, 'html.parser')
    f.write(soup.prettify())
    f.close()

data = requests.get("https://shop.pricechopper.com/shop/categories/16").json()
print(data)

#save_site("https://shop.pricechopper.com/shop/categories/16", "pchop.html")

# with open("deli.html", 'rb') as fp:
#     soup = BeautifulSoup(fp, 'html.parser')
    
#     all_products = soup.find_all("div", {"class":"productPriceInfoWrap"})
    
#     table = []
#     for p in all_products:
#         item_features = []
#         all_sizes = p.find_all("span", {"class":"overline text-truncate"})
#         all_names = p.find_all("span", {"class":"real-product-name"})
#         all_prices = p.find_all("span", {"class":"price item-unit-price"})
#         all_unit_price = p.find_all("p", {"class":"unitPriceDisplay"})
#         item_features.append(all_sizes[0].text.strip() if len(all_sizes)>0 else "")
#         item_features.append(all_names[0].text.strip() if len(all_names)>0 else "")
#         if len(all_prices) > 0:
#             #sale price
#             item_features.append("")
#             item_features.append(all_prices[0].text.strip().replace("\r", "").replace("\n", "").replace(" ", ""))
#         else:
            
#             normal_price = p.find_all("span", {"class":"price item-unit-price strike-price"})
#             sale_price = p.find_all("span", {"class":"salePrice item-unit-price"})
#             #print("sale", all_prices, all_names[0].text.strip(), p)
#             item_features.append(sale_price[0].text.strip().replace("\r", "").replace("\n", "").replace(" ", "") if len(sale_price)>0 else "")
#             item_features.append(normal_price[0].text.strip().replace("\r", "").replace("\n", "").replace(" ", "") if len(normal_price)>0 else "")
#         item_features.append(all_unit_price[0].text.strip() if len(all_unit_price)>0 else "")
#         table.append(item_features)
#     for t in table:
#         print(t)