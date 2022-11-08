from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

driver = webdriver.Chrome(ChromeDriverManager().install())
wait = WebDriverWait(driver, 20)
driver.get("https://www.pricechopper.com/stores/vt/brattleboro/market-32-136.html")
driver.maximize_window()
wait.until(EC.visibility_of_element_located((By.XPATH, '//i[@class="eci dept-bakery"]'))).click()
print(driver.page_source)
time.sleep(10)
#print(driver.page_source)
# f = open("pchop.html", 'w')
# soup = BeautifulSoup(driver.page_source, 'html.parser')
# bake = driver.find_element_by_xpath('')
# driver.execute_scripts("arguments[0].click()", bake)
# f.write(soup.prettify())
# f.close()
# drop_down = driver.find_element(by=By.LINK_TEXT, value="Full Calendar")
# drop_down.click()
# print(driver.title)
# #driver.close()