from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import os

def download_dataset():
  chrome_options = Options()
  prefs = {
      "download.default_directory": os.path.join(os.getcwd(), "datasets"),
      "download.prompt_for_download": False,
      "download.directory_upgrade": True,
      "safebrowsing.enabled": True
  }
  chrome_options.add_experimental_option("prefs",prefs)
  
  driver = webdriver.Chrome(options=chrome_options)
  
  try:
    driver.get("https://www.datos.gov.co")
    
    search_box = WebDriverWait(driver,10).until(
      EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='search']"))
    ) 
    
    search_term = "educacion"
    search_box.send_keys(search_term)
    search_box.send_keys(Keys.RETURN)
    
    time.sleep(3)
    
    first_result = WebDriverWait(driver,10).until(
      EC.element_to_be_clickable((By.CSS_SELECTOR,".dataset-heading a"))
    )
    first_result.click()
    
    time.sleep(3)
    
    download_button = WebDriverWait(driver,10).until(
      EC.element_to_be_clickable((By.XPATH,"//a[contains(text(),'CSV)]"))
    )
    
    download_button.click()
    
    time.sleep(10)
    print("Descarga completada exitosamente")
    
  finally:
    driver.quit()
    
if not os.path.exists("datasets"):
  os.makedirs("datasets")
  
download_dataset()
    