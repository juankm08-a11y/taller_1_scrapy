import os
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import datetime
import requests

def download_text_html(url,donwload_dir="datasets"):
  os.makedirs(donwload_dir,exist_ok=True)
  donwload_dir = os.path.abspath(donwload_dir)
  
  chrome_options = Options()
  chrome_options.add_argument("--no-sandbox")
  chrome_options.add_argument("--disable-dev-shm-usage")
  chrome_options.add_argument("--start-maximized")
  
  prefs = {
    "download.default_directory":donwload_dir,
    "download.prompt_for_download":False,
    "download.directory_upgrade":True,
    "safebrowsing.enabled":True,
    "profile.default_content_setting_values.automatic_downloads":1
  }
  chrome_options.add_experimental_option("prefs",prefs)
  
  service = Service(ChromeDriverManager().install())
  driver = webdriver.Chrome(service=service, options=chrome_options)
  wait = WebDriverWait(driver,15)
  
  try:
    print(f"Abriendo página del dataset: {url}")
    driver.get(url)
    
    print("Buscando el boton")
    download_button = wait.until(
      EC.element_to_be_clickable((By.XPATH,"//a[contains(@class, 'download-file-link') and contains(@class, 'all-caps')]"))
    )
    
    file_url = download_button.get_attribute("href")
    print(f"URL directa: {file_url}")
    
    filename = os.path.join(donwload_dir,"dataset.csv")
    print(f"Descargando archivo en: {filename}")
    
    response = requests.get(file_url)
    with open(filename,"wb") as f:
      f.write(response.content)
      
    print(f"Archivo descargado correctamente: {filename}")
      
  except Exception as e:
    print("Error durante el proceso:",str(e))
  finally:
    time.sleep(2)
    driver.quit()
    
if __name__ == "__main__":
  dataset_url = "https://www.datos.gov.co/dataset/carto2000lasmalvinas81794024/svry-w33a/about_data"
  download_text_html(dataset_url)
  