import scrapy
import csv 
from datetime import datetime 

class WorldPopulationSpider(scrapy.Spider):
    name = 'world_population'
    start_urls = ['https://www.worldometers.info/world-population/']
    
    def parse(self,response):
        table = response.css('table#example2')
        headers = table.css('thead th::text').getall()
        rows = table.css('tbody tr')
        
        filename = f'World population_{datetime.now().strftime("%Y%m%d_%H%M%S")}. csv'
        
        with open(filename,'w', newline='',encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([header.strip() for header in headers if header.strip()])
            
            for row in rows:
                data = row.css('td::text').getall()
                writer.writerow([item.strip() for item in data])
                
        self.log(f"Archivo guardado en: {filename}")