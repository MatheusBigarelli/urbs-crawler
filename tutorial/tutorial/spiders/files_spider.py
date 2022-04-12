from datetime import datetime
import json
import os
import re
import shutil
import scrapy
import lzma

import wget

from tqdm import tqdm

class FilesSpider(scrapy.Spider):
    name = "files"

    def __init__(self) -> None:
        super().__init__()
        self.startdate = datetime(2022, 4, 1).date()
        self.enddate = datetime(2022, 4, 7).date()

    def start_requests(self):
        urls = ['http://dadosabertos.c3sl.ufpr.br/curitibaurbs/']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        selector: scrapy.Selector
        for selector in tqdm(response.xpath('//td/a/text()')):
            link_text = selector.get()
            url = f'http://dadosabertos.c3sl.ufpr.br/curitibaurbs/{link_text}'
            print('O link completo é', url)

            date = re.match(r'\d{4}_\d{2}_\d{2}', link_text)
            if date:
                date = date.group(0)
                date = datetime.strptime(date, '%Y_%m_%d').date()
                if self.startdate <= date <= :
                    print('Downloading', url)
                    archive_path = f'data/{link_text}'
                    if not os.path.exists(archive_path) and re.match(r'.*.json.*', link_text):
                            wget.download(url, archive_path)

                    out_path =f'out/{link_text.split(".")[0]}'

                    if not os.path.exists(out_path):
                        if re.match(r'\d{4}_\d{2}_\d{2}_.*\.json.tar.gz', link_text):
                            shutil.unpack_archive(archive_path, out_path)
                        elif re.match(r'\d{4}_\d{2}_\d{2}_.*\.json.xz', link_text):
                            if not os.path.exists(out_path):
                                os.mkdir(out_path)
                            filename = re.match(r'\d{4}_\d{2}_\d{2}_(\w+)\.json.xz', link_text).group(1)
                            with open(f'{out_path}/{filename}.json', 'w') as jout:
                                json_str = lzma.open(archive_path).read().decode()
                                jout.write(json_str)
                else:
                    print('Não fez nada')
                    print(date, type(date))
                    print()

