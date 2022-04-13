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
        self.startdate = datetime(2022, 3, 1).date()
        self.enddate = datetime(2022, 3, 7).date()

        if not os.path.exists('data'):
            os.mkdir('data')

    def start_requests(self):
        urls = ['http://dadosabertos.c3sl.ufpr.br/curitibaurbs/']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        selector: scrapy.Selector
        for selector in tqdm(response.xpath('//td/a/text()')):
            link_text = selector.get()
            url = f'http://dadosabertos.c3sl.ufpr.br/curitibaurbs/{link_text}'

            date = re.match(r'\d{4}_\d{2}_\d{2}', link_text)
            if date:
                date = date.group(0)
                date = datetime.strptime(date, '%Y_%m_%d').date()
                if self.startdate <= date <= self.enddate:
                    archive_path = self.download_archive(url, link_text)

                    self.extract_archive(archive_path, link_text)


    def download_archive(self, url: str, link_text: str):
        """Faz o download do arquivo compactado na pasta de dados, caso o
        arquivo ainda não exista nessa pasta.
        
        Parameters
        ----------
        url : string
            A url onde o crawler vai buscar os dados.
        link_text : string
            O texto contido no link encontrado para o arquivo compactado.
        """
        archive_path = f'data/{link_text}'

        if not os.path.exists(archive_path) and re.match(r'.*.json.*', link_text):
            wget.download(url, archive_path)

        return archive_path


    def extract_archive(self, archive_path: str, link_text: str):
        """Descompacta um arquivo.
        
        Parameters
        ----------
        archive_path : string
            O caminho do arquivo compactado que se deseja descompactar.
        link_text : string
            O texto contido no link encontrado para o arquivo compactado.
        
        Raises
        ------
        Exception
            Quando o tipo do arquivo compactado não é suportado.
        """
        out_path =f'out/{link_text.split(".")[0]}'

        if not os.path.exists(out_path):
            # Descompactando arquivo tar
            if re.match(r'\d{4}_\d{2}_\d{2}_.*\.json.tar.gz', link_text):
                shutil.unpack_archive(archive_path, out_path)

            # Descompactando arquivo xz
            elif re.match(r'\d{4}_\d{2}_\d{2}_.*\.json.xz', link_text):
                if not os.path.exists(out_path):
                    os.mkdir(out_path)
                filename = re.match(r'\d{4}_\d{2}_\d{2}_(\w+)\.json.xz', link_text).group(1)
                with open(f'{out_path}/{filename}.json', 'w') as jout:
                    json_str = lzma.open(archive_path).read().decode()
                    jout.write(json_str)
            
            else:
                raise Exception('O tipo do arquivo não é suportado')

