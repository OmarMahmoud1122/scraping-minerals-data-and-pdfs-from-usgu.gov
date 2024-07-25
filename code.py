import pandas as pd
from bs4 import BeautifulSoup
import requests
import time
import io
from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError
from multiprocessing import Pool


        
def get_pdfs(l):   
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "DNT": "1",
    "Connection": "close",
    "Upgrade-Insecure-Requests": "1"
    }
    f = []     
    pdfs = []
    for i in l:
        hh = requests.get(i,headers=headers)
        soup1 = BeautifulSoup(hh.text,'html.parser')
        
        z = soup1.find('div',{'class': 'field-text-with-media'}).p
        if z is None:
            f.append('No description')
        else:
            f.append(z.text)
        p = soup1.find_all('a')
        ab = [i.get('href') for i in p if i.get('href') is not None]
        pdfs.append(ab)
            
    ppp = []
        
    
    for i in pdfs:
        if(len(i) > 0):
            mineral = []
            for j in i:
                if ('.pdf' in j) and ('http' in j):
                    try:
                        res = requests.get(j)
                    except:
                        continue
                    con = io.BytesIO(res.content)
                    try:
                        text = PdfReader(con)
                    except PdfReadError:
                        continue
                    all_pages = []
                    for page in text.pages:
                        only_one = page.extract_text()
                        all_pages.append(only_one)
                    full_pdf = '\n'.join(all_pages)
                    mineral.append(full_pdf)
            ppp.append(mineral)
    return ppp
                    
if __name__ == '__main__':
    
    data = requests.get('https://www.usgs.gov/centers/national-minerals-information-center/commodity-statistics-and-information')
    soup = BeautifulSoup(data.text,'html.parser')
    x = soup.find('div', attrs = {'class': 'field-text-with-media'}).find_all('ul')[1:]
    m = []
    d = []
    for ul in x:
        y = ul.find_all('li')
        single = []
        for li in y:
            m.append(li.find('a').text)
            single.append('https://www.usgs.gov' + li.a['href'])
        d.append(single)
        
    processes = Pool()  
    alls = processes.map(get_pdfs,d)  
    alls = [mineral for minerals in alls for mineral in minerals]

    df1 = pd.DataFrame({'Mineral':m})
    df2 = pd.DataFrame(alls)
    df2.columns = df2.columns + 1
    df = pd.concat([df1,df2],axis= 1)
    df.to_csv(#path to save csv file in)
    print(df)
