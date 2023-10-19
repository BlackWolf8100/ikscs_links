import time
import requests
from bs4 import BeautifulSoup
from my_base import My_base
from datetime import datetime, timedelta

WORK_TIME_SEC = 300
BASE = "ikscs.in.ua"

def main():


    db = My_base()
    db.open()
    sql = 'SELECT `link` FROM parse_ikscs_links WHERE `status` = "COMPLETE" LIMIT 1'
    db.cursor.execute(sql)
    urls = [e[0] for e in db.cursor.fetchall()]

    if urls:
        sql = 'DELETE FROM `parse_ikscs_links`'
        db.cursor.execute(sql)
        sql = 'DELETE FROM `parse_ikscs_image`'
        db.cursor.execute(sql)
        sql = f'INSERT INTO parse_ikscs_links (`link`) VALUES ("https://{BASE}")'
        db.cursor.execute(sql)
        db.mydb.commit()
    
    sql = 'SELECT EXISTS (SELECT 1 FROM `parse_ikscs_links`)'
    db.cursor.execute(sql)
    is_table_has_data = [e[0] for e in db.cursor.fetchall()]
    if not is_table_has_data[0]:
        sql = f'INSERT INTO parse_ikscs_links (`link`) VALUES ("https://{BASE}")'
        db.cursor.execute(sql)
        db.mydb.commit()


    count = 0 
    urls = [7]
    start_time = datetime.now()
    print(start_time)
    while (len(urls) > 0) and datetime.now() - start_time < timedelta(seconds = WORK_TIME_SEC):
        print('Records:', len(urls), count)
        time.sleep(0.5)
        count += 1
        sql = "SELECT `link` FROM parse_ikscs_links WHERE `status` IS NULL"
       
        db.cursor.execute(sql)
        urls = [e[0] for e in db.cursor.fetchall()]                    
    
        for url in urls:
            link_for_save_set, data_uniq, status_code, tags = process_one_page(url)
            sql = f'UPDATE parse_ikscs_links SET status_code = {status_code} WHERE link = "{url}"'
            db.cursor.execute(sql)
            
            data = []
            for k, v in data_uniq.items():
                data.append([*k, v])
        
            if link_for_save_set:
                sql = f'INSERT IGNORE INTO parse_ikscs_links (link, status, referer) VALUES (%s, %s, "{url}")'
                values = [(e, None) for e in link_for_save_set]
                db.cursor.executemany(sql, values)
    
            
            sql = 'UPDATE parse_ikscs_links SET status="READY", title=%s, description=%s, lang=%s WHERE link = %s'
            db.cursor.execute(sql, (tags['title'], tags['description'], tags['lang'], url))
            db.mydb.commit()


            
            sql = 'INSERT IGNORE INTO parse_ikscs_image (src, title, alt, a_href, refer) VALUES (%s, %s, %s, %s, %s)'
            values = []
            for row in data:
                values.append((*row, url))
            db.cursor.executemany(sql, values)               
            db.mydb.commit()
            if datetime.now() - start_time > timedelta(seconds = WORK_TIME_SEC):
                break
    if (len(urls) == 0):
        sql = f'UPDATE parse_ikscs_links SET status="COMPLETE" WHERE link = "https://{BASE}"' 
        db.cursor.execute(sql)
        db.mydb.commit()
    db.close()
    
    
    
def process_one_page(url):
        
    headers = {
        'User-Agent' : 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36'
    }

    responce = requests.get(url, headers = headers, allow_redirects = False)

    page = responce.text
    status_code = responce.status_code
    
    
    soup = BeautifulSoup(page, "html.parser")
    
    tags = {'title': None, 'description': None, 'lang': None}

    title_element = soup.title
    if title_element is not None:
        tags['title'] = str(title_element.string)
    # else:
    #     tags['title'] = "Заголовок не знайдено" 

    description_element = soup.find("meta", attrs={"name": "description"})
    if description_element:
        description = description_element.get("content")
        tags['description'] = description
    # else:
    #     description = "Опис не знайдено"
    # tags['description'] = description


    html_element = soup.find("html")
    if html_element:
        lang = html_element.get("lang")
        tags['lang'] = lang
    # else:
    #     lang = "Мова не вказана"
    # tags['lang'] = lang


    links = soup.find_all('img')
    print(len(links), url)

    uniq_data = dict()
    data = []
    for link in links:
        row = dict()
        src = link.get('src', link.get('data-src'))
        alt = link.get('alt')
        title = link.get('title')
        row['src'] = src
        row['alt'] = alt
        row['title'] = title
        data.append(row)
        uniq_data[(src, title, alt)] = None

    links = soup.find_all('a', href = True)
    link_for_save_set = set()
    link_for_save_list = []
    for link in links:
        l = link['href']
        print(l)
        link2 = soup.find('img')
        if link2:
            src = link2.get('src', link2.get('data-src'))
            alt = link2.get('alt')
            title = link2.get('title')
            uniq_data[(src, title, alt)] = l
        if l.startswith('tel:') or l.startswith('#') or l.startswith('mailto:'):
            continue
        if not (l.startswith('/') or l.startswith('.') or l.startswith('http://'+BASE) or l.startswith('https://'+BASE)):
            continue
        l = 'https://'+ BASE + l
        l = l.split('#')[0]
        link_for_save_set.add(l)
        link_for_save_list.append(l)
        
    return link_for_save_set, uniq_data, status_code, tags

if __name__ == '__main__':
    main()