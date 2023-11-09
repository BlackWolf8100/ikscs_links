import time
import requests
from bs4 import BeautifulSoup
from my_base import My_base
from my_loger import My_loger
from datetime import datetime, timedelta
import sys

WORK_TIME_SEC = 12000
SLEEP_TIME_SEC = 0.2
LOG_FILE = 'c:\\API\Mykola\ikscs_links\parse.log'


def format_time(time_duration):
    hours, remainder = divmod(time_duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours}:{minutes}:{seconds}'


def main(BASE, loger):
    
    db = My_base(dbms = 'mysql', logger = loger)

    if not db.open():
        print('Помилка роботи з базою даних!')
        loger.log('Помилка роботи з базою даних!')
        return 
    
    start_procedure(db)
    

    count = 0 
    urls = [7]
    while (len(urls) > 0) and datetime.now() - start_time < timedelta(seconds = WORK_TIME_SEC):
        print('Records:', len(urls), count)
        loger.log(f'Records: {len(urls)} {count}')
        time.sleep(SLEEP_TIME_SEC)
        count += 1
        
        sql = f'SELECT `link` FROM parse WHERE `status` IS NULL AND `domain` = "{BASE}"'
        urls = db.get_one_table(sql)                   
    
        for url in urls:
            uniq_data, status_code, tags, h_list, a_text_list, link_for_save_list, external_links  = process_one_page(url)
            

        
            if link_for_save_list:
                sql = f'INSERT IGNORE INTO parse (link, referer, domain) VALUES (%s, %s, %s)'
                values = []
                for link in set(link_for_save_list):
                    has_stop_words = False
                    for word in ('/add?', '?start='):
                        has_stop_words = has_stop_words or (word in link)
                    if not has_stop_words:
                        values.append((link, url, BASE))
                db.executemany(sql, values)

                sql = f'INSERT IGNORE INTO parse_a (domain, src, href, anchor) VALUES (%s, %s, %s, %s)'
                values = []
                for link, a_text in zip(link_for_save_list, a_text_list):
                    has_stop_words = False
                    for word in ('/shop-basket/', '/store/', '/manufacturers/manufacturer/', '/sklad-cart/'):
                        has_stop_words = has_stop_words or (word in link)
                    if not has_stop_words:   
                        values.append((BASE, url, link, a_text))
                db.cursor.executemany(sql, values)

            if external_links:
                sql = f'INSERT IGNORE INTO parse (domain, link, referer) VALUES (%s, %s, "{url}")'
                values = [(BASE, external_link) for external_link in external_links]
                db.cursor.executemany(sql, values)
                db.mydb.commit()


            
            sql = 'UPDATE parse SET status="READY", status_code=%s, title=%s, description=%s, lang=%s WHERE link = %s'
            db.cursor.execute(sql, (status_code, tags['title'], tags['description'], tags['lang'], url))
            db.mydb.commit()
            
            sql = f'INSERT IGNORE INTO parse_h (domain, src, level, anchor) VALUES ("{BASE}", "{url}", %s, %s)'
            db.cursor.executemany(sql, h_list)
            db.mydb.commit()

            
            sql = f'INSERT IGNORE INTO parse_img (src, title, alt, domain) VALUES (%s, %s, %s, "{BASE}")'
            db.cursor.executemany(sql, uniq_data)               
            db.mydb.commit()
            if datetime.now() - start_time > timedelta(seconds = WORK_TIME_SEC):
                break
    
    
    if (len(urls) == 0):
        sql = f'UPDATE parse SET status="COMPLETE" WHERE link = "https://{BASE}"' 
        db.cursor.execute(sql)
        db.mydb.commit()
    db.close()


    
def process_one_page(url):
    tags = {'title': None, 'description': None, 'lang': None}

    if not (url.startswith('http://'+BASE) or url.startswith('https://'+BASE)):
        return set(), 0, tags, [], [], [], set()

    headers = {
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    responce = requests.get(url, headers = headers, allow_redirects = False)
    contentType = responce.headers['content-type']
    
    
    status_code = responce.status_code
    if not contentType.startswith('text/html;'):
        return set(), status_code, tags, [], [], [], set()

    page = responce.text
    soup = BeautifulSoup(page, "html.parser")

    # Збираємо всі посилання 
    a_text_list = []
    links = soup.find_all('a', href=True)
    for link in links:
        l = link['href']
        a_text = link.get_text(strip=True)
        a_text_list.append(a_text)

    # Збираємо заголовки
    h_list = []
    headings = soup.find_all(["h1", "h2", "h3"])
    for heading in headings:
        tag_name = heading.name
        level = None
        text = heading.get_text()
        if tag_name and tag_name.startswith("h") and tag_name[1:].isdigit():
            level = int(tag_name[1:])
        else:
            level = 0
        h_list.append([level, text])
            
    

    title_element = soup.title
    if title_element is not None:
        tags['title'] = str(title_element.string)

    description_element = soup.find("meta", attrs={"name": "description"})
    tags['description'] = description_element.get("content") if description_element else None

    html_element = soup.find("html")
    tags['lang'] = html_element.get("lang") if html_element else None

    # Збираємо посилання на картинки
    uniq_data = set()
    links = soup.find_all('img')
    print(len(links), url)
    loger.log(f'{len(links), url}')
    for link in links:
        src = link.get('src', link.get('data-src'))
        alt = link.get('alt')
        title = link.get('title')
        uniq_data.add((src, title, alt))
        
        
    link_for_save_list = []
    external_links = set()
    links = soup.find_all('a', href = True)
    for link in links:
        l = link['href']
        link2 = soup.find('img')
        if l.startswith('http://') or l.startswith('https://') and not l.startswith('http://'+BASE) and not l.startswith('https://'+BASE):
            external_links.add(l)
        if link2:
            src = link2.get('src', link2.get('data-src'))
            alt = link2.get('alt')
            title = link2.get('title')
        print(l)
        if l.startswith('tel:') or l.startswith('#') or l.startswith('mailto:'):
            continue
        if not (l.startswith('/') or l.startswith('.')):
            continue
        if not (l.startswith('http://') or l.startswith('https://')):
            l = 'https://'+ BASE + l
        l = l.split('#')[0]
        link_for_save_list.append(l)
        
    return uniq_data, status_code, tags, h_list, a_text_list, link_for_save_list, external_links

def start_procedure(db):
    sql = f'SELECT `link` FROM parse WHERE `status` = "COMPLETE" AND `domain` = "{BASE}" LIMIT 1'
    urls = db.get_one_table(sql)
    
    if urls:
        sql = f'DELETE FROM `parse` WHERE `domain` = "{BASE}"'
        db.cursor.execute(sql)
        sql = f'DELETE FROM `parse_img` WHERE `domain` = "{BASE}"'
        db.cursor.execute(sql)
        sql = f'INSERT INTO parse (`link`, `domain`) VALUES ("https://{BASE}", "{BASE}")'
        db.cursor.execute(sql)
        db.mydb.commit()
    
    sql = f'SELECT EXISTS (SELECT 1 FROM `parse` WHERE `domain` = "{BASE}")'
    is_table_has_data = db.get_one_table(sql)
    
    if not is_table_has_data[0]:
        sql = f'INSERT INTO parse (`link`, `domain`) VALUES ("https://{BASE}", "{BASE}")'
        db.cursor.execute(sql)
        db.mydb.commit()
        
if __name__ == '__main__':
    start_time = datetime.now()
    if len(sys.argv) == 1:
        BASE = 'ingener.in.ua'
        # BASE = 'ikscs.in.ua'
    elif len(sys.argv) == 2:
        BASE = sys.argv[1]
    else:
        print(f'Usage: {sys.argv[0]} BASE')
        quit(1)
    loger = My_loger(LOG_FILE)
    loger.log(f'domain = {BASE}\nparsin starting')
    main(BASE, loger)
    
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    loger.log(f'domain = {BASE}\nparsin end\nЗагальний час роботи: {format_time(elapsed_time)}\n\n')