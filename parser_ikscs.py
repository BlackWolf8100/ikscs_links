import time
import requests
from bs4 import BeautifulSoup
from my_base import My_base
from datetime import datetime, timedelta
import sys

WORK_TIME_SEC = 300
SLEEP_TIME_SEC = 0.2
# BASE = "ikscs.in.ua"
# BASE = "ingener.in.ua"


def main(BASE):


    db = My_base()
    db.open()
    sql = f'SELECT `link` FROM parse WHERE `status` = "COMPLETE" AND `domain` = "{BASE}" LIMIT 1'
    db.cursor.execute(sql)
    urls = [e[0] for e in db.cursor.fetchall()]

    if urls:
        sql = f'DELETE FROM `parse` WHERE `domain` = "{BASE}"'
        db.cursor.execute(sql)
        sql = f'DELETE FROM `parse_img` WHERE `domain` = "{BASE}"'
        db.cursor.execute(sql)
        sql = f'INSERT INTO parse (`link`, `domain`) VALUES ("https://{BASE}", "{BASE}")'
        db.cursor.execute(sql)
        db.mydb.commit()
    
    sql = f'SELECT EXISTS (SELECT 1 FROM `parse` WHERE `domain` = "{BASE}")'
    db.cursor.execute(sql)
    is_table_has_data = [e[0] for e in db.cursor.fetchall()]
    if not is_table_has_data[0]:
        sql = f'INSERT INTO parse (`link`, `domain`) VALUES ("https://{BASE}", "{BASE}")'
        db.cursor.execute(sql)
        db.mydb.commit()


    count = 0 
    urls = [7]
    start_time = datetime.now()
    print(start_time)
    while (len(urls) > 0) and datetime.now() - start_time < timedelta(seconds = WORK_TIME_SEC):
        print('Records:', len(urls), count)
        time.sleep(SLEEP_TIME_SEC)
        count += 1
        sql = f'SELECT `link` FROM parse WHERE `status` IS NULL AND `domain` = "{BASE}"'
       
        db.cursor.execute(sql)
        urls = [e[0] for e in db.cursor.fetchall()]                    
    
        for url in urls:
            link_for_save_set, data_uniq, status_code, tags, h_list = process_one_page(url)
            sql = f'UPDATE parse SET status_code = {status_code} WHERE link = "{url}"'
            db.cursor.execute(sql)
            
            data = []
            for k, v in data_uniq.items():
                data.append([*k, v])
        
            if link_for_save_set:
                sql = f'INSERT IGNORE INTO parse (link, status, referer, domain) VALUES (%s, %s, "{url}", "{BASE}")'
                values = [(e, None) for e in link_for_save_set]
                db.cursor.executemany(sql, values)

                sql = f'INSERT IGNORE INTO parse_a (domain, src, href, anchor) VALUES ("{BASE}", "{url}", %s, %s)'
                values = [(e, 'to do') for e in link_for_save_set]
                db.cursor.executemany(sql, values)

#ancros = link.text
            
            sql = 'UPDATE parse SET status="READY", title=%s, description=%s, lang=%s WHERE link = %s'
            db.cursor.execute(sql, (tags['title'], tags['description'], tags['lang'], url))
            db.mydb.commit()
            
            sql = f'INSERT IGNORE INTO parse_h (domain, src, level, anchor) VALUES ("{BASE}", "{url}", %s, %s)'
            #values = [(row[0], row[1]) for row in processed_data]
            db.cursor.executemany(sql, h_list)
            db.mydb.commit()


            # sql = f'INSERT IGNORE INTO parse_img (src, title, alt, a_href, refer, domain) VALUES (%s, %s, %s, %s, %s, "{BASE}")'
            
            sql = f'INSERT IGNORE INTO parse_img (src, title, alt, domain) VALUES (%s, %s, %s, "{BASE}")'
            values = []
            for row in data:
                values.append((row[0], row[1], row[2]))
                # values.append((*row, url))
            db.cursor.executemany(sql, values)               
            db.mydb.commit()
            if datetime.now() - start_time > timedelta(seconds = WORK_TIME_SEC):
                break
    if (len(urls) == 0):
        sql = f'UPDATE parse SET status="COMPLETE" WHERE link = "https://{BASE}"' 
        db.cursor.execute(sql)
        db.mydb.commit()
    db.close()


    

def process_one_page(url):
    #url = 'https://ingener.in.ua'
        
    headers = {
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    responce = requests.get(url, headers = headers, allow_redirects = False)

    page = responce.text
    status_code = responce.status_code
      
    
    soup = BeautifulSoup(page, "html.parser")
    # soup = BeautifulSoup(page, "lxml")


    headings = soup.find_all(["h1", "h2", "h3"])
    h_list = []

    for heading in headings:
        tag_name = heading.name
        level = None
        text = heading.get_text()

        if tag_name and tag_name.startswith("h") and tag_name[1:].isdigit():
            level = int(tag_name[1:])
        else:
            level = 0
        h_list.append([level, text])
            
    
    tags = {'title': None, 'description': None, 'lang': None}

    title_element = soup.title
    if title_element is not None:
        tags['title'] = str(title_element.string)

    description_element = soup.find("meta", attrs={"name": "description"})
    tags['description'] = description_element.get("content") if description_element else None

    html_element = soup.find("html")
    tags['lang'] = html_element.get("lang") if html_element else None

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
        # row['anchor'] = str(link.text)
        data.append(row)
        uniq_data[(src, title, alt)] = None

    links = soup.find_all('a', href = True)
    link_for_save_set = set()
    link_for_save_list = []
    for link in links:
        l = link['href']
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
        if not (l.startswith('http://'+BASE) or l.startswith('https://'+BASE)):
            l = 'https://'+ BASE + l
        l = l.split('#')[0]
        link_for_save_set.add(l)
        link_for_save_list.append(l)
        
    return link_for_save_set, uniq_data, status_code, tags, h_list

if __name__ == '__main__':
    if len(sys.argv) == 1:
        BASE = 'ingener.in.ua'
        #BASE = 'ikscs.in.ua'
    elif len(sys.argv) == 2:
        BASE = sys.argv[1]
    else:
        print(f'Usage: {sys.argv[0]} BASE')
        quit(1)
    main(BASE)