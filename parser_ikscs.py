import time
import requests
from bs4 import BeautifulSoup
from my_base import My_base


def main():


    db = My_base()
    db.open()
    
    count = 0 
    urls = [7]
    while (len(urls) > 0) and count < 1:
        print('Records:', len(urls), count)
        time.sleep(0.5)
        count += 1
        sql = "SELECT `link` FROM parse_ikscs_links WHERE `status` IS NULL"

        # urls = []
        # db.cursor.execute(sql)
        # for e in db.cursor.fetchall():
        #    urls.append(e[0])
       
        db.cursor.execute(sql)
        urls = [e[0] for e in db.cursor.fetchall()]                    # спрацювало
    
        for url in urls:
            link_for_save_set, data_uniq = process_one_page(url)

            data = []
            for k, v in data_uniq.items():
                data.append([*k, v])
        
            if link_for_save_set:
                sql = 'INSERT IGNORE INTO parse_ikscs_links (link, status) VALUES (%s, %s)'
                values = [(e, None) for e in link_for_save_set]
                db.cursor.executemany(sql, values)


            sql = f'UPDATE parse_ikscs_links SET status="READY" WHERE link = "{url}"'
            db.cursor.execute(sql)

            sql = 'INSERT IGNORE INTO parse_ikscs_image (src, title, alt, a_href, refer) VALUES (%s, %s, %s, %s, %s)'
            values = []
            for row in data:
                values.append((*row, url))
            db.cursor.executemany(sql, values)
            db.mydb.commit()
        
    db.close()
    
    
def process_one_page(url):
    base = "ikscs.in.ua"
        
    headers = {
        'User-Agent' : 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36'
    }

    responce = requests.get(url, headers=headers)

    page = responce.text

    soup = BeautifulSoup(page, "html.parser")
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
        if not (l.startswith('/') or l.startswith('.') or l.startswith('http://'+base) or l.startswith('https://'+base)):
            continue
        if l.startswith('/./'):
            l = 'https://'+ base + l[1:]
        elif l.startswith('..'):
            l = 'https://'+ base + l[2:]
        elif l.startswith('.'):
            l = 'https://'+ base + l[1:]
        elif l.startswith('/'):
            l = 'https://'+ base + l
        l = l.split('#')[0]
        print('!', l)
        if '/./' in l:
            pass
        link_for_save_set.add(l)
        link_for_save_list.append(l)
        
    return link_for_save_set, uniq_data

if __name__ == '__main__':
    main()