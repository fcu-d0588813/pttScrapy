import time
import urllib.parse
from multiprocessing import Pool
import pandas as pd
from bs4 import Tag, BeautifulSoup as bs
import requests
 


INDEX = 'https://www.ptt.cc/bbs/Makeup/index.html'
NOT_EXIST = bs('<a>本文已被刪除</a>', 'lxml').a

titles = []
links = []
datas=[]
authors=[]
contents=[]

#抓取一頁 (某頁面 URL) 中所有的文章的 metadata，並回傳一串 key-value 類型的資料及下一頁的 URL
def get_posts_list(url):
    response = requests.get(url)
    soup =bs(response.text, 'lxml')

    articles = soup.find_all('div', 'r-ent')

    posts = list()
    for article in articles:
        meta = article.find('div', 'title').find('a') or NOT_EXIST
        posts.append({
            'title': meta.getText().strip(),
            'link': meta.get('href'),
            'push': article.find('div', 'nrec').getText(),
            'date': article.find('div', 'date').getText(),
            'author': article.find('div', 'author').getText(),
        })

    next_link = soup.find('div', 'btn-group-paging').find_all('a', 'btn')[1].get('href')

    return posts, next_link


#取最新的 pages 個頁面，並指派 get_paged_meta 去抓每頁面中的資料，把每一串資料合併成一大串後回傳
def get_paged_meta(page):
    page_url = INDEX
    all_posts = list()
    for i in range(page):
        posts, link = get_posts_list(page_url)
        all_posts += posts
        page_url = urllib.parse.urljoin(INDEX, link)
        print(i)
    return all_posts


#抓取文章內容
def get_articles(metadata):
    post_links = [meta['link'] for meta in metadata]  #一串文章的 URL
    with Pool(processes=8) as pool:  #開8個processes來完成任務
        contents = pool.map(fetch_article_content, post_links)
        return contents


def fetch_article_content(link):
    url = urllib.parse.urljoin(INDEX, link) 
    response = requests.get(url)
    return response.text


if __name__ == '__main__':
    pages = 5

    start = time.time()
    metadata = get_paged_meta(pages)
    articles = get_articles(metadata)


    for post, content in zip(metadata, articles):
        link='https://www.ptt.cc/'+str(post['link'])
        titles.append(post['title'])
        links.append(link) 
        datas.append(post['date'])
        authors.append(post['author'])
        soup = bs(content,"html.parser")
        #若文章已刪除會找不到連結及內文
        if post['title'].find('刪除')==-1:
            #擷取內文，將不要的部份去掉
           content=soup.find("div",{"id":"main-content"})
           removes = content.find_all("div",{"class":"article-metaline"})
           for single_remove in removes:
               single_remove.extract()
           removes=content.find_all("div",{"class":"article-metaline-right"})
           for single_remove in removes:
               single_remove.extract()
           removes = content.find_all("span", {"class": "f2"})
           for single_remove in removes:
               single_remove.extract()           
           removes = content.find_all("div", {"class": "push"})
           for single_remove in removes:
               single_remove.extract()
           revise=content.text.replace("\r",'').replace("\n","")
        contents.append(revise)
    
    #計算時間
    print('花費: %f 秒' % (time.time() - start))
    print('共%d項結果：' % len(articles))
    
    #將資料存到dataframe
    activity_df = pd.DataFrame({"日期":datas,"作者":authors,"標題":titles,"連結":links,"內容":contents})
    activity_df.to_csv("TEST.csv", index=False)
