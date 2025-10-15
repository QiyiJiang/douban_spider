import argparse
import json
import random
import re
import os
import sys
import time
from datetime import datetime
import urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm


COOKIE = """
__utma=81379588.1951401692.1760341158.1760341158.1760341158.1; __utmb=81379588.4.10.1760341158; __utmz=81379588.1760341158.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utma=30149280.1653349273.1760341158.1760341158.1760341158.1; __utmb=30149280.5.10.1760341158; __utmz=30149280.1760341158.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); push_doumail_num=0; push_noty_num=0; _pk_ses.100001.3ac3=1; ck=18ki; dbcl2="185390670:6yj1AS6B3cY"; _vwo_uuid_v2=D5D15EC8AC1AF9C42281A1CAF2511DE7B|22bd8cc1ce77128f60aa5749db954905; __yadk_uid=cRfEK8NbNNIgjo7WFm1jub6HOgh9hLDi; __utmc=81379588; __utmc=30149280; ap_v=0,6.0; _pk_id.100001.3ac3=519854a4778ba754.1760341156.; bid=batW6D2h9cA
"""

class DoubanBookScraper:
    """豆瓣读书评论爬虫"""
    
    def __init__(self, cookie=None):
        """初始化爬虫"""
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        
        if cookie:
            cleaned_cookie = cookie.strip().replace("\n", "").replace("\r", "")
            self.headers["Cookie"] = cleaned_cookie
        
        self._setup_logger()
    
    def _setup_logger(self):
        """配置日志输出"""
        logger.remove()
        
        # 添加控制台输出（彩色）
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            level="INFO",
            colorize=True
        )
        
        # 添加文件输出
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logger.add(
            "logs/douban_scraper_{time:YYYY-MM-DD}.log",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            rotation="00:00",
            retention="7 days",
            encoding="utf-8",
            level="DEBUG"
        )
    
    def _load_existing_ids(self, filepath):
        """加载已爬取的ID集合"""
        if not os.path.exists(filepath):
            return set()
        
        existing_ids = set()
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        if "book_id" in data and "title" in data:
                            existing_ids.add(data["book_id"])
                        elif "review_id" in data:
                            if data["review_id"]:
                                existing_ids.add(data["review_id"])
        except Exception as e:
            logger.warning(f"加载已有数据失败: {str(e)}")
        
        return existing_ids
    
    def _append_to_jsonl(self, data, filepath):
        """追加数据到JSONL文件（线程安全）"""
        try:
            # 使用文件锁确保线程安全
            with open(filepath, "a", encoding="utf-8") as f:
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
            return True
        except Exception as e:
            logger.error(f"写入文件失败: {str(e)}")
            return False
    
    def search_book_id(self, book_name):
        """搜索书籍ID"""
        logger.info(f"[{book_name}] 开始搜索书籍")
        
        search_url = "https://www.douban.com/search"
        params = {"cat": "1001", "q": book_name}
        
        try:
            response = requests.get(
                search_url,
                headers=self.headers,
                params=params,
                timeout=15
            )
            response.encoding = "utf-8"
            soup = BeautifulSoup(response.text, "html.parser")
            
            for a in soup.find_all("a", href=True):
                href = a["href"]
                
                direct_match = re.search(
                    r"//(?:m\.)?book\.douban\.com/subject/(\d+)/", href
                )
                if direct_match:
                    book_id = direct_match.group(1)
                    logger.success(f"[{book_name}] 找到书籍ID: {book_id}")
                    return book_id
                
                if "link2" in href:
                    parsed_url = urllib.parse.urlparse(href)
                    qs = urllib.parse.parse_qs(parsed_url.query)
                    if "url" in qs:
                        real_url = urllib.parse.unquote(qs["url"][0])
                        jump_match = re.search(r"subject/(\d+)/", real_url)
                        if jump_match:
                            book_id = jump_match.group(1)
                            logger.success(f"[{book_name}] 找到书籍ID（跳转链接）: {book_id}")
                            return book_id
            
            logger.warning(f"[{book_name}] 未找到有效书籍链接")
            return None
            
        except Exception as e:
            logger.error(f"[{book_name}] 搜索失败: {str(e)}")
            return None

    def get_book_info(self, book_id, output_dir="output"):
        """爬取书籍基本信息"""
        filepath = f"{output_dir}/book_info.jsonl"
        
        existing_ids = self._load_existing_ids(filepath)
        if book_id in existing_ids:
            logger.info(f"书籍信息已存在，跳过: {book_id}")
            return True
        
        logger.info(f"开始爬取书籍信息 (ID: {book_id})")
        
        url = f"https://book.douban.com/subject/{book_id}/"
        headers = self.headers.copy()
        
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 提取书名
            title_elem = soup.select_one("h1 span[property='v:itemreviewed']")
            title = title_elem.text.strip() if title_elem else ""
            
            # 提取封面图片
            cover_elem = soup.select_one("#mainpic img")
            cover_image_url = cover_elem.get("src", "").replace("/s/", "/l/") if cover_elem else ""
            
            # 提取基本信息
            info_elem = soup.select_one("#info")
            subtitle = ""
            original_title = ""
            author_list = []
            translator_list = []
            publisher = ""
            publish_year = ""
            pages = None
            price = ""
            binding = ""
            series = ""
            isbn = ""
            
            if info_elem:
                info_html = str(info_elem)
                info_text = info_elem.get_text()
                
                subtitle_match = re.search(r'副标题:\s*([^\n]+)', info_text)
                if subtitle_match:
                    subtitle = subtitle_match.group(1).strip()
                
                original_match = re.search(r'原作名:\s*([^\n]+)', info_text)
                if original_match:
                    original_title = original_match.group(1).strip()
                
                author_section = re.search(r'<span class="pl">\s*作者</span>:(.*?)(?:<br>|</span>)', info_html, re.DOTALL)
                if author_section:
                    author_links = re.findall(r'<a[^>]*>([^<]+)</a>', author_section.group(1))
                    author_list = [author.strip() for author in author_links]
                
                translator_section = re.search(r'<span class="pl">\s*译者</span>:(.*?)(?:<br>|</span>)', info_html, re.DOTALL)
                if translator_section:
                    translator_links = re.findall(r'<a[^>]*>([^<]+)</a>', translator_section.group(1))
                    translator_list = [translator.strip() for translator in translator_links]
                
                publisher_link = info_elem.find("a", href=re.compile(r'/press/'))
                if publisher_link:
                    publisher = publisher_link.text.strip()
                
                publish_match = re.search(r'出版年:\s*([^\n]+)', info_text)
                if publish_match:
                    publish_year = publish_match.group(1).strip()
                
                pages_match = re.search(r'页数:\s*(\d+)', info_text)
                if pages_match:
                    pages = int(pages_match.group(1))
                
                price_match = re.search(r'定价:\s*([^\n]+)', info_text)
                if price_match:
                    price = price_match.group(1).strip()
                
                binding_match = re.search(r'装帧:\s*([^\n]+)', info_text)
                if binding_match:
                    binding = binding_match.group(1).strip()
                
                isbn_match = re.search(r'ISBN:\s*(\d+)', info_text)
                if isbn_match:
                    isbn = isbn_match.group(1).strip()
                
                series_link = info_elem.find("a", href=re.compile(r'/series/'))
                if series_link:
                    series = series_link.text.strip()
            
            # 提取评分
            rating_elem = soup.select_one("strong.rating_num[property='v:average']")
            rating_score = float(rating_elem.text.strip()) if rating_elem else None
            
            rating_count_elem = soup.select_one("span[property='v:votes']")
            rating_count = int(rating_count_elem.text.strip()) if rating_count_elem else 0
            
            # 提取内容简介
            summary = ""
            summary_h2 = soup.find("h2", string=lambda text: text and "内容简介" in text)
            if not summary_h2:
                summary_span = soup.find("span", string=re.compile(r'内容简介'))
                if summary_span:
                    summary_h2 = summary_span.find_parent("h2")

            if summary_h2:
                link_report = summary_h2.find_next("div", id="link-report")
                if link_report:
                    all_span = link_report.find("span", class_="all")
                    if all_span:
                        intro_div = all_span.find("div", class_="intro")
                        if intro_div:
                            paragraphs = intro_div.find_all("p")
                            summary = "\n".join([p.get_text(strip=True) for p in paragraphs])
                    
                    if not summary:
                        short_span = link_report.find("span", class_="short")
                        if short_span:
                            intro_div = short_span.find("div", class_="intro")
                            if intro_div:
                                paragraphs = intro_div.find_all("p")
                                summary = "\n".join([p.get_text(strip=True) for p in paragraphs])
            
            # 提取作者简介
            author_intro = ""
            author_h2 = soup.find("h2", string=lambda text: text and "作者简介" in text)
            if not author_h2:
                author_span = soup.find("span", string=re.compile(r'作者简介'))
                if author_span:
                    author_h2 = author_span.find_parent("h2")

            if author_h2:
                author_container = author_h2.find_next("div", class_="indent")
                if author_container:
                    all_span = author_container.find("span", class_="all")
                    if all_span:
                        intro_div = all_span.find("div", class_="intro")
                        if intro_div:
                            paragraphs = intro_div.find_all("p")
                            author_intro = "\n".join([p.get_text(strip=True) for p in paragraphs])
                    
                    if not author_intro:
                        short_span = author_container.find("span", class_="short")
                        if short_span:
                            intro_div = short_span.find("div", class_="intro")
                            if intro_div:
                                paragraphs = intro_div.find_all("p")
                                author_intro = "\n".join([p.get_text(strip=True) for p in paragraphs])

            # 提取目录
            catalog = ""
            catalog_span = soup.find("span", string=re.compile(r'目录'))
            if catalog_span:
                catalog_h2 = catalog_span.find_parent("h2")
                if catalog_h2:
                    catalog_div = catalog_h2.find_next_sibling("div", id=re.compile(r'dir_\d+_full'))
                    if not catalog_div:
                        catalog_div = catalog_h2.find_next_sibling("div", id=re.compile(r'dir_\d+_short'))
                    
                    if catalog_div:
                        for br in catalog_div.find_all("br"):
                            br.replace_with("\n")
                        
                        for a in catalog_div.find_all("a"):
                            a.decompose()
                        
                        catalog = catalog_div.get_text(strip=False).strip()
                        catalog = re.sub(r'\n\s*\n+', '\n', catalog)
                        catalog = re.sub(r'· · · · · ·', '', catalog).strip()
            
            # 提取标签
            tag_list = []
            try:
                script_tags = soup.find_all("script", type="application/ld+json")
                for script in script_tags:
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, dict) and "keywords" in data:
                            keywords = data["keywords"].split(",")
                            tag_list = [{"name": tag.strip(), "count": 0} for tag in keywords if tag.strip()]
                    except:
                        pass
            except:
                pass
            
            if not tag_list:
                meta_keywords = soup.find("meta", {"name": "keywords"})
                if meta_keywords and meta_keywords.get("content"):
                    keywords = meta_keywords["content"].split(",")
                    tag_list = [{"name": tag.strip(), "count": 0} for tag in keywords if tag.strip() and tag.strip() not in ["书评", "论坛", "推荐", "二手"]]
            
            # 构建书籍信息
            book_info = {
                "book_id": book_id,
                "title": title,
                "subtitle": subtitle,
                "original_title": original_title,
                "author_list": json.dumps(author_list, ensure_ascii=False),
                "translator_list": json.dumps(translator_list, ensure_ascii=False),
                "publisher": publisher,
                "publish_year": publish_year,
                "pages": pages,
                "price": price,
                "binding": binding,
                "isbn": isbn,
                "series": series,
                "rating_score": rating_score,
                "rating_count": rating_count,
                "summary": summary,
                "author_intro": author_intro,
                "catalog": catalog,
                "douban_url": url,
                "cover_image_url": cover_image_url,
                "tag_list": json.dumps(tag_list, ensure_ascii=False),
                "crawled_at": datetime.now().isoformat()
            }
            
            os.makedirs(output_dir, exist_ok=True)
            if self._append_to_jsonl(book_info, filepath):
                logger.success(f"书籍信息已保存: {title}")
                return True
            
        except Exception as e:
            logger.error(f"爬取书籍信息失败: {str(e)}")
            return False

    def get_book_comments(self, book_id, max_comments=200, comments_per_page=20, output_dir="output"):
        """爬取书籍短评"""
        filepath = f"{output_dir}/comments.jsonl"
        
        existing_ids = self._load_existing_ids(filepath)
        logger.info(f"开始爬取书籍短评 (ID: {book_id}, 目标: {max_comments}条)")
        
        headers = self.headers.copy()
        headers["Referer"] = f"https://book.douban.com/subject/{book_id}/"
        
        base_url = f"https://book.douban.com/subject/{book_id}/comments/"
        new_count = 0
        page_count = 0
        max_pages = (max_comments + comments_per_page - 1) // comments_per_page
        
        os.makedirs(output_dir, exist_ok=True)
        
        with tqdm(total=max_comments, desc="爬取短评", unit="条", leave=False) as pbar:
            while page_count < max_pages and new_count < max_comments:
                params = {
                    "start": page_count * comments_per_page,
                    "limit": comments_per_page,
                    "status": "P",
                    "sort": "score"
                }
                
                try:
                    response = requests.get(base_url, headers=headers, params=params, timeout=15)
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    items = soup.select(".comment-item")
                    if not items:
                        break
                    
                    page_new_count = 0
                    
                    for item in items:
                        try:
                            review_id = item.get("data-cid", "")
                            
                            if not review_id:
                                vote_elem = item.select_one(".vote-count")
                                if vote_elem and vote_elem.get("id"):
                                    review_id = vote_elem.get("id").replace("c-", "")
                            
                            if not review_id:
                                content_elem = item.select_one(".comment-content .short")
                                if content_elem:
                                    content_text = content_elem.text.strip()[:50]
                                    time_elem = item.select_one(".comment-time")
                                    time_text = time_elem.text.strip() if time_elem else ""
                                    review_id = f"{hash(content_text + time_text)}"
                            
                            if not review_id or review_id in existing_ids:
                                continue
                            
                            user_link = item.select_one(".avatar a")
                            user_id = ""
                            if user_link and user_link.get("href"):
                                user_match = re.search(r'/people/([^/]+)/', user_link["href"])
                                user_id = user_match.group(1) if user_match else ""
                            
                            user_name = item.select_one(".comment-info a")
                            user_name = user_name.text.strip() if user_name else ""
                            
                            user_avatar = item.select_one(".avatar img")
                            user_avatar_url = user_avatar.get("src", "") if user_avatar else ""
                            
                            rating_elem = item.select_one(".user-stars")
                            rating = None
                            if rating_elem:
                                rating_class = rating_elem.get("class", [])
                                for cls in rating_class:
                                    if cls.startswith("allstar"):
                                        rating_num = cls.replace("allstar", "")
                                        rating = int(rating_num) // 10 if rating_num.isdigit() else None
                                        break
                            
                            content_elem = item.select_one(".comment-content .short")
                            content = content_elem.text.strip() if content_elem else ""
                            
                            useful_elem = item.select_one(".vote-count")
                            useful_count = int(useful_elem.text.strip()) if useful_elem and useful_elem.text.strip().isdigit() else 0
                            
                            time_elem = item.select_one(".comment-time")
                            published_at = time_elem.text.strip() if time_elem else ""
                            
                            comment = {
                                "review_id": str(review_id),
                                "book_id": book_id,
                                "user_id": user_id,
                                "user_name": user_name,
                                "user_avatar_url": user_avatar_url,
                                "rating": rating,
                                "content": content,
                                "useful_count": useful_count,
                                "published_at": published_at,
                                "crawled_at": datetime.now().isoformat()
                            }
                            
                            if self._append_to_jsonl(comment, filepath):
                                existing_ids.add(str(review_id))
                                new_count += 1
                                page_new_count += 1
                                pbar.update(1)
                            
                            if new_count >= max_comments:
                                break
                            
                        except Exception as e:
                            continue
                    
                    page_count += 1
                    
                    if page_new_count == 0:
                        break
                    
                    if new_count >= max_comments:
                        break
                    
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    logger.error(f"抓取第 {page_count + 1} 页失败: {str(e)}")
                    break
        
        logger.success(f"短评爬取完成: 新增 {new_count} 条")
        return new_count > 0

    def get_book_review(self, book_id, max_comments=200, comments_per_page=20, 
                        fetch_full_content=True, output_dir="output"):
        """爬取书籍长评"""
        filepath = f"{output_dir}/reviews.jsonl"
        
        existing_ids = self._load_existing_ids(filepath)
        logger.info(f"开始爬取书籍长评 (ID: {book_id}, 目标: {max_comments}条)")
        
        headers = self.headers.copy()
        headers["Referer"] = f"https://book.douban.com/subject/{book_id}/"
        
        base_url = f"https://book.douban.com/subject/{book_id}/reviews"
        new_count = 0
        page_count = 0
        max_pages = (max_comments + comments_per_page - 1) // comments_per_page
        
        os.makedirs(output_dir, exist_ok=True)
        
        with tqdm(total=max_comments, desc="爬取长评", unit="条", leave=False) as pbar:
            while page_count < max_pages and new_count < max_comments:
                params = {
                    "start": page_count * comments_per_page,
                    "limit": comments_per_page,
                    "sort": "hotest"
                }
                
                try:
                    response = requests.get(base_url, headers=headers, params=params, timeout=15)
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    items = soup.select(".review-item")
                    if not items:
                        break
                    
                    page_new_count = 0
                    
                    for item in items:
                        try:
                            review_id = item.get("id", "")
                            
                            if not review_id:
                                title_elem = item.select_one("h2 a")
                                if title_elem and title_elem.get("href"):
                                    review_match = re.search(r'/review/(\d+)/', title_elem["href"])
                                    if review_match:
                                        review_id = review_match.group(1)
                            
                            if not review_id or review_id in existing_ids:
                                continue
                            
                            user_link = item.select_one(".avator")
                            user_id = ""
                            if user_link and user_link.get("href"):
                                user_match = re.search(r'/people/([^/]+)/', user_link["href"])
                                user_id = user_match.group(1) if user_match else ""
                            
                            user_name_elem = item.select_one(".name")
                            user_name = user_name_elem.text.strip() if user_name_elem else ""
                            
                            user_avatar = item.select_one(".avator img")
                            user_avatar_url = user_avatar.get("src", "") if user_avatar else ""
                            
                            rating_elem = item.select_one(".main-title-rating")
                            rating = None
                            if rating_elem:
                                rating_class = rating_elem.get("class", [])
                                for cls in rating_class:
                                    if cls.startswith("allstar"):
                                        rating_num = cls.replace("allstar", "")
                                        rating = int(rating_num) // 10 if rating_num.isdigit() else None
                                        break
                            
                            title_elem = item.select_one("h2 a")
                            title = title_elem.text.strip() if title_elem else ""
                            
                            review_url = title_elem.get("href", "") if title_elem else ""
                            if review_url and not review_url.startswith("http"):
                                review_url = "https://book.douban.com" + review_url
                            
                            content_elem = item.select_one(".short-content")
                            content = content_elem.text.strip() if content_elem else ""
                            
                            has_spoiler = 1 if item.select_one(".spoiler-tip") else 0
                            
                            edition_elem = item.select_one(".publisher")
                            book_edition = edition_elem.text.strip() if edition_elem else ""
                            
                            useful_elem = item.select_one("[id^='r-useful_count-']")
                            useful_count = int(useful_elem.text.strip()) if useful_elem and useful_elem.text.strip().isdigit() else 0
                            
                            unuseful_elem = item.select_one("[id^='r-useless_count-']")
                            unuseful_count = int(unuseful_elem.text.strip()) if unuseful_elem and unuseful_elem.text.strip().isdigit() else 0
                            
                            reply_elem = item.select_one(".reply")
                            comment_count = 0
                            if reply_elem:
                                reply_text = reply_elem.text.strip()
                                reply_match = re.search(r'(\d+)', reply_text)
                                comment_count = int(reply_match.group(1)) if reply_match else 0
                            
                            time_elem = item.select_one(".main-meta")
                            published_at = time_elem.text.strip() if time_elem else ""
                            
                            review = {
                                "review_id": str(review_id),
                                "book_id": book_id,
                                "title": title,
                                "user_id": user_id,
                                "user_name": user_name,
                                "user_avatar_url": user_avatar_url,
                                "rating": rating,
                                "content": content,
                                "has_spoiler": has_spoiler,
                                "book_edition": book_edition,
                                "useful_count": useful_count,
                                "unuseful_count": unuseful_count,
                                "comment_count": comment_count,
                                "review_url": review_url,
                                "published_at": published_at,
                                "updated_at": None,
                                "crawled_at": datetime.now().isoformat()
                            }
                            
                            if fetch_full_content and review_url:
                                try:
                                    detail_response = requests.get(review_url, headers=headers, timeout=15)
                                    detail_soup = BeautifulSoup(detail_response.text, "html.parser")
                                    
                                    full_content_elem = detail_soup.select_one(".review-content")
                                    if full_content_elem:
                                        review["content"] = full_content_elem.text.strip()
                                    
                                    time.sleep(random.uniform(1, 2))
                                except Exception as e:
                                    pass
                            
                            if self._append_to_jsonl(review, filepath):
                                existing_ids.add(str(review_id))
                                new_count += 1
                                page_new_count += 1
                                pbar.update(1)
                            
                            if new_count >= max_comments:
                                break
                            
                        except Exception as e:
                            continue
                    
                    page_count += 1
                    
                    if page_new_count == 0:
                        break
                    
                    if new_count >= max_comments:
                        break
                    
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    logger.error(f"抓取第 {page_count + 1} 页失败: {str(e)}")
                    break
        
        logger.success(f"长评爬取完成: 新增 {new_count} 条")
        return new_count > 0

    def run(self, book_name, max_comments=200, manual_id=None, output_dir="output"):
        """执行完整的爬取流程"""
        logger.info(f"开始爬取【{book_name}】的评论")
        
        # 获取书籍ID
        if manual_id:
            book_id = manual_id
            logger.info(f"使用手动指定的书籍ID: {book_id}")
        else:
            book_id = self.search_book_id(book_name)
            
            if not book_id:
                logger.error(f"[{book_name}] 未找到书籍ID")
                return None
        
        # 爬取各类数据
        self.get_book_info(book_id, output_dir=output_dir)
        self.get_book_comments(book_id, max_comments=max_comments, output_dir=output_dir)
        self.get_book_review(book_id, max_comments=max_comments, output_dir=output_dir)

        logger.success(f"【{book_name}】爬取完成")
        return book_id


def crawl_single_book(args_tuple):
    """单本书的爬取任务（用于并行处理）"""
    book_name, max_comments, output_base, cookie = args_tuple
    
    try:
        scraper = DoubanBookScraper(cookie=cookie)
        output = f"{output_base}/{book_name}"
        os.makedirs(output, exist_ok=True)
        
        # 随机延迟启动，避免同时请求
        time.sleep(random.uniform(0.5, 2))
        
        scraper.run(
            book_name=book_name,
            max_comments=max_comments,
            output_dir=output
        )
        
        return True, book_name
    except Exception as e:
        logger.error(f"[{book_name}] 爬取失败: {str(e)}")
        return False, book_name


def main():
    """主程序入口"""
    print("\n" + "="*60)
    print("豆瓣读书评论爬虫 v2.4 (并行优化版)")
    print("="*60 + "\n")
    
    parser = argparse.ArgumentParser(
        description="豆瓣读书评论爬虫",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument("--book_name", "-n", type=str, nargs='+', required=True, help="要爬取的书名（可以是单个或多个，用空格分隔）")
    parser.add_argument("--max_comments", "-m", type=int, default=100, help="要爬取的评论数量（默认100）")
    parser.add_argument("--output_dir", "-o", type=str, default="./output", help="输出目录（默认./output）")
    parser.add_argument("--cookie_file", "-c", type=str, default=None, help="豆瓣Cookie文件路径（可选）")
    parser.add_argument("--workers", "-w", type=int, default=2, help="并发线程数（默认2，建议2-4）")
    
    args = parser.parse_args()
    
    if args.cookie_file:
        with open(args.cookie_file, "r", encoding="utf-8") as f:
            cookie = f.read()
    else:
        cookie = COOKIE
    
    book_names = args.book_name if isinstance(args.book_name, list) else [args.book_name]
    
    logger.info(f"共需要爬取 {len(book_names)} 本书")
    logger.info(f"并发线程数: {args.workers}")
    
    # 准备任务参数
    tasks = [(name, args.max_comments, args.output_dir, cookie) for name in book_names]
    
    # 使用线程池并行爬取
    success_count = 0
    failed_books = []
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(crawl_single_book, task): task[0] for task in tasks}
        
        with tqdm(total=len(book_names), desc="总体进度", unit="本") as pbar:
            for future in as_completed(futures):
                book_name = futures[future]
                try:
                    success, name = future.result()
                    if success:
                        success_count += 1
                    else:
                        failed_books.append(name)
                except Exception as e:
                    logger.error(f"[{book_name}] 任务执行异常: {str(e)}")
                    failed_books.append(book_name)
                finally:
                    pbar.update(1)
    
    # 输出统计信息
    logger.info(f"\n{'='*60}")
    logger.info(f"爬取完成统计:")
    logger.info(f"  成功: {success_count}/{len(book_names)} 本")
    if failed_books:
        logger.warning(f"  失败: {len(failed_books)} 本")
        logger.warning(f"  失败书籍: {', '.join(failed_books)}")
    logger.info(f"{'='*60}\n")


if __name__ == "__main__":
    main()