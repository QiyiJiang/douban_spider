import requests
import time
import random
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed


class ProxyPool:
    """IP代理池管理器"""
    
    def __init__(self, proxy_file=None):
        """
        初始化代理池
        参数:
            proxy_file: 代理IP文件路径（每行一个代理，格式：http://ip:port 或 ip:port）
        """
        self.proxies = []
        self.valid_proxies = []
        self.failed_proxies = set()
        self.proxy_file = proxy_file
        
        if proxy_file:
            self._load_proxies_from_file()
        else:
            logger.warning("未提供代理文件，将尝试从免费代理网站获取")
            self._fetch_free_proxies()
    
    def _load_proxies_from_file(self):
        """从文件加载代理"""
        try:
            with open(self.proxy_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # 统一格式化代理
                        if not line.startswith('http'):
                            line = f"http://{line}"
                        self.proxies.append(line)
            
            logger.info(f"从文件加载了 {len(self.proxies)} 个代理")
            
            # 验证代理
            self._validate_proxies()
            
        except FileNotFoundError:
            logger.error(f"代理文件不存在: {self.proxy_file}")
        except Exception as e:
            logger.error(f"加载代理文件失败: {str(e)}")
    
    def _fetch_free_proxies(self):
        """从免费代理网站获取代理"""
        logger.info("正在获取免费代理...")
        
        # 尝试多个免费代理源
        proxy_sources = [
            self._fetch_from_free_proxy_list,
            self._fetch_from_kuaidaili,
            self._fetch_from_89ip,
        ]
        
        for source in proxy_sources:
            try:
                source()
                if len(self.proxies) >= 10:
                    break
            except Exception as e:
                logger.debug(f"从代理源获取失败: {str(e)}")
                continue
        
        if self.proxies:
            logger.info(f"获取到 {len(self.proxies)} 个免费代理")
            self._validate_proxies()
        else:
            logger.warning("未能获取到任何代理，将使用本地IP")
    
    def _fetch_from_kuaidaili(self):
        """从快代理获取免费代理"""
        try:
            url = "https://www.kuaidaili.com/free/inha/"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")
            
            for tr in soup.select("tbody tr")[:20]:
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    ip = tds[0].text.strip()
                    port = tds[1].text.strip()
                    proxy = f"http://{ip}:{port}"
                    self.proxies.append(proxy)
                    
        except Exception as e:
            logger.debug(f"从快代理获取失败: {str(e)}")
    
    def _fetch_from_89ip(self):
        """从89免费代理获取"""
        try:
            url = "https://www.89ip.cn/index_1.html"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")
            
            for tr in soup.select("tbody tr")[:20]:
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    ip = tds[0].text.strip()
                    port = tds[1].text.strip()
                    proxy = f"http://{ip}:{port}"
                    self.proxies.append(proxy)
                    
        except Exception as e:
            logger.debug(f"从89IP获取失败: {str(e)}")
    
    def _fetch_from_free_proxy_list(self):
        """从其他免费代理源获取"""
        try:
            # 这里可以添加更多免费代理源
            pass
        except Exception as e:
            logger.debug(f"从代理列表获取失败: {str(e)}")
    
    def _validate_single_proxy(self, proxy):
        """验证单个代理是否可用"""
        test_url = "https://www.douban.com"
        proxies = {
            "http": proxy,
            "https": proxy
        }
        
        try:
            response = requests.get(
                test_url,
                proxies=proxies,
                timeout=5,
                headers={"User-Agent": "Mozilla/5.0"}
            )
            
            if response.status_code == 200:
                return True, proxy
            else:
                return False, proxy
                
        except Exception:
            return False, proxy
    
    def _validate_proxies(self):
        """并发验证所有代理"""
        if not self.proxies:
            return
        
        logger.info(f"开始验证 {len(self.proxies)} 个代理...")
        
        valid_count = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self._validate_single_proxy, proxy): proxy 
                      for proxy in self.proxies}
            
            for future in as_completed(futures):
                try:
                    is_valid, proxy = future.result()
                    if is_valid:
                        self.valid_proxies.append(proxy)
                        valid_count += 1
                except Exception:
                    pass
        
        logger.info(f"代理验证完成: {valid_count}/{len(self.proxies)} 个可用")
        
        if not self.valid_proxies:
            logger.warning("没有可用的代理，将使用本地IP")
    
    def get_proxy(self):
        """获取一个随机代理"""
        # 优先使用已验证的代理
        if self.valid_proxies:
            available = [p for p in self.valid_proxies if p not in self.failed_proxies]
            if available:
                proxy = random.choice(available)
                return {
                    "http": proxy,
                    "https": proxy
                }
        
        # 如果没有已验证的，从所有代理中选择
        if self.proxies:
            available = [p for p in self.proxies if p not in self.failed_proxies]
            if available:
                proxy = random.choice(available)
                return {
                    "http": proxy,
                    "https": proxy
                }
        
        # 没有可用代理，返回None（使用本地IP）
        return None
    
    def mark_proxy_failed(self, proxy_dict):
        """标记代理失败"""
        if proxy_dict and "http" in proxy_dict:
            proxy = proxy_dict["http"]
            self.failed_proxies.add(proxy)
            logger.debug(f"标记代理失败: {proxy}")
    
    def get_stats(self):
        """获取代理池统计信息"""
        total = len(self.proxies)
        valid = len(self.valid_proxies)
        failed = len(self.failed_proxies)
        available = len([p for p in self.valid_proxies if p not in self.failed_proxies])
        
        return {
            "total": total,
            "valid": valid,
            "failed": failed,
            "available": available
        }


if __name__ == "__main__":
    # 测试代理池
    print("测试代理池...")
    
    # 方式1: 从文件加载
    # pool = ProxyPool(proxy_file="proxies.txt")
    
    # 方式2: 自动获取免费代理
    pool = ProxyPool()
    
    # 获取统计信息
    stats = pool.get_stats()
    print(f"\n代理池状态:")
    print(f"  总代理数: {stats['total']}")
    print(f"  可用代理: {stats['valid']}")
    print(f"  失败代理: {stats['failed']}")
    print(f"  当前可用: {stats['available']}")
    
    # 测试获取代理
    proxy = pool.get_proxy()
    if proxy:
        print(f"\n获取到代理: {proxy['http']}")
    else:
        print("\n未获取到代理，将使用本地IP")

