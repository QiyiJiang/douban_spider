#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPUB 电子书内容解析脚本

功能：
1. 提取 EPUB 元数据（书名、作者、出版社等）
2. 提取章节内容和目录结构
3. 导出为 JSON 或 TXT 格式
4. 支持批量处理

使用方法：
    python epub_parser.py <epub_file_path> [--output <output_path>] [--format json|txt]
"""

import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from ebooklib import epub
from bs4 import BeautifulSoup

from logger_config import setup_logger

# 配置日志（模块级别，只配置一次）
logger = setup_logger(module_name="epub_parser", console_level="INFO", file_level="DEBUG")


class EpubParser:
    """EPUB 电子书解析器"""

    def __init__(self, epub_path: str):
        """
        初始化解析器
        
        Args:
            epub_path: EPUB 文件路径
        """
        self.epub_path = epub_path
        self.book = None
        self.metadata = {}
        self.chapters = []
        self.toc = []

    def load_book(self) -> bool:
        """
        加载 EPUB 文件
        
        Returns:
            bool: 是否成功加载
        """
        try:
            logger.info(f"正在加载 EPUB 文件: {self.epub_path}")
            self.book = epub.read_epub(self.epub_path)
            logger.success("EPUB 文件加载成功")
            return True
        except Exception as e:
            logger.error(f"加载 EPUB 文件失败: {e}")
            return False

    def extract_metadata(self) -> Dict:
        """
        提取元数据
        
        Returns:
            Dict: 元数据字典
        """
        if not self.book:
            logger.error("请先加载 EPUB 文件")
            return {}

        try:
            logger.info("正在提取元数据...")
            
            # 提取基本元数据
            metadata = {
                'title': self._get_metadata('DC', 'title'),
                'author': self._get_metadata('DC', 'creator'),
                'language': self._get_metadata('DC', 'language'),
                'publisher': self._get_metadata('DC', 'publisher'),
                'date': self._get_metadata('DC', 'date'),
                'identifier': self._get_metadata('DC', 'identifier'),
                'subject': self._get_metadata('DC', 'subject'),
                'description': self._get_metadata('DC', 'description'),
                'rights': self._get_metadata('DC', 'rights'),
                'file_path': self.epub_path,
                'file_name': os.path.basename(self.epub_path),
                'file_size': f"{os.path.getsize(self.epub_path)/1024/1024:.2f}MB",
            }
            
            self.metadata = metadata
            logger.success(f"元数据提取成功: {metadata.get('title', 'Unknown')}")
            return metadata
            
        except Exception as e:
            logger.error(f"提取元数据失败: {e}")
            return {}

    def _get_metadata(self, namespace: str, name: str) -> Optional[str]:
        """
        获取指定的元数据
        
        Args:
            namespace: 命名空间
            name: 元数据名称
            
        Returns:
            str: 元数据值
        """
        try:
            data = self.book.get_metadata(namespace, name)
            if data:
                # 元数据是列表形式，取第一个
                return data[0][0] if isinstance(data[0], tuple) else data[0]
        except:
            pass
        return None

    def extract_chapters(self, clean_text: bool = True) -> List[Dict]:
        """
        提取章节内容
        
        Args:
            clean_text: 是否清理文本（去除多余空白）
            
        Returns:
            List[Dict]: 章节列表
        """
        if not self.book:
            logger.error("请先加载 EPUB 文件")
            return []

        try:
            logger.info("正在提取章节内容...")
            chapters = []
            chapter_num = 0
            
            for item in self.book.get_items():
                # 只处理文档类型的项
                if item.get_type() == 9:  # ebooklib.ITEM_DOCUMENT
                    try:
                        # 解析 HTML 内容
                        content = item.get_content()
                        soup = BeautifulSoup(content, 'html.parser')
                        
                        # 提取文本
                        text = soup.get_text(separator='\n', strip=True)
                        
                        # 清理文本
                        if clean_text:
                            text = self._clean_text(text)
                        
                        # 跳过空章节
                        if not text or len(text.strip()) < 10:
                            continue
                        
                        chapter_num += 1
                        
                        # 尝试提取章节标题
                        title = self._extract_chapter_title(soup, item.get_name())
                        
                        chapter_info = {
                            'chapter_num': chapter_num,
                            'title': title,
                            'file_name': item.get_name(),
                            'content': text,
                            'word_count': len(text),
                        }
                        
                        chapters.append(chapter_info)
                        logger.debug(f"提取章节 {chapter_num}: {title} (字数: {len(text)})")
                        
                    except Exception as e:
                        logger.warning(f"提取章节失败: {item.get_name()}, 错误: {e}")
                        continue
            
            self.chapters = chapters
            logger.success(f"章节提取完成，共 {len(chapters)} 个章节")
            return chapters
            
        except Exception as e:
            logger.error(f"提取章节内容失败: {e}")
            return []

    def _clean_text(self, text: str) -> str:
        """
        清理文本内容
        
        Args:
            text: 原始文本
            
        Returns:
            str: 清理后的文本
        """
        # 移除多余的空行
        text = re.sub(r'\n\s*\n', '\n\n', text)
        # 移除行首行尾空白
        text = '\n'.join(line.strip() for line in text.split('\n'))
        return text.strip()

    def _extract_chapter_title(self, soup: BeautifulSoup, default_name: str) -> str:
        """
        提取章节标题
        
        Args:
            soup: BeautifulSoup 对象
            default_name: 默认名称
            
        Returns:
            str: 章节标题
        """
        # 尝试从 h1, h2, h3 标签中提取标题
        for tag in ['h1', 'h2', 'h3', 'h4']:
            heading = soup.find(tag)
            if heading:
                title = heading.get_text(strip=True)
                if title:
                    return title
        
        # 如果没有找到标题，使用默认名称
        return default_name.replace('.xhtml', '').replace('.html', '').replace('_', ' ')

    def extract_toc(self) -> List[Dict]:
        """
        提取目录结构
        
        Returns:
            List[Dict]: 目录列表
        """
        if not self.book:
            logger.error("请先加载 EPUB 文件")
            return []

        try:
            logger.info("正在提取目录结构...")
            toc = []
            
            def parse_toc_item(item, level=0):
                """递归解析目录项"""
                if isinstance(item, tuple):
                    # (Section, [子项列表])
                    section, children = item
                    toc_item = {
                        'title': section.title,
                        'href': section.href if hasattr(section, 'href') else None,
                        'level': level,
                        'children': []
                    }
                    
                    # 递归处理子项
                    for child in children:
                        child_item = parse_toc_item(child, level + 1)
                        if child_item:
                            toc_item['children'].append(child_item)
                    
                    return toc_item
                else:
                    # Link 对象
                    return {
                        'title': item.title,
                        'href': item.href if hasattr(item, 'href') else None,
                        'level': level,
                        'children': []
                    }
            
            # 解析 TOC
            for item in self.book.toc:
                toc_item = parse_toc_item(item)
                if toc_item:
                    toc.append(toc_item)
            
            self.toc = toc
            logger.success(f"目录提取完成，共 {len(toc)} 个顶级项")
            return toc
            
        except Exception as e:
            logger.error(f"提取目录结构失败: {e}")
            return []

    def get_full_text(self) -> str:
        """
        获取全书文本
        
        Returns:
            str: 全书文本
        """
        if not self.chapters:
            self.extract_chapters()
        
        return '\n\n'.join(chapter['content'] for chapter in self.chapters)

    def get_statistics(self) -> Dict:
        """
        获取统计信息
        
        Returns:
            Dict: 统计信息
        """
        if not self.chapters:
            self.extract_chapters()
        
        full_text = self.get_full_text()
        
        return {
            'chapter_count': len(self.chapters),
            'total_words': len(full_text),
            'total_chars': len(full_text),
            'avg_chapter_length': len(full_text) // len(self.chapters) if self.chapters else 0,
        }

    def export_to_json(self, output_path: Optional[str] = None) -> str:
        """
        导出为 JSON 格式
        
        Args:
            output_path: 输出路径，不指定则使用默认路径
            
        Returns:
            str: 输出文件路径
        """
        if not self.chapters:
            self.extract_chapters()
        
        if not self.metadata:
            self.extract_metadata()
        
        if not self.toc:
            self.extract_toc()
        
        # 确定输出路径
        if not output_path:
            output_path = str(Path(self.epub_path).with_suffix('.json'))
        
        try:
            logger.info(f"正在导出为 JSON: {output_path}")
            
            data = {
                'metadata': self.metadata,
                'statistics': self.get_statistics(),
                'toc': self.toc,
                'chapters': self.chapters,
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.success(f"导出成功: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"导出 JSON 失败: {e}")
            return ""

    def export_to_txt(self, output_path: Optional[str] = None, 
                      include_metadata: bool = True,
                      include_toc: bool = True) -> str:
        """
        导出为纯文本格式
        
        Args:
            output_path: 输出路径，不指定则使用默认路径
            include_metadata: 是否包含元数据
            include_toc: 是否包含目录
            
        Returns:
            str: 输出文件路径
        """
        if not self.chapters:
            self.extract_chapters()
        
        if not self.metadata:
            self.extract_metadata()
        
        if not self.toc:
            self.extract_toc()
        
        # 确定输出路径
        if not output_path:
            output_path = str(Path(self.epub_path).with_suffix('.txt'))
        
        try:
            logger.info(f"正在导出为 TXT: {output_path}")
            
            with open(output_path, 'w', encoding='utf-8') as f:
                # 写入元数据
                if include_metadata and self.metadata:
                    f.write("=" * 80 + "\n")
                    f.write("书籍信息\n")
                    f.write("=" * 80 + "\n\n")
                    
                    for key, value in self.metadata.items():
                        if value and key not in ['file_path', 'file_size']:
                            f.write(f"{key}: {value}\n")
                    
                    # 统计信息
                    stats = self.get_statistics()
                    f.write(f"\n章节数: {stats['chapter_count']}\n")
                    f.write(f"总字数: {stats['total_words']:,}\n")
                    f.write("\n\n")
                
                # 写入目录
                if include_toc and self.toc:
                    f.write("=" * 80 + "\n")
                    f.write("目录\n")
                    f.write("=" * 80 + "\n\n")
                    
                    def write_toc_item(item, indent=0):
                        """递归写入目录项"""
                        prefix = "  " * indent + "• "
                        f.write(f"{prefix}{item['title']}\n")
                        for child in item.get('children', []):
                            write_toc_item(child, indent + 1)
                    
                    for item in self.toc:
                        write_toc_item(item)
                    
                    f.write("\n\n")
                
                # 写入章节内容
                f.write("=" * 80 + "\n")
                f.write("正文\n")
                f.write("=" * 80 + "\n\n")
                
                for chapter in self.chapters:
                    f.write("\n" + "=" * 80 + "\n")
                    f.write(f"第 {chapter['chapter_num']} 章: {chapter['title']}\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(chapter['content'])
                    f.write("\n\n")
            
            logger.success(f"导出成功: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"导出 TXT 失败: {e}")
            return ""

    def parse(self) -> Dict:
        """
        完整解析 EPUB（提取所有信息）
        
        Returns:
            Dict: 包含所有信息的字典
        """
        if not self.load_book():
            return {}
        
        self.extract_metadata()
        self.extract_chapters()
        self.extract_toc()
        
        return {
            'metadata': self.metadata,
            'statistics': self.get_statistics(),
            'toc': self.toc,
            'chapters': self.chapters,
        }


def batch_parse_directory(directory: str, output_format: str = 'json') -> List[str]:
    """
    批量解析目录下的所有 EPUB 文件
    
    Args:
        directory: 目录路径
        output_format: 输出格式 (json/txt)
        
    Returns:
        List[str]: 输出文件路径列表
    """
    directory_path = Path(directory)
    epub_files = list(directory_path.glob('**/*.epub'))
    
    logger.info(f"找到 {len(epub_files)} 个 EPUB 文件")
    
    output_files = []
    
    for epub_file in epub_files:
        try:
            logger.info(f"正在处理: {epub_file}")
            parser = EpubParser(str(epub_file))
            parser.parse()
            
            if output_format == 'json':
                output_file = parser.export_to_json()
            else:
                output_file = parser.export_to_txt()
            
            if output_file:
                output_files.append(output_file)
                
        except Exception as e:
            logger.error(f"处理 {epub_file} 失败: {e}")
            continue
    
    logger.success(f"批量处理完成，成功处理 {len(output_files)} 个文件")
    return output_files


def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description='EPUB 电子书内容解析工具')
    parser.add_argument('epub_path', help='EPUB 文件路径或目录路径')
    parser.add_argument('-o', '--output', help='输出文件路径')
    parser.add_argument('-f', '--format', choices=['json', 'txt'], default='json',
                        help='输出格式 (默认: json)')
    parser.add_argument('-b', '--batch', action='store_true',
                        help='批量处理模式（处理目录下所有 EPUB 文件）')
    parser.add_argument('--no-metadata', action='store_true',
                        help='导出 TXT 时不包含元数据')
    parser.add_argument('--no-toc', action='store_true',
                        help='导出 TXT 时不包含目录')
    
    args = parser.parse_args()
    
    # 批量处理模式
    if args.batch:
        batch_parse_directory(args.epub_path, args.format)
        return
    
    # 单文件处理模式
    epub_parser = EpubParser(args.epub_path)
    
    if not epub_parser.load_book():
        logger.error("加载 EPUB 文件失败")
        return
    
    # 解析
    epub_parser.extract_metadata()
    epub_parser.extract_chapters()
    epub_parser.extract_toc()
    
    # 导出
    if args.format == 'json':
        output_file = epub_parser.export_to_json(args.output)
    else:
        output_file = epub_parser.export_to_txt(
            args.output,
            include_metadata=not args.no_metadata,
            include_toc=not args.no_toc
        )
    
    if output_file:
        logger.success(f"处理完成！输出文件: {output_file}")
        
        # 显示统计信息
        stats = epub_parser.get_statistics()
        print("\n" + "=" * 60)
        print("解析统计")
        print("=" * 60)
        print(f"书名: {epub_parser.metadata.get('title', 'Unknown')}")
        print(f"作者: {epub_parser.metadata.get('author', 'Unknown')}")
        print(f"章节数: {stats['chapter_count']}")
        print(f"总字数: {stats['total_words']:,}")
        print(f"平均章节长度: {stats['avg_chapter_length']:,}")
        print("=" * 60)


if __name__ == '__main__':
    main()

