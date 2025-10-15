import os
import re
import subprocess

def get_all_subdirectories(base_path):
    """
    收集一个文件夹下所有子文件夹的名称
    
    参数:
        base_path: 基础路径
    
    返回:
        子文件夹名称的列表
    """
    subdirectories = []
    
    # 检查路径是否存在
    if not os.path.exists(base_path):
        print(f"路径不存在: {base_path}")
        return subdirectories
    
    # 遍历文件夹
    try:
        for item in os.listdir(base_path):
            item_path = os.path.join(base_path, item)
            # 只添加文件夹，不添加文件
            if os.path.isdir(item_path):
                subdirectories.append(item)
    except Exception as e:
        print(f"读取文件夹时出错: {e}")

    subdirectories = sorted(subdirectories)
    print(subdirectories)
    for subdirectory in subdirectories:
        subprocess.run(["python", "douban_book_spider.py", "-n", subdirectory, "-m", "10000"])

if __name__ == "__main__":
    base_path = "./output"
    subdirectories = get_all_subdirectories(base_path)
    print(subdirectories)