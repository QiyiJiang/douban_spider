import os
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
    
    return sorted(subdirectories)


if __name__ == "__main__":
    base_path = "./output"
    
    # 获取所有已存在的书籍文件夹名称
    subdirectories = get_all_subdirectories(base_path)
    
    if not subdirectories:
        print("❌ 未找到任何书籍文件夹")
        exit(1)
    
    print(f"找到 {len(subdirectories)} 本书:")
    for i, book in enumerate(subdirectories[:10], 1):
        print(f"  {i}. {book}")
    if len(subdirectories) > 10:
        print(f"  ... 还有 {len(subdirectories) - 10} 本")
    
    # 构建命令（关键修正：展开列表）
    cmd = [
        "python", "douban_book_spider.py",
        "-n"
    ] + subdirectories + [
        "-m", "10000",
        "-w", "10"
    ] + [
        "--use_proxy",
        "--proxy_file", "proxies.txt"
    ]
    
    print(f"\n{'='*60}")
    print("即将执行爬取任务（增量更新）...")
    print(f"{'='*60}\n")
    
    # 执行爬取
    try:
        subprocess.run(cmd, check=True)
        print("\n✅ 爬取任务完成！")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ 爬取失败: {e}")
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断爬取")