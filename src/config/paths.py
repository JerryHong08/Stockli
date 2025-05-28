import os

def get_project_root():
    current_dir = os.path.abspath(__file__)
    while not os.path.exists(os.path.join(current_dir, "README.md")):  # 假设根目录有 README.md
        current_dir = os.path.dirname(current_dir)
        if current_dir == os.path.dirname(current_dir):  # 到达文件系统根目录
            raise Exception("无法找到项目根目录")
    return current_dir

# 项目根目录
BASE_DIR = get_project_root()

# 资源文件路径
RESOURCES_DIR = os.path.join(BASE_DIR, "resources")
ERRORstock_DIR = os.path.join(RESOURCES_DIR, "csv")
CSV_DIR = os.path.join(RESOURCES_DIR, "csv")
ICONS_DIR = os.path.join(RESOURCES_DIR, "icons")
# 日志文件路径
LOG_PATH = os.path.join(BASE_DIR, "logs")

# 创建必要的目录
for directory in [RESOURCES_DIR, ERRORstock_DIR, CSV_DIR, ICONS_DIR, LOG_PATH]:
    if not os.path.exists(directory):
        os.makedirs(directory, mode=0o777, exist_ok=True)

# 具体文件路径
STOCK_LIST_PATH = os.path.join(CSV_DIR, "stock_list.csv")
REFRESH_ICON_PATH = os.path.join(ICONS_DIR, "refresh_icon.png")
DOWNLOAD_ICON_PATH = os.path.join(ICONS_DIR, "download_icon.png")
ERRORstock_PATH = os.path.join(ERRORstock_DIR, "error_log_enriched_errorout.csv")