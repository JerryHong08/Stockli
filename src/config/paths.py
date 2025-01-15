import os

# 项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 资源文件路径
RESOURCES_DIR = os.path.join(BASE_DIR, "resources")
ERRORstock_DIR = os.path.join(RESOURCES_DIR, "csv")
CSV_DIR = os.path.join(RESOURCES_DIR, "csv")
ICONS_DIR = os.path.join(RESOURCES_DIR, "icons")
# 日志文件路径
LOG_PATH = os.path.join(BASE_DIR, "logs")

# Create necessary directories if they don't exist
for directory in [RESOURCES_DIR, ERRORstock_DIR, CSV_DIR, ICONS_DIR, LOG_PATH]:
    if not os.path.exists(directory):
        os.makedirs(directory, mode=0o777, exist_ok=True)

# 具体文件路径
STOCK_LIST_PATH = os.path.join(CSV_DIR, "stock_list.csv")
REFRESH_ICON_PATH = os.path.join(ICONS_DIR, "refresh_icon.png")
DOWNLOAD_ICON_PATH = os.path.join(ICONS_DIR, "download_icon.png")
ERRORstock_PATH = os.path.join(ERRORstock_DIR, "error_log.csv")