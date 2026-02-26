import logging
import os
import sys

def get_scraper_logger():
    """
    Configures and returns a logger named 'rfp_scraper_v2'.

    - FileHandler: Writes DEBUG+ logs to 'scraper_debug.log' in the project root.
    - StreamHandler: Writes INFO+ logs to the console.
    """
    logger = logging.getLogger("rfp_scraper_v2")
    logger.setLevel(logging.DEBUG)

    # Prevent adding handlers multiple times if get_scraper_logger is called repeatedly
    if logger.hasHandlers():
        return logger

    # formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )

    # Determine project root (2 dirs up from here: rfp_scraper_v2/core/ -> rfp_scraper_v2/ -> root)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
    log_file_path = os.path.join(project_root, "scraper_debug.log")

    # File Handler (DEBUG level)
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Stream Handler (INFO level)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger

# Instantiate the logger so it can be easily imported
logger = get_scraper_logger()
