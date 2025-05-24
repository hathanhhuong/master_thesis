from logger.logger import Logger, LogType
from utils.constants import LOG_FILE_BASE


def main():
    my_logger = Logger(file_name=LOG_FILE_BASE)
    my_logger.log_info("START")


if __name__ == "__main__":
    main()
