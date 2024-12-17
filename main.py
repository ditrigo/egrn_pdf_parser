import argparse
import logging
import os
import sys
from typing import Any, Dict

from PyQt5 import QtWidgets

from gui import ParserApp
from parser import EGRNParser


def parse_arguments() -> argparse.Namespace:
    """
    Парсинг аргументов командной строки.
    """
    parser = argparse.ArgumentParser(
        description="Парсер выписок из ЕГРН."
    )
    parser.add_argument(
        '--db_type',
        type=str,
        choices=['postgres', 'sqlite'],
        default='sqlite',
        help='Тип базы данных: postgres или sqlite (по умолчанию: sqlite).'
    )
    parser.add_argument(
        '--db_host',
        type=str,
        help='Хост PostgreSQL.'
    )
    parser.add_argument(
        '--db_port',
        type=int,
        help='Порт PostgreSQL.'
    )
    parser.add_argument(
        '--db_name',
        type=str,
        help='Название базы данных PostgreSQL.'
    )
    parser.add_argument(
        '--db_user',
        type=str,
        help='Пользователь PostgreSQL.'
    )
    parser.add_argument(
        '--db_password',
        type=str,
        help='Пароль PostgreSQL.'
    )
    parser.add_argument(
        '--sqlite_path',
        type=str,
        default='egrn_database.sqlite',
        help='Путь к файлу SQLite (по умолчанию: ./egrn_database.sqlite).'
    )
    parser.add_argument(
        '--xml_directory',
        type=str,
        default='xml_files',
        help='Путь к директории с XML-файлами (по умолчанию: ./xml_files).'
    )
    parser.add_argument(
        '--output_csv',
        type=str,
        default='output/restrict_records.csv',
        help='Путь к выходному CSV файлу (по умолчанию: output/restrict_records.csv).'
    )
    parser.add_argument(
        '--output_xlsx',
        type=str,
        default='output/restrict_records.xlsx',
        help='Путь к выходному XLSX файлу (по умолчанию: output/restrict_records.xlsx).'
    )
    parser.add_argument(
        '--log_file',
        type=str,
        default='parser.log',
        help='Путь к файлу логов (по умолчанию: parser.log).'
    )
    return parser.parse_args()


def setup_logging(log_file: str) -> None:
    """
    Настройка логирования для приложения.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def create_output_directories(paths: list) -> None:
    """
    Создаёт необходимые директории для выходных файлов, если они не существуют.
    """
    for path in paths:
        dir_name = os.path.dirname(path)
        if dir_name and not os.path.exists(dir_name):
            logging.info(f"Создание директории для {path}: {dir_name}")
            os.makedirs(dir_name, exist_ok=True)


def get_db_config(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Формирует конфигурацию базы данных на основе аргументов.
    """
    if args.db_type == 'postgres':
        required_fields = ['db_host', 'db_port', 'db_name', 'db_user', 'db_password']
        missing = [field for field in required_fields if not getattr(args, field)]
        if missing:
            logging.error(f"Отсутствуют необходимые параметры для PostgreSQL: {', '.join(missing)}")
            sys.exit(1)
        return {
            'type': 'postgres',
            'host': args.db_host,
            'port': args.db_port,
            'database': args.db_name,
            'user': args.db_user,
            'password': args.db_password
        }
    elif args.db_type == 'sqlite':
        return {
            'type': 'sqlite',
            'sqlite_path': args.sqlite_path or 'egrn_database.sqlite'
        }
    else:
        logging.error("Unsupported database type.")
        sys.exit(1)


def run_cli(args: argparse.Namespace) -> None:
    """
    Запуск парсера в CLI-режиме.
    """
    logging.info("Запуск парсера в CLI-режиме.")

    db_config = get_db_config(args)

    create_output_directories([
        args.output_csv, args.output_xlsx,
        args.log_file
    ])

    try:
        parser = EGRNParser(
            db_config=db_config,
            xml_directory=args.xml_directory,
            output_csv=args.output_csv,
            output_xlsx=args.output_xlsx,
            log_file=args.log_file
        )
        parser.run()
        logging.info("Парсинг завершен успешно.")
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")
        sys.exit(1)


def run_gui():
    """
    Запуск GUI-приложения.
    """
    app = QtWidgets.QApplication(sys.argv)
    window = ParserApp()
    window.show()
    sys.exit(app.exec_())


def main():
    """
    Основная функция, которая определяет режим работы приложения.
    """
    args = parse_arguments()
    # setup_logging(args.log_file)

    if len(sys.argv) > 1:
        # Режим CLI
        run_cli(args)
    else:
        # Режим GUI
        run_gui()


if __name__ == "__main__":
    main()
