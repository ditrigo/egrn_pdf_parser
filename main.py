# main.py

import argparse
import logging
import os
import sys
from typing import Any, Dict, Optional

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
        default='sqlite',  # По умолчанию sqlite
        help='Тип базы данных: postgres или sqlite (по умолчанию: sqlite).'
    )
    # Параметры для PostgreSQL
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
    # Параметры для SQLite
    parser.add_argument(
        '--sqlite_path',
        type=str,
        default='egrn_database.sqlite',  # Стандартное имя файла
        help='Путь к файлу SQLite (по умолчанию: ./egrn_database.sqlite).'
    )
    # Общие параметры
    parser.add_argument(
        '--xml_directory',
        type=str,
        default='xml_files',  # Стандартная директория
        help='Путь к директории с XML-файлами (по умолчанию: ./xml_files).'
    )
    parser.add_argument(
        '--output_csv',
        type=str,
        default='output/restrict_records.csv',  # Стандартный путь
        help='Путь к выходному CSV файлу (по умолчанию: output/restrict_records.csv).'
    )
    parser.add_argument(
        '--output_xlsx',
        type=str,
        default='output/restrict_records.xlsx',  # Стандартный путь
        help='Путь к выходному XLSX файлу (по умолчанию: output/restrict_records.xlsx).'
    )
    parser.add_argument(
        '--special_output_csv',
        type=str,
        default='output/special_restrict_records.csv',  # Стандартный путь для специальных записей
        help='Путь к выходному CSV файлу для специальных записей (по умолчанию: output/special_restrict_records.csv).'
    )
    parser.add_argument(
        '--special_output_xlsx',
        type=str,
        default='output/special_restrict_records.xlsx',  # Стандартный путь для специальных записей
        help='Путь к выходному XLSX файлу для специальных записей (по умолчанию: output/special_restrict_records.xlsx).'
    )
    parser.add_argument(
        '--log_file',
        type=str,
        default='parser.log',  # Стандартный файл логов
        help='Путь к файлу логов (по умолчанию: parser.log).'
    )
    return parser.parse_args()


def run_cli(args: argparse.Namespace) -> None:
    """
    Запуск парсера в CLI-режиме.
    """
    # Настройка логирования
    logging.basicConfig(
        filename=args.log_file,
        filemode='a',
        format='%(asctime)s %(levelname)s:%(message)s',
        level=logging.INFO
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    logging.info("Запуск парсера в CLI-режиме.")

    # Настройка параметров подключения к базе данных
    db_config = {}
    if args.db_type == 'postgres':
        required_fields = ['db_host', 'db_port', 'db_name', 'db_user', 'db_password']
        missing = [field for field in required_fields if not getattr(args, field)]
        if missing:
            logging.error(f"Отсутствуют необходимые параметры для PostgreSQL: {', '.join(missing)}")
            sys.exit(1)
        db_config = {
            'type': 'postgres',
            'host': args.db_host,
            'port': args.db_port,
            'database': args.db_name,
            'user': args.db_user,
            'password': args.db_password
        }
    elif args.db_type == 'sqlite':
        db_config = {
            'type': 'sqlite',
            'sqlite_path': args.sqlite_path or 'egrn_database.sqlite'
        }

    # Убедитесь, что директория xml_directory существует
    if not os.path.isdir(args.xml_directory):
        logging.info(f"Директория {args.xml_directory} не существует. Создание...")
        os.makedirs(args.xml_directory, exist_ok=True)

    # Убедитесь, что директории для выходных файлов существуют
    for path in [args.output_csv, args.output_xlsx, args.special_output_csv, args.special_output_xlsx, args.log_file]:
        dir_name = os.path.dirname(path)
        if dir_name and not os.path.exists(dir_name):
            logging.info(f"Создание директории для {path}: {dir_name}")
            os.makedirs(dir_name, exist_ok=True)

    try:
        parser = EGRNParser(
            # db_config=db_config,
            xml_directory=args.xml_directory,
            output_csv=args.output_csv,
            output_xlsx=args.output_xlsx,
            special_output_csv=args.special_output_csv,
            special_output_xlsx=args.special_output_xlsx,
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
    if len(sys.argv) > 1:
        # Режим CLI
        args = parse_arguments()
        run_cli(args)
    else:
        # Режим GUI
        run_gui()


if __name__ == "__main__":
    main()
