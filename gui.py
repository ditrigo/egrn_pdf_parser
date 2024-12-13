# gui.py

import os
import logging
import sys
from PyQt5 import QtWidgets, QtCore

from parser import EGRNParser


class ParserApp(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle('Парсер выписок из ЕГРН')
        self.setFixedSize(800, 600)  # Увеличенная ширина

        # Создание всех необходимых виджетов
        self.db_type_label = QtWidgets.QLabel('Тип базы данных:')
        self.db_type_combo = QtWidgets.QComboBox()
        self.db_type_combo.addItems(['sqlite', 'postgres'])  # По умолчанию sqlite
        self.db_type_combo.currentTextChanged.connect(self.toggle_db_fields)

        # Поля для PostgreSQL
        self.pg_group = QtWidgets.QGroupBox('PostgreSQL Параметры')
        self.pg_layout = QtWidgets.QFormLayout()
        self.pg_host = QtWidgets.QLineEdit()
        self.pg_port = QtWidgets.QLineEdit()
        self.pg_name = QtWidgets.QLineEdit()
        self.pg_user = QtWidgets.QLineEdit()
        self.pg_password = QtWidgets.QLineEdit()
        self.pg_password.setEchoMode(QtWidgets.QLineEdit.Password)

        self.pg_layout.addRow('Хост:', self.pg_host)
        self.pg_layout.addRow('Порт:', self.pg_port)
        self.pg_layout.addRow('Название БД:', self.pg_name)
        self.pg_layout.addRow('Пользователь:', self.pg_user)
        self.pg_layout.addRow('Пароль:', self.pg_password)
        self.pg_group.setLayout(self.pg_layout)

        # Поля для SQLite
        self.sqlite_group = QtWidgets.QGroupBox('SQLite Параметры')
        self.sqlite_layout = QtWidgets.QFormLayout()
        self.sqlite_path = QtWidgets.QLineEdit('egrn_database.sqlite')  # Стандартное имя файла
        self.sqlite_browse = QtWidgets.QPushButton('Обзор')
        self.sqlite_browse.clicked.connect(self.browse_sqlite)

        sqlite_hbox = QtWidgets.QHBoxLayout()
        sqlite_hbox.addWidget(self.sqlite_path)
        sqlite_hbox.addWidget(self.sqlite_browse)

        self.sqlite_layout.addRow('Путь к файлу:', sqlite_hbox)
        self.sqlite_group.setLayout(self.sqlite_layout)
        self.sqlite_group.show()  # Показать по умолчанию
        self.pg_group.hide()

        # Остальные поля
        self.xml_dir_label = QtWidgets.QLabel('Директория с XML-файлами:')
        self.xml_dir = QtWidgets.QLineEdit('xml_files')  # Стандартная директория
        self.xml_browse = QtWidgets.QPushButton('Обзор')
        self.xml_browse.clicked.connect(self.browse_xml_dir)

        xml_hbox = QtWidgets.QHBoxLayout()
        xml_hbox.addWidget(self.xml_dir)
        xml_hbox.addWidget(self.xml_browse)

        self.output_csv_label = QtWidgets.QLabel('Выходной CSV файл:')
        self.output_csv = QtWidgets.QLineEdit('output/restrict_records.csv')  # Стандартный путь
        self.csv_browse = QtWidgets.QPushButton('Обзор')
        self.csv_browse.clicked.connect(self.browse_output_csv)

        csv_hbox = QtWidgets.QHBoxLayout()
        csv_hbox.addWidget(self.output_csv)
        csv_hbox.addWidget(self.csv_browse)

        self.output_xlsx_label = QtWidgets.QLabel('Выходной XLSX файл:')
        self.output_xlsx = QtWidgets.QLineEdit('output/restrict_records.xlsx')  # Стандартный путь
        self.xlsx_browse = QtWidgets.QPushButton('Обзор')
        self.xlsx_browse.clicked.connect(self.browse_output_xlsx)

        xlsx_hbox = QtWidgets.QHBoxLayout()
        xlsx_hbox.addWidget(self.output_xlsx)
        xlsx_hbox.addWidget(self.xlsx_browse)

        self.log_file_label = QtWidgets.QLabel('Файл логов:')
        self.log_file = QtWidgets.QLineEdit('parser.log')  # Стандартный файл логов
        self.log_browse = QtWidgets.QPushButton('Обзор')
        self.log_browse.clicked.connect(self.browse_log_file)

        log_hbox = QtWidgets.QHBoxLayout()
        log_hbox.addWidget(self.log_file)
        log_hbox.addWidget(self.log_browse)

        # Кнопка запуска
        self.run_button = QtWidgets.QPushButton('Запустить парсер')
        self.run_button.clicked.connect(self.run_parser)

        # Поле для отображения логов
        self.log_output = QtWidgets.QTextEdit()
        self.log_output.setReadOnly(True)

        # Расположение элементов в основном макете
        main_layout = QtWidgets.QVBoxLayout()

        db_layout = QtWidgets.QHBoxLayout()
        db_layout.addWidget(self.db_type_label)
        db_layout.addWidget(self.db_type_combo)
        main_layout.addLayout(db_layout)

        main_layout.addWidget(self.pg_group)
        main_layout.addWidget(self.sqlite_group)

        main_layout.addWidget(self.xml_dir_label)
        main_layout.addLayout(xml_hbox)

        main_layout.addWidget(self.output_csv_label)
        main_layout.addLayout(csv_hbox)

        main_layout.addWidget(self.output_xlsx_label)
        main_layout.addLayout(xlsx_hbox)

        main_layout.addWidget(self.log_file_label)
        main_layout.addLayout(log_hbox)

        main_layout.addWidget(self.run_button)
        main_layout.addWidget(QtWidgets.QLabel('Логи:'))
        main_layout.addWidget(self.log_output)

        self.setLayout(main_layout)

    def toggle_db_fields(self, text: str):
        """
        Переключает видимость полей в зависимости от выбранного типа базы данных.
        """
        if text == 'postgres':
            self.pg_group.show()
            self.sqlite_group.hide()
        elif text == 'sqlite':
            self.pg_group.hide()
            self.sqlite_group.show()

    def browse_sqlite(self):
        """
        Открывает диалог выбора файла для SQLite.
        """
        file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            'Выберите файл SQLite',
            '',
            'SQLite Files (*.sqlite *.db);;All Files (*)'
        )
        if file_path:
            self.sqlite_path.setText(file_path)

    def browse_xml_dir(self):
        """
        Открывает диалог выбора директории с XML-файлами.
        """
        dir_path = QtWidgets.QFileDialog.getExistingDirectory(
            self,
            'Выберите директорию с XML-файлами'
        )
        if dir_path:
            self.xml_dir.setText(dir_path)

    def browse_output_csv(self):
        """
        Открывает диалог выбора выходного CSV файла.
        """
        file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            'Выберите выходной CSV файл',
            '',
            'CSV Files (*.csv);;All Files (*)'
        )
        if file_path:
            self.output_csv.setText(file_path)

    def browse_output_xlsx(self):
        """
        Открывает диалог выбора выходного XLSX файла.
        """
        file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            'Выберите выходной XLSX файл',
            '',
            'Excel Files (*.xlsx);;All Files (*)'
        )
        if file_path:
            self.output_xlsx.setText(file_path)

    def browse_log_file(self):
        """
        Открывает диалог выбора файла логов.
        """
        file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            'Выберите файл логов',
            '',
            'Log Files (*.log);;All Files (*)'
        )
        if file_path:
            self.log_file.setText(file_path)

    def run_parser(self):
        """
        Сбор параметров и запуск парсера.
        """
        # Сбор всех параметров
        db_type = self.db_type_combo.currentText()
        db_config = {'type': db_type}
        if db_type == 'postgres':
            db_config.update({
                'host': self.pg_host.text(),
                'port': self.pg_port.text(),
                'database': self.pg_name.text(),
                'user': self.pg_user.text(),
                'password': self.pg_password.text()
            })
            # Проверка обязательных полей
            required_pg_fields = ['host', 'port', 'database', 'user', 'password']
            missing = [field for field in required_pg_fields if not db_config.get(field)]
            if missing:
                QtWidgets.QMessageBox.critical(
                    self,
                    'Ошибка',
                    f"Отсутствуют обязательные поля для PostgreSQL: {', '.join(missing)}"
                )
                return
        elif db_type == 'sqlite':
            db_config.update({
                'sqlite_path': self.sqlite_path.text() or 'egrn_database.sqlite'
            })
            if not db_config['sqlite_path']:
                QtWidgets.QMessageBox.critical(
                    self,
                    'Ошибка',
                    "Отсутствует путь к файлу SQLite."
                )
                return

        xml_directory = self.xml_dir.text() or 'xml_files'
        if not os.path.isdir(xml_directory):
            # Создание директории, если не существует
            try:
                os.makedirs(xml_directory, exist_ok=True)
                logging.info(f"Создана директория для XML-файлов: {xml_directory}")
            except Exception as e:
                QtWidgets.QMessageBox.critical(
                    self,
                    'Ошибка',
                    f"Не удалось создать директорию {xml_directory}: {e}"
                )
                return

        output_csv = self.output_csv.text() or 'output/restrict_records.csv'
        output_xlsx = self.output_xlsx.text() or 'output/restrict_records.xlsx'
        log_file = self.log_file.text() or 'parser.log'

        # Проверка и создание директорий для выходных файлов
        try:
            for path in [output_csv, output_xlsx, log_file]:
                dir_name = os.path.dirname(path)
                if dir_name and not os.path.exists(dir_name):
                    os.makedirs(dir_name, exist_ok=True)
                    logging.info(f"Создана директория для {path}: {dir_name}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(
                self,
                'Ошибка',
                f"Не удалось создать директорию для выходных файлов: {e}"
            )
            return

        # Настройка логирования в GUI
        self.setup_logging(log_file)

        # Запуск парсера
        self.run_button.setEnabled(False)
        self.run_button.setText('Парсинг...')

        QtCore.QThreadPool.globalInstance().start(
            WorkerThread(
                self.execute_parser,
                db_config,
                xml_directory,
                output_csv,
                output_xlsx,
                log_file
            )
        )

    def execute_parser(self, db_config, xml_directory, output_csv, output_xlsx, log_file):
        """
        Выполняет парсинг в отдельном потоке.
        """
        try:
            parser = EGRNParser(
                db_config=db_config,
                xml_directory=xml_directory,
                output_csv=output_csv,
                output_xlsx=output_xlsx,
                log_file=log_file
            )
            parser.run()
            logging.info("Парсинг завершен успешно.")
            QtWidgets.QMessageBox.information(self, 'Успех', 'Парсинг завершен успешно.')
        except Exception as e:
            logging.error(f"Произошла ошибка: {e}")
            QtWidgets.QMessageBox.critical(self, 'Ошибка', f"Произошла ошибка: {e}")
        finally:
            self.run_button.setEnabled(True)
            self.run_button.setText('Запустить парсер')

    def setup_logging(self, log_file: str):
        """
        Настройка логирования для GUI.
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        # Удаление всех обработчиков, чтобы избежать дублирования
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        # Создание обработчика для файла
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
        logger.addHandler(file_handler)
        # Создание обработчика для консоли
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
        logger.addHandler(console_handler)
        # Создание обработчика для вывода в QTextEdit
        text_handler = QTextEditLogger(self.log_output)
        text_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
        logger.addHandler(text_handler)


class QTextEditLogger(logging.Handler):
    """
    Логгер, который выводит логи в QTextEdit.
    """
    def __init__(self, text_edit: QtWidgets.QTextEdit):
        super().__init__()
        self.text_edit = text_edit

    def emit(self, record):
        msg = self.format(record)
        QtCore.QMetaObject.invokeMethod(
            self.text_edit,
            "append",
            QtCore.Qt.QueuedConnection,
            QtCore.Q_ARG(str, msg)
        )


class WorkerThread(QtCore.QRunnable):
    """
    Рабочий поток для выполнения парсинга без блокировки GUI.
    """
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    @QtCore.pyqtSlot()
    def run(self):
        self.fn(*self.args, **self.kwargs)
