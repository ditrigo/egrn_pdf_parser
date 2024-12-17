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
        self.worker_thread = None

    def init_ui(self):
        self.setWindowTitle('Парсер выписок из ЕГРН')
        self.setFixedSize(600, 400)

        self.xml_dir_label = QtWidgets.QLabel('Директория с XML-файлами:')
        self.xml_dir = QtWidgets.QLineEdit()
        self.browse_xml_dir_button = QtWidgets.QPushButton('Обзор')
        self.browse_xml_dir_button.clicked.connect(self.browse_xml_dir)

        self.output_dir_label = QtWidgets.QLabel('Директория для вывода:')
        self.output_dir = QtWidgets.QLineEdit()
        self.browse_output_dir_button = QtWidgets.QPushButton('Обзор')
        self.browse_output_dir_button.clicked.connect(self.browse_output_dir)

        self.run_button = QtWidgets.QPushButton('Запустить парсер')
        self.run_button.clicked.connect(self.run_parser)

        self.log_output = QtWidgets.QTextEdit()
        self.log_output.setReadOnly(True)

        main_layout = QtWidgets.QVBoxLayout()

        xml_layout = QtWidgets.QHBoxLayout()
        xml_layout.addWidget(self.xml_dir_label)
        xml_layout.addWidget(self.xml_dir)
        xml_layout.addWidget(self.browse_xml_dir_button)
        main_layout.addLayout(xml_layout)

        output_layout = QtWidgets.QHBoxLayout()
        output_layout.addWidget(self.output_dir_label)
        output_layout.addWidget(self.output_dir)
        output_layout.addWidget(self.browse_output_dir_button)
        main_layout.addLayout(output_layout)

        main_layout.addWidget(self.run_button)

        main_layout.addWidget(self.log_output)

        self.setLayout(main_layout)

    def browse_xml_dir(self):
        """
        Открывает диалоговое окно для выбора директории с XML-файлами.
        """
        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Выберите директорию с XML-файлами")
        if directory:
            self.xml_dir.setText(directory)

    def browse_output_dir(self):
        """
        Открывает диалоговое окно для выбора директории вывода.
        """
        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Выберите директорию для вывода")
        if directory:
            self.output_dir.setText(directory)

    def run_parser(self):
        """
        Сбор параметров и запуск парсера.
        """
        xml_directory = self.xml_dir.text().strip()
        output_directory = self.output_dir.text().strip()

        if not xml_directory:
            QtWidgets.QMessageBox.critical(self, 'Ошибка', "Пожалуйста, выберите директорию с XML-файлами.")
            return

        if not os.path.isdir(xml_directory):
            QtWidgets.QMessageBox.critical(self, 'Ошибка', "Указанная директория с XML-файлами не существует.")
            return

        if not output_directory:
            QtWidgets.QMessageBox.critical(self, 'Ошибка', "Пожалуйста, выберите директорию для вывода.")
            return

        try:
            os.makedirs(output_directory, exist_ok=True)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, 'Ошибка', f"Не удалось создать директорию для вывода: {e}")
            return

        base_name = 'restrict_records'
        output_csv = os.path.join(output_directory, f"{base_name}.csv")
        output_xlsx = os.path.join(output_directory, f"{base_name}.xlsx")
        log_file = os.path.join(output_directory, 'parser.log')

        self.setup_logging(log_file)

        self.run_button.setEnabled(False)
        self.run_button.setText('Парсинг...')

        self.worker = ParserWorker(
            db_config={
                'type': 'sqlite',
                'sqlite_path': os.path.join(output_directory, 'egrn_database.sqlite')
            },
            xml_directory=xml_directory,
            output_csv=output_csv,
            output_xlsx=output_xlsx,
            log_file=log_file
        )
        self.worker_thread = QtCore.QThread()
        self.worker.moveToThread(self.worker_thread)

        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_parser_finished)
        self.worker.error.connect(self.on_parser_error)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.error.connect(self.worker_thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.worker.error.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)

        self.worker_thread.start()

    def on_parser_finished(self):
        """
        Обработчик сигнала завершения парсинга.
        """
        self.run_button.setEnabled(True)
        self.run_button.setText('Запустить парсер')
        QtWidgets.QMessageBox.information(self, 'Успех', 'Парсинг завершен успешно.')

    def on_parser_error(self, error_message):
        """
        Обработчик сигнала ошибки парсинга.
        """
        self.run_button.setEnabled(True)
        self.run_button.setText('Запустить парсер')
        QtWidgets.QMessageBox.critical(self, 'Ошибка', f"Произошла ошибка: {error_message}")

    def setup_logging(self, log_file):
        """
        Настройка логирования для GUI.
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
        logger.addHandler(console_handler)

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


class ParserWorker(QtCore.QObject):
    """
    Рабочий объект для выполнения парсинга в отдельном потоке.
    """
    finished = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)

    def __init__(self, db_config, xml_directory, output_csv, output_xlsx, log_file):
        super().__init__()
        self.db_config = db_config
        self.xml_directory = xml_directory
        self.output_csv = output_csv
        self.output_xlsx = output_xlsx
        self.log_file = log_file

    @QtCore.pyqtSlot()
    def run(self):
        """
        Метод, выполняемый в отдельном потоке.
        """
        try:
            parser = EGRNParser(
                db_config=self.db_config,
                xml_directory=self.xml_directory,
                output_csv=self.output_csv,
                output_xlsx=self.output_xlsx,
                log_file=self.log_file
            )
            parser.run()
            logging.info("Парсинг завершен успешно.")
            self.finished.emit()
        except Exception as e:
            logging.error(f"Произошла ошибка: {e}")
            self.error.emit(str(e))


def main():
    app = QtWidgets.QApplication(sys.argv)
    parser_app = ParserApp()
    parser_app.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
