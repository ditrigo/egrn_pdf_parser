# parser.py

import os
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from lxml import etree
from sqlalchemy import create_engine, Column, String, DateTime, Integer, ForeignKey, Float
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import pandas as pd
import json

# Обновленный импорт declarative_base
Base = declarative_base()

# ORM Модели

class MainRecord(Base):
    __tablename__ = 'main_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # details_statement fields
    organ_registr_rights = Column(String, nullable=True)
    date_formation = Column(DateTime, nullable=True)
    registration_number = Column(String, nullable=True)
    # details_request fields
    date_received_request = Column(DateTime, nullable=True)
    date_receipt_request_reg_authority_rights = Column(DateTime, nullable=True)
    # land_record fields
    cad_number = Column(String, nullable=True)  # Кадастровый номер ЗУ
    readable_address = Column(String, nullable=True)  # Местоположение ЗУ
    purpose_code = Column(String, nullable=True)  # Код категории земель ЗУ
    purpose_value = Column(String, nullable=True)  # Категория земель ЗУ
    permitted_use_code = Column(String, nullable=True)  # Код вида разрешенного использования ЗУ
    permitted_use_established = Column(String, nullable=True)  # Вид(ы) разрешенного использования ЗУ
    area = Column(Float, nullable=True)  # Площадь ЗУ (кв. м)

    # Связи с другими таблицами
    right_records = relationship('RightRecord', back_populates='main_record', cascade='all, delete-orphan')
    restrict_records = relationship('RestrictRecord', back_populates='main_record', cascade='all, delete-orphan')
    deal_records = relationship('DealRecord', back_populates='main_record', cascade='all, delete-orphan')

    # Метаданные
    source_file = Column(String, nullable=True)
    parsed_at = Column(DateTime, default=datetime.utcnow)


class RightRecord(Base):
    __tablename__ = 'right_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    main_record_id = Column(Integer, ForeignKey('main_records.id'), nullable=False)
    registration_date = Column(DateTime, nullable=True)  # Дата государственной регистрации права
    right_number = Column(String, nullable=True)  # Номер государственной регистрации права
    right_type_code = Column(String, nullable=True)  # Код вида государственной регистрации права
    right_type = Column(String, nullable=True)  # Вид государственной регистрации права
    holders = Column(String, nullable=True)  # Правообладатели ЗУ (строка JSON)
    documents = Column(String, nullable=True)  # Документы (строка JSON)

    main_record = relationship('MainRecord', back_populates='right_records')


class RestrictRecord(Base):
    __tablename__ = 'restrict_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    main_record_id = Column(Integer, ForeignKey('main_records.id'), nullable=False)
    restriction_number = Column(String, nullable=True)  # Номер обременения/ограничения
    restriction_type_code = Column(String, nullable=True)  # Код вида обременения/ограничения
    restriction_type = Column(String, nullable=True)  # Вид обременения/ограничения
    start_date = Column(DateTime, nullable=True)  # Начало срока обременения / ограничения
    end_date = Column(DateTime, nullable=True)  # Конец срока обременения / ограничения
    deal_validity_time = Column(String, nullable=True)  # Сделка действительна время
    transfer_deadline = Column(String, nullable=True)  # Признак ипотеки
    guarantee_period = Column(String, nullable=True)  # Срок обременения / ограничения
    bank = Column(String, nullable=True)  # Банк
    bank_inn = Column(String, nullable=True)  # ИНН Банка
    registration_date = Column(DateTime, nullable=True)  # Дата государственной регистрации обременения / ограничения
    documents = Column(String, nullable=True)  # Документы (строка JSON)
    deal_number = Column(String, nullable=True)  # Номер сделки для сопоставления

    main_record = relationship('MainRecord', back_populates='restrict_records')


class DealRecord(Base):
    __tablename__ = 'deal_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    main_record_id = Column(Integer, ForeignKey('main_records.id'), nullable=False)
    deal_number = Column(String, nullable=True)  # Номер сделки
    deal_type_code = Column(String, nullable=True)
    deal_type_value = Column(String, nullable=True)
    first_ddu_date = Column(DateTime, nullable=True)  # Дата заключения ДДУ
    room_name = Column(String, nullable=True)  # Тип объекта ДДУ
    room_number = Column(String, nullable=True)  # Условный номер объекта ДДУ
    floor_number = Column(Integer, nullable=True)  # Этаж расположения объекта ДДУ
    room_area = Column(Float, nullable=True)  # Площадь объекта ДДУ (кв. м)
    bank = Column(String, nullable=True)  # Банк
    bank_inn = Column(String, nullable=True)  # ИНН Банка
    guarantee_period = Column(String, nullable=True)  # Срок обременения / ограничения
    transfer_deadline = Column(String, nullable=True)  # Признак ипотеки
    documents = Column(String, nullable=True)  # Документы (строка JSON)

    main_record = relationship('MainRecord', back_populates='deal_records')


# Класс Парсера

class EGRNParser:
    def __init__(self, db_config: Dict[str, Any], xml_directory: str,
                 output_csv: str, output_xlsx: str,
                 log_file: str = 'parser.log'):
        self.db_config = db_config
        self.xml_directory = xml_directory
        self.output_csv = output_csv
        self.output_xlsx = output_xlsx
        self.log_file = log_file

        # Настройка логирования (файл и консоль)
        self.logger = logging.getLogger('EGRNParser')
        self.logger.setLevel(logging.INFO)

        # Создание форматера
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        # Файловый обработчик
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Консольный обработчик
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        self.logger.info("Инициализация парсера начата.")

        # Инициализация базы данных
        self.Session = self.init_db()

    def init_db(self) -> sessionmaker:
        """
        Инициализирует соединение с базой данных и создаёт все таблицы.
        """
        if self.db_config['type'] == 'postgres':
            user = self.db_config['user']
            password = self.db_config['password']
            host = self.db_config['host']
            port = self.db_config['port']
            database = self.db_config['database']
            connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        elif self.db_config['type'] == 'sqlite':
            sqlite_path = self.db_config['sqlite_path']
            connection_string = f'sqlite:///{sqlite_path}'
        else:
            self.logger.error("Unsupported database type.")
            raise ValueError("Unsupported database type.")

        engine = create_engine(connection_string, echo=False)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.logger.info("Соединение с базой данных установлено и таблицы созданы.")
        return Session

    def serialize_datetime(self, obj: Any) -> Any:
        """
        Рекурсивно сериализует объекты datetime в строки.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, list):
            return [self.serialize_datetime(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self.serialize_datetime(value) for key, value in obj.items()}
        else:
            return obj

    def parse_xml(self, file_path: str, session):
        """
        Парсит XML-файл и сохраняет данные в базу данных.
        """
        self.logger.info(f"Начало парсинга файла: {file_path}")

        try:
            # Используем iterparse для эффективной обработки больших файлов
            context = etree.iterparse(file_path, events=('end',), tag='extract_contract_participation_share_holdings', recover=True, encoding='utf-8')

            for event, elem in context:
                # Извлечение данных из различных разделов XML
                details_statement = self.parse_details_statement(elem)
                details_request = self.parse_details_request(elem)
                land_record = self.parse_land_record(elem)
                right_records = self.parse_right_records(elem)
                restrict_records = self.parse_restrict_records(elem)
                deal_records = self.parse_deal_records(elem)

                # Создание основной записи
                main_record = MainRecord(
                    # Заполнение fields из details_statement
                    organ_registr_rights=details_statement.get('organ_registr_rights'),
                    date_formation=details_statement.get('date_formation'),
                    registration_number=details_statement.get('registration_number'),
                    # Заполнение fields из details_request
                    date_received_request=details_request.get('date_received_request'),
                    date_receipt_request_reg_authority_rights=details_request.get('date_receipt_request_reg_authority_rights'),
                    # Заполнение land_record
                    cad_number=land_record.get('cad_number'),
                    readable_address=land_record.get('readable_address'),
                    purpose_code=land_record.get('purpose_code'),
                    purpose_value=land_record.get('purpose_value'),
                    permitted_use_code=land_record.get('permitted_use_code'),
                    permitted_use_established=land_record.get('permitted_use_established'),
                    area=land_record.get('area'),
                    source_file=os.path.basename(file_path)
                )

                # Добавление RightRecords
                for right in right_records:
                    right_record = RightRecord(
                        registration_date=right.get('registration_date'),
                        right_number=right.get('right_number'),
                        right_type_code=right.get('right_type_code'),
                        right_type=right.get('right_type'),
                        holders=json.dumps(right.get('holders')),  # Сохранение как JSON
                        documents=json.dumps(right.get('documents'))  # Сохранение как JSON
                    )
                    main_record.right_records.append(right_record)

                # Добавление RestrictRecords
                for restrict in restrict_records:
                    restrict_record = RestrictRecord(
                        restriction_number=restrict.get('restriction_number'),
                        restriction_type_code=restrict.get('restriction_type_code'),
                        restriction_type=restrict.get('restriction_type'),
                        start_date=restrict.get('start_date'),
                        end_date=restrict.get('end_date'),
                        deal_validity_time=restrict.get('deal_validity_time'),
                        transfer_deadline=restrict.get('transfer_deadline'),
                        guarantee_period=restrict.get('guarantee_period'),
                        bank=restrict.get('bank'),
                        bank_inn=restrict.get('bank_inn'),
                        registration_date=restrict.get('registration_date'),
                        documents=json.dumps(restrict.get('documents')),  # Сохранение как JSON
                        deal_number=restrict.get('deal_number')  # Добавлено поле для сопоставления
                    )
                    main_record.restrict_records.append(restrict_record)

                # Добавление DealRecords
                for deal in deal_records:
                    deal_record = DealRecord(
                        deal_number=deal.get('deal_number'),
                        deal_type_code=deal.get('deal_type_code'),
                        deal_type_value=deal.get('deal_type_value'),
                        first_ddu_date=deal.get('first_ddu_date'),
                        room_name=deal.get('room_name'),
                        room_number=deal.get('room_number'),
                        floor_number=deal.get('floor_number'),
                        room_area=deal.get('room_area'),
                        bank=deal.get('bank'),
                        bank_inn=deal.get('bank_inn'),
                        guarantee_period=deal.get('guarantee_period'),
                        transfer_deadline=deal.get('transfer_deadline'),
                        documents=json.dumps(deal.get('documents'))  # Сохранение как JSON
                    )
                    main_record.deal_records.append(deal_record)

                # Добавление основной записи в сессию
                session.add(main_record)

                # Очистка обработанного элемента для экономии памяти
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

            # Коммит всех изменений
            session.commit()
            self.logger.info(f"Парсинг и сохранение данных из файла {file_path} завершены успешно.")

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге файла {file_path}: {e}")
            session.rollback()

    def parse_details_statement(self, elem: etree._Element) -> Dict[str, Any]:
        """
        Парсит раздел details_statement из XML и возвращает словарь с данными.
        """
        try:
            details = {}
            details_elem = elem.find('.//details_statement')
            if details_elem is not None:
                details['organ_registr_rights'] = details_elem.findtext('.//organ_registr_rights') or ""
                date_formation_str = details_elem.findtext('.//date_formation') or ""
                try:
                    details['date_formation'] = datetime.strptime(date_formation_str, '%Y-%m-%d') if date_formation_str else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат date_formation: {date_formation_str}")
                    details['date_formation'] = None
                details['registration_number'] = details_elem.findtext('.//registration_number') or ""
            return details
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге details_statement: {e}")
            return {}

    def parse_details_request(self, elem: etree._Element) -> Dict[str, Any]:
        """
        Парсит раздел details_request из XML и возвращает словарь с данными.
        """
        try:
            details = {}
            details_elem = elem.find('.//details_request')
            if details_elem is not None:
                date_received_str = details_elem.findtext('.//date_received_request') or ""
                try:
                    details['date_received_request'] = datetime.strptime(date_received_str, '%Y-%m-%d') if date_received_str else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат date_received_request: {date_received_str}")
                    details['date_received_request'] = None
                date_receipt_str = details_elem.findtext('.//date_receipt_request_reg_authority_rights') or ""
                try:
                    details['date_receipt_request_reg_authority_rights'] = datetime.strptime(date_receipt_str, '%Y-%m-%d') if date_receipt_str else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат date_receipt_request_reg_authority_rights: {date_receipt_str}")
                    details['date_receipt_request_reg_authority_rights'] = None
            return details
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге details_request: {e}")
            return {}

    def parse_land_record(self, elem: etree._Element) -> Dict[str, Any]:
        """
        Парсит раздел land_record из XML и возвращает словарь с данными.
        """
        try:
            land_record = {}
            land_elem = elem.find('.//land_record')

            if land_elem is None:
                self.logger.warning("Раздел land_record отсутствует в XML.")
                return land_record

            # Кадастровый номер ЗУ
            cad_number = land_elem.findtext('.//object/common_data/cad_number')
            land_record['cad_number'] = cad_number.strip() if cad_number else ""

            # Местоположение ЗУ
            readable_address = land_elem.findtext('.//address_location/address/readable_address')
            land_record['readable_address'] = readable_address.strip() if readable_address else ""

            # Категория земель ЗУ
            purpose_code = land_elem.findtext('.//params/category/type/code')
            purpose_value = land_elem.findtext('.//params/category/type/value')
            land_record['purpose_code'] = purpose_code.strip() if purpose_code else ""
            land_record['purpose_value'] = purpose_value.strip() if purpose_value else ""

            # Вид(ы) разрешенного использования ЗУ
            permitted_use_established = land_elem.findtext('.//params/permitted_use/permitted_use_established/by_document')
            land_record['permitted_use_established'] = permitted_use_established.strip() if permitted_use_established else ""

            # Площадь ЗУ (кв. м)
            area_str = land_elem.findtext('.//params/area/value')
            try:
                land_record['area'] = float(area_str.strip()) if area_str else None
            except ValueError:
                self.logger.warning(f"Некорректный формат площади ЗУ: {area_str}")
                land_record['area'] = None

            return land_record

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге land_record: {e}")
            return {}

    def parse_right_records(self, elem: etree._Element) -> List[Dict[str, Any]]:
        """
        Парсит раздел right_records из XML и возвращает список словарей с данными.
        """
        try:
            right_records = []
            right_elems = elem.findall('.//right_records/right_record')
            for right_elem in right_elems:
                record = {}
                # Дата государственной регистрации права
                registration_date_str = right_elem.findtext('.//record_info/registration_date')
                try:
                    registration_date = datetime.fromisoformat(registration_date_str.replace('Z', '+00:00')) if registration_date_str else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат даты регистрации: {registration_date_str}")
                    registration_date = None
                record['registration_date'] = registration_date

                # Номер государственной регистрации права
                right_number = right_elem.findtext('.//right_data/right_number')
                record['right_number'] = right_number.strip() if right_number else ""

                # Вид государственной регистрации права
                restriction_type_code = right_elem.findtext('.//right_data/right_type/code')
                restriction_type = right_elem.findtext('.//right_data/right_type/value')
                record['right_type_code'] = restriction_type_code.strip() if restriction_type_code else ""
                record['right_type'] = restriction_type.strip() if restriction_type else ""

                # Правообладатели ЗУ
                holders = []
                holder_elems = right_elem.findall('.//right_holders/right_holder')
                for holder_elem in holder_elems:
                    name = holder_elem.findtext('.//legal_entity/entity/resident/name')
                    inn = holder_elem.findtext('.//legal_entity/entity/resident/inn')
                    ogrn = holder_elem.findtext('.//legal_entity/entity/resident/ogrn')
                    holders.append({
                        'name': name.strip() if name else "",
                        'inn': inn.strip() if inn else "",
                        'ogrn': ogrn.strip() if ogrn else ""
                    })
                record['holders'] = holders

                # Документы
                documents = self.parse_documents(right_elem)
                record['documents'] = documents

                right_records.append(record)

            return right_records

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге right_records: {e}")
            return []

    def parse_restrict_records(self, elem: etree._Element) -> List[Dict[str, Any]]:
        """
        Парсит раздел restrict_records из XML и возвращает список словарей с данными.
        """
        try:
            restrict_records = []
            restrict_elems = elem.findall('.//restrict_records/restrict_record')
            for restrict_elem in restrict_elems:
                record = {}
                # Номер обременения/ограничения
                restriction_number = restrict_elem.findtext('.//restrictions_encumbrances_data/restriction_encumbrance_number') or ""
                record['restriction_number'] = restriction_number.strip()

                # Вид обременения/ограничения
                restriction_type_code = restrict_elem.findtext('.//restrictions_encumbrances_data/restriction_encumbrance_type/code') or ""
                restriction_type = restrict_elem.findtext('.//restrictions_encumbrances_data/restriction_encumbrance_type/value') or ""
                record['restriction_type_code'] = restriction_type_code.strip()
                record['restriction_type'] = restriction_type.strip()

                # Начало срока
                start_date_str = restrict_elem.findtext('.//restrictions_encumbrances_data/period/period_info/start_date') or ""
                try:
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d') if start_date_str else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат start_date: {start_date_str}")
                    start_date = None
                record['start_date'] = start_date

                # Конец срока (если есть)
                # В XML нет явного end_date, возможно, его нужно вычислить или извлечь из других полей
                record['end_date'] = None  # Пока устанавливаем как None

                # Сделка действительна время (deal_validity_time)
                deal_validity_time = restrict_elem.findtext('.//restrictions_encumbrances_data/period/period_info/deal_validity_time') or ""
                record['deal_validity_time'] = deal_validity_time.strip()

                # Признак ипотеки (transfer_deadline)
                transfer_deadline = restrict_elem.findtext('.//restrictions_encumbrances_data/period/period_info/transfer_deadline') or ""
                record['transfer_deadline'] = transfer_deadline.strip()

                # Срок обременения / ограничения
                guarantee_period = restrict_elem.findtext('.//restrictions_encumbrances_data/period/period_info/guarantee_period') or ""
                record['guarantee_period'] = guarantee_period.strip()

                # Банк
                bank = restrict_elem.findtext('.//right_holders/right_holder/legal_entity/entity/resident/name') or ""
                record['bank'] = bank.strip()

                # ИНН Банка
                bank_inn = restrict_elem.findtext('.//right_holders/right_holder/legal_entity/entity/resident/inn') or ""
                record['bank_inn'] = bank_inn.strip()

                # Дата государственной регистрации обременения / ограничения
                registration_date_str = restrict_elem.findtext('.//record_info/registration_date') or ""
                try:
                    registration_date = datetime.fromisoformat(registration_date_str.replace('Z', '+00:00')) if registration_date_str else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат registration_date: {registration_date_str}")
                    registration_date = None
                record['registration_date'] = registration_date

                # Извлечение deal_number из документов
                documents = self.parse_documents(restrict_elem)
                record['documents'] = documents
                # Предполагается, что в документах есть поле 'deal_number'
                # Извлечем его, если есть
                deal_number = next((doc['deal_number'] for doc in documents if doc.get('deal_number')), None)
                record['deal_number'] = deal_number or ""

                restrict_records.append(record)

            return restrict_records

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге restrict_records: {e}")
            return []

    def parse_deal_records(self, elem: etree._Element) -> List[Dict[str, Any]]:
        """
        Парсит раздел deal_records из XML и возвращает список словарей с данными.
        """
        try:
            deal_records = []
            deal_elems = elem.findall('.//deal_records/deal_record')
            for deal_elem in deal_elems:
                record = {}
                # Номер сделки
                deal_number = deal_elem.findtext('.//deal_number') or ""
                record['deal_number'] = deal_number.strip()

                # Тип сделки
                deal_type_code = deal_elem.findtext('.//deal_type/code') or ""
                deal_type_value = deal_elem.findtext('.//deal_type/value') or ""
                record['deal_type_code'] = deal_type_code.strip()
                record['deal_type_value'] = deal_type_value.strip()

                # Дата заключения ДДУ
                first_ddu_date_str = deal_elem.findtext('.//deal_data/subject/share_subject_description/house_descriptions/house_description/first_ddu_date') or ""
                try:
                    first_ddu_date = datetime.strptime(first_ddu_date_str, '%Y-%m-%d') if first_ddu_date_str else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат first_ddu_date: {first_ddu_date_str}")
                    first_ddu_date = None
                record['first_ddu_date'] = first_ddu_date

                # Тип объекта ДДУ
                room_name = deal_elem.findtext('.//deal_data/subject/share_subject_description/house_descriptions/house_description/room_descriptions/room_description/room_name') or ""
                record['room_name'] = room_name.strip()

                # Условный номер объекта ДДУ
                room_number = deal_elem.findtext('.//deal_data/subject/share_subject_description/house_descriptions/house_description/room_descriptions/room_description/room_number') or ""
                record['room_number'] = room_number.strip()

                # Этаж расположения объекта ДДУ
                floor_number_str = deal_elem.findtext('.//deal_data/subject/share_subject_description/house_descriptions/house_description/room_descriptions/room_description/floor_number') or ""
                try:
                    floor_number = int(floor_number_str.strip()) if floor_number_str and floor_number_str.strip().isdigit() else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат floor_number: {floor_number_str}")
                    floor_number = None
                record['floor_number'] = floor_number

                # Площадь объекта ДДУ (кв. м)
                room_area_str = deal_elem.findtext('.//deal_data/subject/share_subject_description/house_descriptions/house_description/room_descriptions/room_description/room_area') or ""
                try:
                    room_area = float(room_area_str.strip()) if room_area_str else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат room_area: {room_area_str}")
                    room_area = None
                record['room_area'] = room_area

                # Банк
                bank = deal_elem.findtext('.//deal_data/subject/share_subject_description/bank') or ""
                record['bank'] = bank.strip()

                # ИНН Банка
                # В предоставленном XML нет поля ИНН Банка для DealRecord, поэтому оставляем пустым
                bank_inn = ""  # Если есть поле, добавьте его
                record['bank_inn'] = bank_inn

                # Срок обременения / ограничения
                guarantee_period = deal_elem.findtext('.//deal_data/subject/share_subject_description/house_descriptions/house_description/room_descriptions/room_description/guarantee_period') or ""
                record['guarantee_period'] = guarantee_period.strip()

                # Признак ипотеки (transfer_deadline)
                transfer_deadline = deal_elem.findtext('.//deal_data/subject/share_subject_description/house_descriptions/house_description/room_descriptions/room_description/transfer_deadline') or ""
                record['transfer_deadline'] = transfer_deadline.strip()

                # Документы
                documents = self.parse_documents(deal_elem)
                record['documents'] = documents

                deal_records.append(record)

            return deal_records

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге deal_records: {e}")
            return []

    def parse_documents(self, elem: etree._Element) -> List[Dict[str, Any]]:
        """
        Парсит раздел documents из любого раздела и возвращает список словарей с данными.
        """
        documents = []
        try:
            for doc in elem.findall('.//underlying_documents/underlying_document'):
                doc_code = doc.findtext('.//document_code/code') or ""
                doc_value = doc.findtext('.//document_code/value') or ""
                doc_name = doc.findtext('document_name') or ""
                doc_number = doc.findtext('document_number') or ""
                doc_date_str = doc.findtext('document_date') or ""
                deal_number = doc.findtext('.//deal_registered_number/number') or ""
                right_number = doc.findtext('.//deal_registered_number/right_number') or ""
                deal_date_str = doc.findtext('.//deal_registered_date') or ""
                deal_organ = doc.findtext('.//deal_registered_organ') or ""

                # Сохраняем даты как строки, чтобы избежать проблем с JSON сериализацией
                documents.append({
                    'doc_code': doc_code.strip(),
                    'doc_value': doc_value.strip(),
                    'doc_name': doc_name.strip(),
                    'doc_number': doc_number.strip(),
                    'doc_date': doc_date_str.strip(),
                    'deal_number': deal_number.strip(),
                    'right_number': right_number.strip(),
                    'deal_date': deal_date_str.strip(),
                    'deal_organ': deal_organ.strip()
                })
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге документов: {e}")
        return documents

    def save_to_csv_xlsx(self, session, output_csv: str, output_xlsx: str,
                         log_extra: Optional[bool] = False):
        """
        Экспортирует данные из базы данных в CSV и XLSX файлы с заданной структурой.
        """
        self.logger.info("Начало экспорта данных в CSV и XLSX")
        try:
            # Получение данных из базы
            main_records = session.query(MainRecord).all()
            records_data = []

            for main in main_records:
                main_id = main.id
                # Извлечение данных из основного раздела
                record_dict = {
                    '№': main.id,
                    'Кадастровый номер ЗУ': main.cad_number or "",
                    'Местоположение ЗУ': main.readable_address or "",
                    'Категория земель ЗУ': main.purpose_value or "",
                    'Вид(ы) разрешенного использования ЗУ': main.permitted_use_established or "",
                    'Площадь ЗУ (кв. м)': main.area,
                    'Правообладатель (правообладатели) ЗУ': "",
                    'ИНН правообладателя (правообладателей) ЗУ': "",
                    'Вид государственной регистрации права': "",
                    'Номер государственной регистрации права': "",
                    'Дата государственной регистрации права': "",
                    'Реквизиты договора ДДУ': "",
                    'Дата заключение ДДУ': "",
                    'Номер государственной регистрации ДДУ': "",
                    'Дата государственной регистрации ДДУ': "",
                    'Тип объект ДДУ': "",
                    'Условный номер объекта ДДУ': "",
                    'Этаж расположения объекта ДДУ': "",
                    'Площадь объекта ДДУ (кв. м)': "",
                    'Правообладатель': "",
                    'Признак ипотеки': "",
                    'Банк': "",
                    'ИНН Банка': "",
                    'Номер государственной регистрации обременения / ограничения': "",
                    'Дата государственной регистрации обременения / ограничения': "",
                    'Срок обременения / ограничения': "",
                    'Эскроу счет': "",
                    'Признак уступки ДДУ': "",
                    'Реквизиты договора УДДУ': "",
                    'Дата заключение УДДУ': "",
                    'Номер государственной регистрации УДДУ': "",
                    'Дата государственной регистрации УДДУ': ""
                }

                # Извлечение данных из RightRecords
                if main.right_records:
                    right = main.right_records[0]  # Предполагается один RightRecord на MainRecord
                    # Правообладатели
                    holders = right.holders
                    if holders:
                        # Поскольку holders хранится как JSON-строка, необходимо преобразовать ее обратно
                        try:
                            holders_list = json.loads(holders)
                            holder_names = "; ".join([holder['name'] for holder in holders_list if holder.get('name')])
                            holder_inns = "; ".join([holder['inn'] for holder in holders_list if holder.get('inn')])
                            record_dict['Правообладатель (правообладатели) ЗУ'] = holder_names
                            record_dict['ИНН правообладателя (правообладателей) ЗУ'] = holder_inns
                        except json.JSONDecodeError:
                            self.logger.warning(f"Невозможно распарсить holders JSON: {holders}")
                            record_dict['Правообладатель (правообладатели) ЗУ'] = right.holders
                            record_dict['ИНН правообладателя (правообладателей) ЗУ'] = ""
                    # Вид государственной регистрации права
                    record_dict['Вид государственной регистрации права'] = right.right_type or ""
                    # Номер и дата регистрации права
                    record_dict['Номер государственной регистрации права'] = right.right_number or ""
                    record_dict['Дата государственной регистрации права'] = right.registration_date.isoformat() if right.registration_date else ""

                # Извлечение данных из DealRecords
                if main.deal_records:
                    for deal in main.deal_records:
                        deal_number = deal.deal_number or ""
                        # Поиск соответствующих RestrictRecords по deal_number
                        restricts = [r for r in main.restrict_records if r.deal_number == deal_number]
                        # Заполнение данных сделки
                        record_deal = record_dict.copy()
                        record_deal.update({
                            'Реквизиты договора ДДУ': deal.documents or "",
                            'Дата заключение ДДУ': deal.first_ddu_date.isoformat() if deal.first_ddu_date else "",
                            'Номер государственной регистрации ДДУ': deal.deal_number or "",
                            'Дата государственной регистрации ДДУ': deal.first_ddu_date.isoformat() if deal.first_ddu_date else "",
                            'Тип объект ДДУ': deal.room_name or "",
                            'Условный номер объекта ДДУ': deal.room_number or "",
                            'Этаж расположения объекта ДДУ': deal.floor_number if deal.floor_number is not None else "",
                            'Площадь объекта ДДУ (кв. м)': deal.room_area if deal.room_area is not None else "",
                            'Правообладатель': record_dict['Правообладатель (правообладатели) ЗУ'],
                            'Признак ипотеки': deal.transfer_deadline or "",
                            'Банк': deal.bank or "",
                            'ИНН Банка': deal.bank_inn or "",
                            'Номер государственной регистрации обременения / ограничения': "",
                            'Дата государственной регистрации обременения / ограничения': "",
                            'Срок обременения / ограничения': "",
                            'Эскроу счет': "",
                            'Признак уступки ДДУ': "",
                            'Реквизиты договора УДДУ': "",
                            'Дата заключение УДДУ': "",
                            'Номер государственной регистрации УДДУ': "",
                            'Дата государственной регистрации УДДУ': ""
                        })

                        # Если есть соответствующие RestrictRecords
                        for restrict in restricts:
                            record_restrict = record_deal.copy()
                            record_restrict.update({
                                'Номер государственной регистрации обременения / ограничения': restrict.restriction_number or "",
                                'Дата государственной регистрации обременения / ограничения': restrict.registration_date.isoformat() if restrict.registration_date else "",
                                'Срок обременения / ограничения': restrict.guarantee_period or ""
                                # Дополнительные поля можно добавить здесь
                            })
                            records_data.append(record_restrict)
                else:
                    # Если нет DealRecords, просто добавляем основной Record
                    records_data.append(record_dict)

            # Создание DataFrame с нужными колонками
            df = pd.DataFrame(records_data)

            # Определение списка необходимых колонок (без дублирования)
            desired_columns = [
                '№',
                'Кадастровый номер ЗУ',
                'Местоположение ЗУ',
                'Категория земель ЗУ',
                'Вид(ы) разрешенного использования ЗУ',
                'Площадь ЗУ (кв. м)',
                'Правообладатель (правообладатели) ЗУ',
                'ИНН правообладателя (правообладателей) ЗУ',
                'Вид государственной регистрации права',
                'Номер государственной регистрации права',
                'Дата государственной регистрации права',
                'Реквизиты договора ДДУ',
                'Дата заключение ДДУ',
                'Номер государственной регистрации ДДУ',
                'Дата государственной регистрации ДДУ',
                'Тип объект ДДУ',
                'Условный номер объекта ДДУ',
                'Этаж расположения объекта ДДУ',
                'Площадь объекта ДДУ (кв. м)',
                'Правообладатель',
                'Признак ипотеки',
                'Банк',
                'ИНН Банка',
                'Номер государственной регистрации обременения / ограничения',
                'Дата государственной регистрации обременения / ограничения',
                'Срок обременения / ограничения',
                'Эскроу счет',
                'Признак уступки ДДУ',
                'Реквизиты договора УДДУ',
                'Дата заключение УДДУ',
                'Номер государственной регистрации УДДУ',
                'Дата государственной регистрации УДДУ'
            ]

            # Проверка наличия всех нужных колонок
            missing_columns = set(desired_columns) - set(df.columns)
            if missing_columns:
                self.logger.warning(f"Отсутствуют следующие колонки в данных: {missing_columns}")
                for col in missing_columns:
                    df[col] = None  # Заполнение отсутствующих колонок пустыми значениями

            # Перестановка столбцов в требуемом порядке
            df = df[desired_columns]

            # Проверка наличия данных
            if df.empty:
                self.logger.warning("DataFrame пустой. Возможно, нет соответствующих данных для экспорта.")
            else:
                # Сохранение в CSV
                df.to_csv(output_csv, index=False, encoding='utf-8-sig')
                self.logger.info(f"Данные успешно сохранены в CSV файл: {output_csv}")

                # Сохранение в XLSX
                df.to_excel(output_xlsx, index=False, engine='openpyxl')
                self.logger.info(f"Данные успешно сохранены в XLSX файл: {output_xlsx}")

        except Exception as e:
            self.logger.error(f"Ошибка при экспорте данных в CSV/XLSX: {e}")

    def run(self):
        """
        Основной метод для запуска парсера: находит все XML-файлы и обрабатывает их.
        """
        # Проверка существования директории с XML-файлами
        if not os.path.isdir(self.xml_directory):
            self.logger.error(f"Указанная директория не существует: {self.xml_directory}")
            return

        # Получение списка XML-файлов
        xml_files = [
            os.path.join(self.xml_directory, f)
            for f in os.listdir(self.xml_directory)
            if f.lower().endswith('.xml')
        ]
        if not xml_files:
            self.logger.warning(f"В директории {self.xml_directory} не найдено XML-файлов для обработки.")
            return

        self.logger.info(f"Найдено {len(xml_files)} XML-файлов для обработки.")

        # Обработка файлов последовательно
        session = self.Session()
        for file in xml_files:
            try:
                self.parse_xml(file, session)
            except Exception as e:
                self.logger.error(f"Файл {file} вызвал исключение: {e}")
        session.close()

        self.logger.info("Парсинг всех файлов завершён.")

        # Экспорт данных в CSV и XLSX
        session = self.Session()
        self.save_to_csv_xlsx(session, self.output_csv, self.output_xlsx)
        session.close()
        self.logger.info("Экспорт данных завершён успешно.")


if __name__ == "__main__":
    import argparse

    def parse_arguments():
        parser = argparse.ArgumentParser(description='Парсер XML-файлов и экспорт в CSV/XLSX.')
        parser.add_argument('--db_type', choices=['sqlite', 'postgres'], required=True, help='Тип базы данных.')
        parser.add_argument('--sqlite_path', help='Путь к SQLite файлу.')
        parser.add_argument('--postgres_user', help='Пользователь PostgreSQL.')
        parser.add_argument('--postgres_password', help='Пароль PostgreSQL.')
        parser.add_argument('--postgres_host', help='Хост PostgreSQL.')
        parser.add_argument('--postgres_port', help='Порт PostgreSQL.')
        parser.add_argument('--postgres_database', help='Имя базы данных PostgreSQL.')
        parser.add_argument('--xml_directory', required=True, help='Директория с XML-файлами.')
        parser.add_argument('--output_csv', required=True, help='Путь к выходному CSV файлу.')
        parser.add_argument('--output_xlsx', required=True, help='Путь к выходному XLSX файлу.')
        parser.add_argument('--log_file', default='parser.log', help='Файл логов.')

        return parser.parse_args()

    args = parse_arguments()

    # Настройка конфигурации базы данных
    db_config = {}
    if args.db_type == 'sqlite':
        if not args.sqlite_path:
            raise ValueError("Путь к SQLite файлу обязателен при использовании sqlite.")
        db_config = {
            'type': 'sqlite',
            'sqlite_path': args.sqlite_path
        }
    elif args.db_type == 'postgres':
        required_fields = ['postgres_user', 'postgres_password', 'postgres_host', 'postgres_port', 'postgres_database']
        for field in required_fields:
            if not getattr(args, field):
                raise ValueError(f"Поле {field} обязательно для PostgreSQL.")
        db_config = {
            'type': 'postgres',
            'user': args.postgres_user,
            'password': args.postgres_password,
            'host': args.postgres_host,
            'port': args.postgres_port,
            'database': args.postgres_database
        }

    # Создание экземпляра парсера
    parser = EGRNParser(
        db_config=db_config,
        xml_directory=args.xml_directory,
        output_csv=args.output_csv,
        output_xlsx=args.output_xlsx,
        log_file=args.log_file
    )

    # Запуск парсера
    parser.run()
