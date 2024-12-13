import os
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
import re

from lxml import etree
from sqlalchemy import create_engine, Column, String, DateTime, Integer, ForeignKey, Float, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import pandas as pd
import json

Base = declarative_base()


class MainRecord(Base):
    __tablename__ = 'main_records'
    __table_args__ = (UniqueConstraint('registration_number', name='uix_registration_number'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    # details_statement fields
    organ_registr_rights = Column(String, nullable=True)
    date_formation = Column(DateTime, nullable=True)
    registration_number = Column(String, nullable=True, unique=True)
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
    __table_args__ = (UniqueConstraint('right_number', name='uix_right_number'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    main_record_id = Column(Integer, ForeignKey('main_records.id'), nullable=False)
    registration_date = Column(DateTime, nullable=True)  # Дата государственной регистрации права
    right_number = Column(String, nullable=True, unique=True)  # Номер государственной регистрации права
    right_type_code = Column(String, nullable=True)  # Код вида государственной регистрации права
    right_type = Column(String, nullable=True)  # Вид государственной регистрации права
    holders = Column(String, nullable=True)  # Правообладатели ЗУ (строка JSON)
    documents = Column(String, nullable=True)  # Документы (строка JSON)

    main_record = relationship('MainRecord', back_populates='right_records')


class RestrictRecord(Base):
    __tablename__ = 'restrict_records'
    __table_args__ = (UniqueConstraint('restriction_number', name='uix_restriction_number'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    main_record_id = Column(Integer, ForeignKey('main_records.id'), nullable=False)
    restriction_number = Column(String, nullable=True, unique=True)  # Номер обременения/ограничения
    restriction_type_code = Column(String, nullable=True)  # Код вида обременения/ограничения
    restriction_type = Column(String, nullable=True)  # Вид обременения/ограничения
    start_date = Column(DateTime, nullable=True)  # Начало срока обременения / ограничения
    end_date = Column(Integer, nullable=True)  # Срок обременения / ограничения (месяцы)
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
    __table_args__ = (UniqueConstraint('deal_number', name='uix_deal_number'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    main_record_id = Column(Integer, ForeignKey('main_records.id'), nullable=False)
    deal_number = Column(String, nullable=True, unique=True)  # Номер сделки
    registration_date = Column(DateTime, nullable=True)
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
    deal_parties = relationship('DealParty', back_populates='deal_record', cascade='all, delete-orphan')


class DealParty(Base):
    __tablename__ = 'deal_parties'

    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_record_id = Column(Integer, ForeignKey('deal_records.id'), nullable=False)
    concession_mark = Column(String, nullable=True)
    party_type_code = Column(String, nullable=True)
    party_type_value = Column(String, nullable=True)
    party_info = Column(String, nullable=True)

    deal_record = relationship('DealRecord', back_populates='deal_parties')


class EGRNParser:
    def __init__(
        self,
        db_config: Dict[str, Any],
        xml_directory: str,
        output_csv: str,
        output_xlsx: str,
        log_file: str = 'parser.log'
    ):
        self.db_config = db_config
        self.xml_directory = xml_directory
        self.output_csv = output_csv
        self.output_xlsx = output_xlsx
        self.log_file = log_file

        self.logger = logging.getLogger('EGRNParser')
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        self.logger.info("Инициализация парсера начата.")

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
            engine = create_engine(connection_string, echo=False, client_encoding='utf8')
        elif self.db_config['type'] == 'sqlite':
            sqlite_path = self.db_config['sqlite_path']
            connection_string = f'sqlite:///{sqlite_path}'
            engine = create_engine(connection_string, echo=False)
        else:
            self.logger.error("Unsupported database type.")
            raise ValueError("Unsupported database type.")

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
            context = etree.iterparse(file_path, events=('end',), tag='extract_contract_participation_share_holdings', recover=True, encoding='utf-8')

            for event, elem in context:
                details_statement = self.parse_details_statement(elem)
                details_request = self.parse_details_request(elem)
                land_record = self.parse_land_record(elem)
                right_records = self.parse_right_records(elem)
                restrict_records = self.parse_restrict_records(elem)
                deal_records = self.parse_deal_records(elem)

                registration_number = details_statement.get('registration_number')
                existing_main = session.query(MainRecord).filter_by(registration_number=registration_number).first()
                if existing_main:
                    self.logger.info(f"MainRecord с registration_number '{registration_number}' уже существует. Пропуск.")
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]
                    continue

                main_record = MainRecord(
                    organ_registr_rights=details_statement.get('organ_registr_rights'),
                    date_formation=details_statement.get('date_formation'),
                    registration_number=details_statement.get('registration_number'),
                    date_received_request=details_request.get('date_received_request'),
                    date_receipt_request_reg_authority_rights=details_request.get('date_receipt_request_reg_authority_rights'),
                    cad_number=land_record.get('cad_number'),
                    readable_address=land_record.get('readable_address'),
                    purpose_code=land_record.get('purpose_code'),
                    purpose_value=land_record.get('purpose_value'),
                    permitted_use_code=land_record.get('permitted_use_code'),
                    permitted_use_established=land_record.get('permitted_use_established'),
                    area=land_record.get('area'),
                    source_file=os.path.basename(file_path)
                )

                for right in right_records:
                    right_number = right.get('right_number')
                    existing_right = session.query(RightRecord).filter_by(right_number=right_number).first()
                    if existing_right:
                        self.logger.info(f"RightRecord с right_number '{right_number}' уже существует. Пропуск.")
                        continue

                    right_record = RightRecord(
                        registration_date=right.get('registration_date'),
                        right_number=right.get('right_number'),
                        right_type_code=right.get('right_type_code'),
                        right_type=right.get('right_type'),
                        holders=json.dumps(right.get('holders'), ensure_ascii=False),
                        documents=json.dumps(right.get('documents'), ensure_ascii=False),
                    )
                    main_record.right_records.append(right_record)

                for restrict in restrict_records:
                    restriction_number = restrict.get('restriction_number')
                    existing_restrict = session.query(RestrictRecord).filter_by(restriction_number=restriction_number).first()
                    if existing_restrict:
                        self.logger.info(f"RestrictRecord с restriction_number '{restriction_number}' уже существует. Пропуск.")
                        continue

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
                        documents=json.dumps(restrict.get('documents'), ensure_ascii=False),
                        deal_number=restrict.get('deal_number'),
                    )
                    main_record.restrict_records.append(restrict_record)

                # Добавление DealRecords
                for deal in deal_records:
                    deal_number = deal.get('deal_number')
                    # Проверка существования DealRecord
                    existing_deal = session.query(DealRecord).filter_by(deal_number=deal_number).first()
                    if existing_deal:
                        self.logger.info(f"DealRecord с deal_number '{deal_number}' уже существует. Пропуск.")
                        continue

                    deal_record = DealRecord(
                        deal_number=deal.get('deal_number'),
                        registration_date=deal.get('registration_date'),
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
                        documents=json.dumps(deal.get('documents'), ensure_ascii=False)
                    )
                    main_record.deal_records.append(deal_record)

                    # Добавление DealParties
                    deal_parties = deal.get('deal_parties', [])
                    for party in deal_parties:
                        # Сериализуем информацию об участнике в JSON строку
                        party_info_json = json.dumps(party, ensure_ascii=False)
                        deal_party = DealParty(
                            concession_mark=party.get('concession_mark'),
                            party_type_code=party.get('party_type_code'),
                            party_type_value=party.get('party_type_value'),
                            party_info=party_info_json
                        )
                        deal_record.deal_parties.append(deal_party)

                session.add(main_record)
                session.commit()

                self.logger.info(f"Парсинг и сохранение данных из файла {file_path} завершены успешно.")

                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге файла {file_path}: {e}")

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

                # Конец срока (из deal_validity_time)
                deal_validity_time_str = restrict_elem.findtext('.//restrictions_encumbrances_data/period/period_info/deal_validity_time') or ""
                months_match = re.search(r'(\d+)\s+месяц', deal_validity_time_str)
                if months_match:
                    record['end_date'] = int(months_match.group(1))
                else:
                    record['end_date'] = None

                # Сделка действительна время (deal_validity_time)
                record['deal_validity_time'] = deal_validity_time_str.strip()

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
                # Извлечем deal_number из документов, если он присутствует
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
                bank_inn = ""
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

                # Парсинг участников сделки (deal_parties)
                deal_parties = self.parse_deal_parties(deal_elem)
                record['deal_parties'] = deal_parties

                registration_date_str = deal_elem.findtext('.//record_info/registration_date')
                try:
                    registration_date = datetime.fromisoformat(registration_date_str.replace('Z', '+00:00')) if registration_date_str else None
                except ValueError:
                    self.logger.warning(f"Некорректный формат даты регистрации: {registration_date_str}")
                    registration_date = None
                record['registration_date'] = registration_date

                deal_records.append(record)

            return deal_records

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге deal_records: {e}")
            return []

    def parse_deal_parties(self, deal_elem: etree._Element) -> List[Dict[str, Any]]:
        """
        Парсит раздел deal_parties из DealRecord и возвращает список словарей с данными.
        """
        try:
            deal_parties = []
            deal_parties_elems = deal_elem.findall('.//deal_parties/deal_party')
            for party_elem in deal_parties_elems:
                party = {}
                party['concession_mark'] = party_elem.findtext('.//concession_mark') or ""

                party_type_code = party_elem.findtext('.//party_type/code') or ""
                party_type_value = party_elem.findtext('.//party_type/value') or ""
                party['party_type_code'] = party_type_code.strip()
                party['party_type_value'] = party_type_value.strip()

                # Определяем тип участника сделки
                individual = party_elem.find('.//party_info/individual')
                legal_entity = party_elem.find('.//party_info/legal_entity')

                if individual is not None:
                    name = individual.findtext('.//name') or ""
                    party['party_info'] = {
                        'type': 'individual',
                        'name': name.strip()
                    }
                elif legal_entity is not None:
                    entity = legal_entity.find('.//entity/resident')
                    contacts = legal_entity.find('.//contacts')
                    if entity is not None:
                        name = entity.findtext('.//name') or ""
                        inn = entity.findtext('.//inn') or ""
                        ogrn = entity.findtext('.//ogrn') or ""
                        mailing_address = contacts.findtext('.//mailing_address') if contacts is not None else ""
                        party['party_info'] = {
                            'type': 'legal_entity',
                            'name': name.strip(),
                            'inn': inn.strip(),
                            'ogrn': ogrn.strip(),
                            'mailing_address': mailing_address.strip() if mailing_address else ""
                        }
                else:
                    party['party_info'] = {}

                deal_parties.append(party)

            return deal_parties

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге deal_parties: {e}")
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
    
    def save_to_csv_xlsx(self, session, output_csv: str, output_xlsx: str):
        """
        Экспортирует данные из базы данных в CSV и XLSX файлы с заданной структурой.
        """
        self.logger.info("Начало экспорта данных в CSV и XLSX")
        try:
            main_records = session.query(MainRecord).all()
            records_data = []

            for main in main_records:
                main_id = main.id
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
                    'Дата государственной регистрации УДДУ': "",
                    'Правообладатель2': "",
                    'Признак ипотеки2': "",
                    'Банк2': "",
                    'ИНН Банка2': "",
                    'Номер государственной регистрации обременения / ограничения2': "",
                    'Дата государственной регистрации обременения / ограничения2': "",
                    'Срок обременения / ограничения2': "",
                }

                # Извлечение данных из RightRecords
                if main.right_records:
                    right = main.right_records[0]
                    # Правообладатели
                    holders = right.holders
                    if holders:
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
                            'Дата государственной регистрации УДДУ': "",
                            'Правообладатель2': "",
                            'Признак ипотеки2': "",
                            'Банк2': "",
                            'ИНН Банка2': "",
                            'Номер государственной регистрации обременения / ограничения2': "",
                            'Дата государственной регистрации обременения / ограничения2': "",
                            'Срок обременения / ограничения2': "",
                        })

                        # Парсинг документов
                        main_doc = None
                        related_doc = None
                        related_main_doc = None
                        if deal.documents:
                            try:
                                documents_list = json.loads(deal.documents)
                                for doc in documents_list:
                                    if 'уступке' in doc.get('doc_value', '').lower():
                                        related_doc = doc
                                        for restrict in main.restrict_records:
                                            if (restrict.documents and 
                                                any(r_doc.get('doc_value') == doc.get('doc_value') and
                                                    r_doc.get('doc_name') == doc.get('doc_name') and
                                                    r_doc.get('doc_number') == doc.get('doc_number')
                                                    for r_doc in json.loads(restrict.documents))):
                                                related_main_doc = restrict
                                                break
                                    else:
                                        if not main_doc:
                                            main_doc = doc
                                    
                            except json.JSONDecodeError:
                                self.logger.warning(f"Невозможно распарсить documents JSON: {deal.documents}")

                        # Заполнение данных основного документа
                        if main_doc:
                            record_deal['Реквизиты договора ДДУ'] = f"{main_doc.get('doc_name', '')} {main_doc.get('doc_number', '')}".strip()
                            record_deal['Дата заключение ДДУ'] = main_doc.get('doc_date', '')
                            record_deal['Номер государственной регистрации ДДУ'] = deal.deal_number or ""
                            record_deal['Дата государственной регистрации ДДУ'] = deal.registration_date or ""
                            record_deal['Тип объект ДДУ'] = deal.room_name or ""
                            record_deal['Условный номер объекта ДДУ'] = deal.room_number or ""
                            record_deal['Этаж расположения объекта ДДУ'] = deal.floor_number if deal.floor_number is not None else ""
                            record_deal['Площадь объекта ДДУ (кв. м)'] = deal.room_area if deal.room_area is not None else ""
                            record_deal['Эскроу счет'] = deal.bank or ""

                            # Признак уступки ДДУ
                            if related_doc:
                                record_deal['Признак уступки ДДУ'] = "Да"
                                record_deal['Реквизиты договора УДДУ'] = f"{related_doc.get('doc_name', '')} {related_doc.get('doc_number', '')}".strip()
                                record_deal['Дата заключение УДДУ'] = related_doc.get('doc_date', '')
                                record_deal['Номер государственной регистрации УДДУ'] = related_main_doc.deal_number or ""
                                record_deal['Дата государственной регистрации УДДУ'] = related_main_doc.registration_date or ""
                                
                                deal_parties = [dp.party_info for dp in deal.deal_parties]
                                deal_parties_str = "; ".join([self.format_deal_party(party) for party in deal_parties])
                                record_deal['Правообладатель2'] = deal_parties_str
                                
                                record_deal['Банк2'] = related_main_doc.bank or ""
                                record_deal['ИНН Банка2'] = related_main_doc.bank_inn or ""
                                record_deal['Номер государственной регистрации обременения / ограничения2'] = related_main_doc.restriction_number or ""
                                record_deal['Дата государственной регистрации обременения / ограничения2'] = related_main_doc.registration_date.isoformat() if related_main_doc.registration_date else ""
                                record_deal['Срок обременения / ограничения2'] = related_main_doc.end_date if related_main_doc.end_date is not None else ""
                                record_deal['Признак ипотеки2'] = "Да" if related_main_doc.restriction_number else "Нет"
                            else:
                                record_deal['Признак уступки ДДУ'] = "Нет"
                                record_deal['Реквизиты договора УДДУ'] = "данные отсутствуют"
                                record_deal['Дата заключение УДДУ'] = "данные отсутствуют"
                                record_deal['Номер государственной регистрации УДДУ'] = "данные отсутствуют"
                                record_deal['Дата государственной регистрации УДДУ'] = "данные отсутствуют"

                        # Извлечение и агрегирование DealParties
                        deal_parties = [dp.party_info for dp in deal.deal_parties]
                        deal_parties_str = "; ".join([self.format_deal_party(party) for party in deal_parties])
                        record_deal['Правообладатель'] = deal_parties_str

                        # Сопоставление RestrictRecords
                        if restricts:
                            for restrict in restricts:
                                record_restrict = record_deal.copy()
                                record_restrict['Признак ипотеки'] = "Да" if restrict.restriction_number else "Нет"
                                record_restrict['Номер государственной регистрации обременения / ограничения'] = restrict.restriction_number or ""
                                record_restrict['Дата государственной регистрации обременения / ограничения'] = restrict.registration_date.isoformat() if restrict.registration_date else ""
                                record_restrict['Срок обременения / ограничения'] = restrict.end_date if restrict.end_date is not None else ""
                                record_restrict['Банк'] = restrict.bank or ""
                                record_restrict['ИНН Банка'] = restrict.bank_inn or ""

                                # Заполнение информации о связанном документе, если он есть
                                if related_doc:
                                    # record_restrict['Признак ипотеки2'] = "Да" if restrict.restriction_number else "Нет"
                                    # record_restrict['Номер государственной регистрации обременения / ограничения2'] = ""
                                    # record_restrict['Дата государственной регистрации обременения / ограничения2'] = ""
                                    # record_restrict['Срок обременения / ограничения2'] = ""
                                    continue

                                records_data.append(record_restrict)
                        else:
                            records_data.append(record_deal)
                else:
                    # Если нет DealRecords, просто добавляем основной Record
                    records_data.append(record_dict)

            df = pd.DataFrame(records_data)

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
                'Дата государственной регистрации УДДУ',
                'Правообладатель2',
                'Признак ипотеки2',
                'Банк2',
                'ИНН Банка2',
                'Номер государственной регистрации обременения / ограничения2',
                'Дата государственной регистрации обременения / ограничения2',
                'Срок обременения / ограничения2',
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


    def format_deal_party(self, party_str: Dict[str, Any]) -> str:
        """
        Форматирует информацию об участнике сделки для экспорта.
        """
        party = json.loads(party_str).get('party_info')
        if party.get('type') == 'individual':
            name = party.get('name', '')
            return f"{name}"
        elif party.get('type') == 'legal_entity':
            name = party.get('name', '')
            inn = party.get('inn', '')
            ogrn = party.get('ogrn', '')
            mailing_address = party.get('mailing_address', '')
            return f"{name}"
        else:
            return "Неизвестный тип участника"

    def run(self):
        """
        Основной метод для запуска парсера: находит все XML-файлы и обрабатывает их.
        """
        # Проверка существования директории с XML-файлами
        if not os.path.isdir(self.xml_directory):
            self.logger.error(f"Указанная директория не существует: {self.xml_directory}")
            return

        xml_files = [
            os.path.join(self.xml_directory, f)
            for f in os.listdir(self.xml_directory)
            if f.lower().endswith('.xml')
        ]
        if not xml_files:
            self.logger.warning(f"В директории {self.xml_directory} не найдено XML-файлов для обработки.")
            return

        self.logger.info(f"Найдено {len(xml_files)} XML-файлов для обработки.")

        session = self.Session()
        for file in xml_files:
            try:
                self.parse_xml(file, session)
            except Exception as e:
                self.logger.error(f"Файл {file} вызвал исключение: {e}")
        session.close()

        self.logger.info("Парсинг всех файлов завершён.")

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

    parser = EGRNParser(
        db_config=db_config,
        xml_directory=args.xml_directory,
        output_csv=args.output_csv,
        output_xlsx=args.output_xlsx,
        log_file=args.log_file
    )

    parser.run()
