from datetime import datetime
from typing import List
from sqlalchemy import (
    Column, String, DateTime, Integer, ForeignKey, Float, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class MainRecord(Base):
    __tablename__ = 'main_records'
    __table_args__ = (UniqueConstraint('registration_number', name='uix_registration_number'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Details Statement Fields
    organ_registr_rights = Column(String, nullable=True)
    date_formation = Column(DateTime, nullable=True)
    registration_number = Column(String, nullable=True, unique=True)
    # Details Request Fields
    date_received_request = Column(DateTime, nullable=True)
    date_receipt_request_reg_authority_rights = Column(DateTime, nullable=True)
    # Land Record Fields
    cad_number = Column(String, nullable=True)  # Кадастровый номер ЗУ
    readable_address = Column(String, nullable=True)  # Местоположение ЗУ
    purpose_code = Column(String, nullable=True)  # Код категории земель ЗУ
    purpose_value = Column(String, nullable=True)  # Категория земель ЗУ
    permitted_use_code = Column(String, nullable=True)  # Код вида разрешенного использования ЗУ
    permitted_use_established = Column(String, nullable=True)  # Вид(ы) разрешенного использования ЗУ
    area = Column(Float, nullable=True)  # Площадь ЗУ (кв. м)

    # Relationships
    right_records = relationship(
        'RightRecord', back_populates='main_record', cascade='all, delete-orphan'
    )
    restrict_records = relationship(
        'RestrictRecord', back_populates='main_record', cascade='all, delete-orphan'
    )
    deal_records = relationship(
        'DealRecord', back_populates='main_record', cascade='all, delete-orphan'
    )

    # Metadata
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
    deal_parties = relationship(
        'DealParty', back_populates='deal_record', cascade='all, delete-orphan'
    )


class DealParty(Base):
    __tablename__ = 'deal_parties'

    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_record_id = Column(Integer, ForeignKey('deal_records.id'), nullable=False)
    concession_mark = Column(String, nullable=True)
    party_type_code = Column(String, nullable=True)
    party_type_value = Column(String, nullable=True)
    party_info = Column(String, nullable=True)

    deal_record = relationship('DealRecord', back_populates='deal_parties')
