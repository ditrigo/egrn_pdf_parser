# Парсер выписок из ЕГРН

## Описание

Этот инструмент позволяет парсить XML-выписки из ЕГРН, сохранять данные в базе данных (PostgreSQL или SQLite) и экспортировать результаты в CSV и XLSX файлы. Приложение поддерживает как командную строку (CLI), так и графический интерфейс (GUI) на базе PyQt5.

## Установка

1. **Клонируйте репозиторий:**

    ```bash
    git clone https://github.com/ваш-репозиторий/egrn_parser.git
    cd egrn_parser
    ```

2. **Установите зависимости:**

    ```bash
    pip install -r requirements.txt
    ```

## Использование

### CLI-Режим

Запуск парсера через командную строку с указанием всех необходимых параметров.

**Пример для PostgreSQL:**

```bash
python main.py \
    --db_type postgres \
    --db_host localhost \
    --db_port 5432 \
    --db_name egrn_db \
    --db_user your_username \
    --db_password your_password \
    --xml_directory path/to/xml/files \
    --output_csv output/restrict_records.csv \
    --output_xlsx output/restrict_records.xlsx \
    --log_file parser.log \
    --max_workers 4
```

** Пример для SQLite **

```bash
python main.py \
    --db_type sqlite \
    --xml_directory path/to/xml/files \
    --output_csv output/restrict_records.csv \
    --output_xlsx output/restrict_records.xlsx \
    --log_file parser.log \
    --max_workers 4
```

```bash
python3 parser.py     --db_type sqlite     --sqlite_path ./data/egrn_database.sqlite     --xml_directory ./xml_files     --output_csv ./output/restrict_records.csv     --output_xlsx ./output/restrict_records.xlsx     --log_file ./logs/parser.log
```
### CLI-Режим

```bash
python main.py
```