# Парсер выписок из ЕГРН

Инструмент для пакетной обработки XML‑выписок ЕГРН: извлекает сведения из секций выписки, сохраняет результаты в базу данных (SQLite или PostgreSQL) и формирует отчёты в CSV/XLSX. Доступны два варианта запуска — консольный (`main.py`, `parser.py`) и графический интерфейс на PyQt5 (`gui.py`).

## Требования
- Python 3.10+ (рекомендуется виртуальное окружение)
- Установленные системные зависимости для `lxml` (libxml2/libxslt)
- Для работы GUI необходима рабочая среда Qt (PyQt5)

## Установка
```bash
git clone <repo-url>
cd egrn_pdf_parser
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Быстрый старт (SQLite, CLI)
В поставке есть пример XML в каталоге `xml_files/`. Команда ниже создаст базу SQLite, распарсит файл и сформирует отчёты в `output/`:
```bash
python parser.py \
  --db_type sqlite \
  --sqlite_path ./output/egrn.sqlite \
  --xml_directory ./xml_files \
  --output_csv ./output/restrict_records.csv \
  --output_xlsx ./output/restrict_records.xlsx \
  --log_file ./logs/parser.log
```
Каталоги для БД, логов и отчётов создаются автоматически.

## Запуск через `main.py`
`main.py` содержит CLI-обёртку с набором аргументов (значения по умолчанию указаны справа):

| Аргумент | Назначение |
| --- | --- |
| `--db_type {sqlite,postgres}` (sqlite) | Тип хранилища |
| `--sqlite_path PATH` (`egrn_database.sqlite`) | Файл SQLite |
| `--db_host`, `--db_port`, `--db_name`, `--db_user`, `--db_password` | Параметры Postgres (обязательны при `--db_type postgres`) |
| `--xml_directory PATH` (`xml_files`) | Каталог с XML |
| `--output_csv PATH` (`output/restrict_records.csv`) | Путь для CSV |
| `--output_xlsx PATH` (`output/restrict_records.xlsx`) | Путь для XLSX |
| `--log_file PATH` (`parser.log`) | Файл логов |

Примеры:
```bash
# SQLite (значения по умолчанию использовать не обязательно)
python main.py --xml_directory data/xml --output_csv output/result.csv

# PostgreSQL
python main.py \
  --db_type postgres \
  --db_host localhost \
  --db_port 5432 \
  --db_name egrn \
  --db_user parser \
  --db_password secret \
  --xml_directory /data/egrn/xml \
  --output_csv output/restrict_records.csv \
  --output_xlsx output/restrict_records.xlsx
```

`main.py` автоматически создаёт недостающие директории под результаты и логи.

## Запуск напрямую `parser.py`
`parser.py` экспортирует класс `EGRNParser`, который можно использовать как библиотеку или запускать напрямую (пример в секции «Быстрый старт»). При программном использовании достаточно передать конфигурацию БД, каталог XML и пути для отчётов:
```python
from parser import EGRNParser

parser = EGRNParser(
    db_config={'type': 'sqlite', 'sqlite_path': 'output/egrn.sqlite'},
    xml_directory='xml_files',
    output_csv='output/restrict_records.csv',
    output_xlsx='output/restrict_records.xlsx',
    log_file='logs/parser.log',
)
parser.run()
```

## Обработка нескольких выгрузок
Скрипт `run_all.sh` запускает CLI для каждого подкаталога в `./extracted`. Убедитесь, что он исполняемый:
```bash
chmod +x run_all.sh
./run_all.sh
```
Отчёты и база для каждого каталога будут сохранены рядом с исходными XML.

## Графический интерфейс
```bash
python gui.py
```
В GUI выберите каталог с XML и директорию, куда сохранить результаты. Приложение использует SQLite в выбранной папке (файл `egrn_database.sqlite`) и пишет лог `parser.log`. Выполнение парсинга вынесено в отдельный поток, прогресс отображается в текстовом поле.

## Структура данных
- **База данных**: таблицы `file_records`, `main_records`, `right_records`, `restrict_records`, `deal_records`, `deal_parties`. Уникальные ограничения предотвращают повторное сохранение записей.
- **Отчёт (CSV/XLSX)**: одна строка на комбинацию «сделка/обременение». Для участков без сделок формируется строка с данными ограничений или правообладателей. Все даты сериализуются в ISO 8601.
- **Логи**: подробный лог процесса в файле, дублируется в консоль.
