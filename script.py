# script.py
# Автоматический сбор .xlsx (по неделям) → парсинг → генерация schedule.ics
# Настройки: укажите GROUP_ID (ваша группа — у вас 427997), и WEEKS_AHEAD (сколько недель вперед пытаться скачать)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime, timedelta, date
from urllib.parse import urljoin
import os
from uuid import uuid4

GROUP_ID = "427997"        # <- Замените, если у вас другой id группы
WEEKS_AHEAD = 16          # <- сколько недель вперед пытаться скачать (можно уменьшить/увеличить)
SITE_ROOT = "https://timetable.spbu.ru"
OUT_ICS = "schedule.ics"

# Русские названия месяцев для парсинга
MONTHS = {
    'января':1, 'февраля':2, 'марта':3, 'апреля':4, 'мая':5, 'июня':6,
    'июля':7, 'августа':8, 'сентября':9, 'октября':10, 'ноября':11, 'декабря':12
}

def get_this_monday(d: date):
    return d - timedelta(days=d.weekday())

def find_excel_link_from_page(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    # Ищем ссылку на .xlsx или на DownloadExcel
    for a in soup.find_all("a", href=True):
        href = a['href']
        txt = (a.get_text() or "").lower()
        if ".xlsx" in href or ".xls" in href or "downloadexcel" in href or "скачать" in txt or "download" in txt:
            return urljoin(base_url, href)
    return None

def parse_header_year_from_raw(raw_df):
    # Попробовать найти год в первых строках
    for i in range(min(8, raw_df.shape[0])):
        for j in range(min(8, raw_df.shape[1])):
            val = raw_df.iat[i, j]
            if isinstance(val, str) and ('г.' in val or re.search(r'\d{4}', val)):
                m = re.search(r'(\d{4})', val)
                if m:
                    return int(m.group(1))
    # fallback: год из сегодняшней даты
    return datetime.utcnow().year

def parse_week_excel_bytes(content_bytes, assumed_year=None):
    excel_io = pd.io.common.BytesIO(content_bytes)
    # Найдём год внутри файла при помощи чтения без заголовка
    raw = pd.read_excel(excel_io, header=None)
    excel_io.seek(0)
    year = parse_header_year_from_raw(raw) if assumed_year is None else assumed_year

    # Основная таблица — обычно начинается после 3 строк заголовка (как в ваших файлах)
    df = pd.read_excel(excel_io, skiprows=3)
    # первая колонка — день + дата (например "среда\n 24 сентября")
    df = df.rename(columns={df.columns[0]:'DayAndDate'})
    df['DayAndDate'] = df['DayAndDate'].ffill()

    events = []
    for _, row in df.iterrows():
        daydate = row.get('DayAndDate')
        time_raw = row.get('Время', None)
        name = row.get('Название', None)
        location = row.get('Места проведения', None)
        teacher = row.get('Преподаватели', None)
        if pd.isna(time_raw) or pd.isna(name):
            continue

        # извлечь число и месяц из DayAndDate
        if not isinstance(daydate, str):
            continue
        dd = daydate.strip()
        if '\n' in dd:
            dd = dd.splitlines()[-1].strip()

        m = re.search(r'(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?', dd, flags=re.IGNORECASE)
        if not m:
            continue
        day = int(m.group(1))
        mon_name = m.group(2).lower()
        yr = int(m.group(3)) if m.group(3) else year
        mon = MONTHS.get(mon_name)
        if not mon:
            continue
        try:
            dt_date = datetime(yr, mon, day).date()
        except Exception:
            continue

        # Разбор времени "09:00–10:35"
        time_text = str(time_raw).strip()
        parts = re.split(r'[–—\-]', time_text)
        if len(parts) < 2:
            continue
        start_t = parts[0].strip()
        end_t = parts[1].strip()
        try:
            start_dt = datetime.strptime(f"{dt_date} {start_t}", "%Y-%m-%d %H:%M")
            end_dt   = datetime.strptime(f"{dt_date} {end_t}", "%Y-%m-%d %H:%M")
        except Exception:
            continue

        events.append({
            "start": start_dt,
            "end": end_dt,
            "summary": str(name),
            "location": "" if pd.isna(location) else str(location),
            "teacher": "" if pd.isna(teacher) else str(teacher)
        })
    return events

def events_to_ics(events):
    # Уникализируем по (start,end,summary) чтобы не дублировать
    seen = set()
    unique = []
    for e in events:
        key = (e['start'], e['end'], e['summary'])
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//spbu-schedule//EN",
        "CALSCALE:GREGORIAN"
    ]
    now = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    for e in unique:
        uid = str(uuid4())
        def dtf(dt): return dt.strftime("%Y%m%dT%H%M%S")
        lines.append("BEGIN:VEVENT")
        lines.append(f"UID:{uid}")
        lines.append(f"DTSTAMP:{now}")
        lines.append(f"DTSTART:{dtf(e['start'])}")
        lines.append(f"DTEND:{dtf(e['end'])}")
        summary = e['summary'].replace('\n',' ').replace(',','\\,').replace(';','\\;')
        lines.append(f"SUMMARY:{summary}")
        if e['location']:
            loc = e['location'].replace('\n',' ').replace(',','\\,').replace(';','\\;')
            lines.append(f"LOCATION:{loc}")
        if e['teacher']:
            desc = ("Преподаватель: " + e['teacher']).replace('\n',' ').replace(',','\\,').replace(';','\\;')
            lines.append(f"DESCRIPTION:{desc}")
        lines.append("END:VEVENT")
    lines.append("END:VCALENDAR")
    return "\n".join(lines)

def main():
    today = datetime.utcnow().date()
    start_monday = get_this_monday(today)
    all_events = []

    for w in range(WEEKS_AHEAD):
        week_date = start_monday + timedelta(weeks=w)
        date_str = week_date.strftime("%Y-%m-%d")
        page_url = f"{SITE_ROOT}/EARTH/StudentGroupEvents/Primary/{GROUP_ID}/{date_str}"
        try:
            r = requests.get(page_url, timeout=20)
            if r.status_code != 200:
                print(f"[skip] {page_url} -> {r.status_code}")
                continue
            excel_link = find_excel_link_from_page(r.text, page_url)
            if not excel_link:
                print(f"[no-download-link] {page_url}")
                continue
            print(f"[found] {excel_link}")
            rx = requests.get(excel_link, timeout=30)
            if rx.status_code == 200:
                evs = parse_week_excel_bytes(rx.content)
                print(f"  events parsed: {len(evs)}")
                all_events.extend(evs)
            else:
                print(f"  failed download: {rx.status_code}")
        except Exception as ex:
            print("Error while fetching", page_url, ex)

    if not all_events:
        print("No events found — schedule.ics не будет создан.")
        return

    ics_text = events_to_ics(all_events)
    with open(OUT_ICS, "w", encoding="utf-8") as f:
        f.write(ics_text)
    print("Wrote", OUT_ICS)

if __name__ == "__main__":
    main()
