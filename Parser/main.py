import asyncio
from telethon import TelegramClient
import csv
from re import search
import datetime
import sys
import aiofiles
from typing import List
from time import perf_counter
from config import api_id, api_hash


async def set_data_regex() -> List[str]:
    print('Введите ключевые слова через пробел в одну строку:')
    return [word for word in input().split(' ') if word]


async def get_data_urls() -> List[str]:
    try:
        async with aiofiles.open(r"Parser/urls_data.txt", 'r') as txt_file:
            lines = await txt_file.readlines()
    except FileNotFoundError:
        print("Создайте в папке программы файл 'urls_data.txt' и на каждой новой строке файла введите ссылки на телеграм беседы.")
        await asyncio.sleep(.5)
        print("Если вы восстановили файл 'urls_data.txt' вы можете продолжить. Продолжить? y/n")
        await get_data_urls() if search('^y', input().lower()) else sys.exit(0)
    urls = [line.replace('\n', '') for line in lines if search(r"^https://t.me/", line)]
    if len(urls) == 0:
        print("Не найдено ни одной ссылки. Удостоверьтесь, что в файле 'urls_data.txt' ссылки начинаются на 'https://t.me/'")
        await asyncio.sleep(.5)
        print("Если вы восстановили ссылки в файле 'urls_data.txt' вы можете продолжить. Продолжить? y/n")
        await get_data_urls() if search('^y', input().lower()) else sys.exit(0)
    else:
        print(f"Найдено ссылок: {len(urls)}.")
        return urls


async def set_date_begin() -> datetime:
    print('Введите дату в формате: год.месяц.число. Например: 22.11.9 или 22.11.09')
    try:
        return datetime.datetime.strptime(input(), '%y.%m.%d')
    except ValueError:
        print('Неверная дата. Попробовать снова? y/n')
        await set_date_begin() if search('^y', input().lower()) else sys.exit(0)


async def set_file_name_csv() -> str:
    print("Введите название файла, куда запишутся результаты. Например: clients. "
          "Расширение указывать не нужно.\n "
          "Файл создастся автоматически в файле программы, либо перезапишется существующий.")
    return input()+".csv"


async def clear_csv(csv_file_name: str):
    async with aiofiles.open(csv_file_name, 'w', newline='', encoding="utf16") as csvfile:
        writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        await writer.writerow(["username", "фамилия", "имя", "телефон", "дата сообщения", "сокращенный текст сообщения"])


async def process_csv() -> str:
    csv_file_name = await set_file_name_csv()
    print(f"Идет подготовка файла {csv_file_name}...")
    try:
        await clear_csv(csv_file_name)
    except PermissionError:
        print(f"Необходимо закрыть файл {csv_file_name}.")
        await asyncio.sleep(.5)
        print(f"Если вы закрыли файл {csv_file_name}, вы можете продолжить. Продолжить? y/n")
        await process_csv() if search('^y', input().lower()) else sys.exit(0)
    except OSError:
        print("Введите корректное название файла. Продолжить? y/n")
        await process_csv() if search('^y', input().lower()) else sys.exit(0)
    return csv_file_name


async def write_to_csv(csv_file_name: str, user_list: List[List[str]]):
    print(f"Идет внесение данных в {csv_file_name}...")
    try:
        async with aiofiles.open(csv_file_name, 'a', newline='', encoding="utf16") as csvfile:
            writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for user_info in user_list:
                await writer.writerow(user_info)
    except PermissionError:
        print(f"Необходимо закрыть файл {csv_file_name}.")
        await asyncio.sleep(.5)
        print(f"Если вы закрыли файл {csv_file_name}, вы можете продолжить. Продолжить? y/n")
        await write_to_csv(csv_file_name, user_list) if search('^y', input().lower()) else sys.exit(0)


async def get_entities(client, message, users, regex):
    for r in regex:
        if search(r, str(message.message)):
            try:
                parsing_entity = await client.get_entity(message.from_id.user_id)
                if parsing_entity.username or parsing_entity.phone:
                    date_mes = datetime.datetime.strftime(message.date, "%d/%m/%y")
                    text_mes = ' '.join(str(message.message).split(' ')[:17]).replace('\n', ' ')
                    users.append([parsing_entity.username, parsing_entity.last_name, parsing_entity.first_name,
                                  parsing_entity.phone, date_mes, text_mes])
            except (ValueError, AttributeError):
                pass
            break


async def get_clients(url_group, date_begin, regex, users):
    async with TelegramClient('my', api_id, api_hash) as client:
        print(f"Идет обработка группы {url_group}...")
        await client.get_participants(url_group)
        async for message in client.iter_messages(entity=url_group,
                                                  offset_date=date_begin,
                                                  reverse=True,
                                                  limit=None,
                                                  wait_time=2):
            await get_entities(client, message, users, regex)


async def get_clients_with_time(urls, date_begin, regex):
    start = perf_counter()
    users = []
    [await get_clients(url, date_begin, regex, users) for url in urls]
    past_time = int(perf_counter() - start)
    print(f"Обработка групп заняла {past_time // 60} минут {past_time % 60} секунд")
    return users


async def main():
    csv_file_name = await process_csv()
    date_begin = await set_date_begin()
    urls = await get_data_urls()
    regex = await set_data_regex()
    users = await get_clients_with_time(urls, date_begin, regex)
    await write_to_csv(csv_file_name, users)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)

