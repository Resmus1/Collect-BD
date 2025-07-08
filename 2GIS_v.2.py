import re
import json
import time
import base64
import logging
from pathlib import Path
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from multiprocessing import Pool, cpu_count
from urllib.parse import unquote, urlparse
from urllib.parse import quote
from playwright.sync_api import sync_playwright


file_handler = logging.FileHandler("2GIS.log", mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)  # Лог в файл только важное

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # В консоль всё (если нужно)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)

regions = [
    # "Амурская область",
    # "Архангельская область",
    # "Астраханская область",
    # "Белгородская область",
    # "Брянская область",
    # "Владимирская область",
    # "Волгоградская область",
    # "Вологодская область",
    # "Воронежская область",
    # "Ивановская область",
    # "Иркутская область",
    # "Калининградская область",
    # "Калужская область",
    # "Камчатская область",
    # "Кемеровская область",
    # "Кировская область",
    # "Костромская область",
    # "Курганская область",
    # "Курская область",
    # "Ленинградская область",
    # "Липецкая область",
    "Магаданская область",
    # "Московская область",
    # "Мурманская область",
    # "Нижегородская область",
    # "Новгородская область",
    # "Новосибирская область",
    # "Омская область",
    # "Оренбургская область",
    # "Орловская область",
    # "Пензенская область",
    # "Пермская область",
    # "Псковская область",
    # "Ростовская область",
    # "Рязанская область",
    # "Самарская область",
    # "Саратовская область",
    # "Сахалинская область",
    # "Свердловская область",
    # "Смоленская область",
    # "Тамбовская область",
    # "Тверская область",
    # "Томская область",
    # "Тульская область",
    # "Тюменская область",
    # "Ульяновская область",
    # "Челябинская область",
    # "Ярославская область"
]

search_words = [
    "Автосервис",
    # "Красота",
]

args_list = [(region, word) for word in search_words for region in regions]
country = "ru"


def clean_invisible(text):
    return re.sub(r'\u2012|\u00a0|\u200b|\+7', '', text).strip()


def write_json_data(data: dict, filename: str | Path) -> None:
    path = Path(filename)

    # 1. Создаём все недостающие каталоги
    path.parent.mkdir(parents=True, exist_ok=True)

    # 2. Записываем файл
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ Данные успешно сохранены в {path}")
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении данных в {path}: {e}")


def read_json_data(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении данных из {filename}: {e}")
        return []


def decode_possible_base64_url(url):
    """
    Распознаёт base64-закодированные части в ссылке и возвращает первую строку, начинающуюся с http.
    JSON игнорируется.
    """
    try:
        for part in url.split('/'):
            # Находим base64-подобные фрагменты
            if part.startswith(('aHR0', 'ey')) and len(part) >= 16:
                padded = part + '=' * (-len(part) % 4)
                try:
                    decoded = base64.urlsafe_b64decode(padded).decode(
                        'utf-8', errors='replace').strip()
                except Exception:
                    continue

                # Обработка \n и многострочных base64
                decoded = decoded.replace('\\n', '\n')
                for line in decoded.splitlines():
                    if line.strip().startswith("http"):
                        return line.strip()

                # Если ссылка не найдена, можно вернуть весь декод
                return decoded
    except Exception:
        pass

    # Если ничего не найдено — вернуть оригинальный url
    return url


def get_header(wrapper):
    name = wrapper.locator("h1").text_content()

    try:
        rating = wrapper.locator("._y10azs").inner_text(timeout=300)
    except:
        rating = ""

    reviews_elem = wrapper.locator("._jspzdm")
    count_reviews = reviews_elem.text_content(
    ).split()[0] if reviews_elem.count() > 0 else ""

    return name, rating, count_reviews


def get_data_card(wrapper):
    address = None
    website = None
    email = None
    phones = []

    # Адрес
    selectors = [
        "div._172gbf8 >> ._49kxlr >> ._13eh3hvq >> ._oqoid",               # резерв
        "div._172gbf8 >> ._49kxlr >> ._oqoid",                             # второй резерв
        "div._172gbf8 >> ._49kxlr >> ._13eh3hvq >> ._14quei >> ._wrdavn"
    ]

    for sel in selectors:
        locator = wrapper.locator(sel)
        if locator.count() > 0:
            try:
                address = locator.nth(0).inner_text(timeout=500).strip()
                break
            except Exception as e:
                logger.debug(f"❌ Ошибка при чтении адреса из {sel}: {e}")
    if not address:
        logger.debug("❌ Адрес не найден ни по одному из селекторов")

    # Email
    try:
        email_info = wrapper.locator(
            'div._172gbf8 >> ._49kxlr >> div >> a[href^="mailto:"]')
        if email_info.count() > 0:
            email = email_info.first.inner_text().strip()
    except Exception as e:
        logger.debug(f"❌ Ошибка при получении email: {e}")

    # Website
    try:
        website_info = wrapper.locator(
            'div._172gbf8 >> ._49kxlr >> div >> a[href^="https://"]')
        if website_info.count() > 0:
            website = website_info.first.inner_text().strip()
    except Exception as e:
        logger.debug(f"❌ Ошибка при получении сайта: {e}")

    # Кнопка "Показать все телефоны"
    try:
        view_all_phones = wrapper.locator("._1tkj2hw")
        if view_all_phones.count() > 0:
            view_all_phones.first.click()
    except Exception as e:
        logger.debug(f"❌ Ошибка при клике на кнопку телефонов: {e}")

    # Телефоны (умное ожидание прогрузки)
    try:
        phones = []
        max_attempts = 5
        attempt = 0

        while attempt < max_attempts:
            phones_info = wrapper.locator(
                'div._172gbf8 >> ._49kxlr >> div >> a[href^="tel:"]')
            raw_phones = []

            for i in range(phones_info.count()):
                phone = phones_info.nth(i).inner_text().strip()
                raw_phones.append(phone)

            if all("..." not in p for p in raw_phones):
                phones = raw_phones
                break

            logger.warning(f"⚠ Найдены номера с многоточием: {raw_phones}")
            if view_all_phones.count() > 0:
                logger.debug(
                    f"🔁 Попытка #{attempt + 1} — кликаем повторно по кнопке телефонов")
                view_all_phones.first.click()

            attempt += 1

        # даже если есть "...", сохраняем всё что нашли
        if not phones:
            phones = raw_phones

    except Exception as e:
        logger.debug(f"❌ Ошибка при получении телефонов: {e}")

    return address, email, phones, website


def get_socials(wrapper):
    data = {
        'Instagram': [],
        'Telegram': [],
        'WhatsApp': [],
        'ВКонтакте': [],
        'Одноклассники': [],
        'Facebook': [],
        'Twitter': [],
        'YouTube': []
    }

    links = wrapper.locator(
        '[aria-label="Instagram"], [aria-label="Telegram"], [aria-label="WhatsApp"],'
        '[aria-label="ВКонтакте"], [aria-label="Одноклассники"],'
        '[aria-label="Facebook"], [aria-label="Twitter"], [aria-label="YouTube"]'
    )
    count = links.count()

    for i in range(count):
        link = links.nth(i)
        raw_url = link.get_attribute("href")
        name = link.get_attribute("aria-label")

        if raw_url:
            decoded_url = unquote(raw_url)
            final_data = decode_possible_base64_url(decoded_url)

            if name == "WhatsApp":
                match = re.search(r'wa\.me/(\d+)', final_data)
                value = match.group(1) if match else final_data

            elif name == "Telegram":
                match = re.search(r't\.me/([\w@+]+)', final_data)
                value = f"@{match.group(1)}" if match else final_data

            elif name == "ВКонтакте":
                match = re.search(r'vk\.com/([\w-]+)', final_data)
                value = match.group(1) if match else final_data

            elif name == "Одноклассники":
                match = re.search(r'ok\.ru/([\w-]+)', final_data)
                value = match.group(1) if match else final_data

            elif name == "YouTube":
                match = re.search(r'youtube\.com/(@[\w\d_-]+)', final_data)
                value = match.group(1) if match else final_data

            elif name == "Twitter":
                match = re.search(r'twitter\.com/([\w-]+)', final_data)
                value = match.group(1) if match else final_data

            else:
                try:
                    parsed = urlparse(final_data)
                    value = parsed.netloc + parsed.path
                    value = value.replace("www.", "").lstrip('/')
                except Exception:
                    value = final_data

            data[name].append(value.strip())

    return data


def get_pagination_info(page, selector="div._jcreqo >> ._1xhlznaa", max_cards=12):
    try:
        count_cards_text = page.locator(selector).inner_text(timeout=1500)
        count_cards = int(re.search(r"\d+", count_cards_text).group())
        count_pages = (count_cards + max_cards - 1) // max_cards
        return count_cards, count_pages
    except Exception as e:
        logger.warning(f"⚠ Не удалось получить число карточек: {e}")
        return 0, 1  # безопасное значение


def run_parser_for_region(region, search_word, attempt=1):
    logger.info(
        f"🚀 Запуск парсинга: регион = '{region}', категория = '{search_word}'")
    old_data = read_json_data(f"output/{region}/{search_word}.json")
    existing_names = set(x["name"] for x in old_data)
    collect_data = []

    def process_data(existing_data, new_card_data):
        if new_card_data["name"] not in (x["name"] for x in existing_data):
            collect_data.append(new_card_data)
            existing_data.append(new_card_data)

    try:
        with sync_playwright() as p:
            start_time_program = time.time()
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                permissions=[],
                viewport={"width": 1280, "height": 800},
                java_script_enabled=True,
                record_video_dir=None,
                bypass_csp=True,  # Возможно убрать
            )
            context.set_default_timeout(1500)
            page = context.new_page()
            page.set_default_timeout(3000)

            page.goto(
                f"https://2gis.{country}/search/{quote(region)}%20{quote(search_word)}",
                wait_until="domcontentloaded",
                timeout=5000
            )

            count_cards, count_pages = get_pagination_info(page)
            logger.info(
                f"🔢 Найдено {count_cards} карточек, {count_pages} страниц в {region}")

            for page_index in range(count_pages + 1):
                start_time_page = time.time()
                logger.info(
                    f"Переход по странице {page_index + 1} из {count_pages + 1}")

                card_blocks = page.locator("._awwm2v > div")
                total = card_blocks.count()

                for i in range(total):
                    try:
                        start_time_card = time.time()
                        card = card_blocks.nth(i)
                        card.scroll_into_view_if_needed()
                        page.wait_for_timeout(150)

                        if not card.is_visible():
                            logger.debug(
                                f"[{i+1}] ❌ Элемент не виден — пропуск")
                            continue

                        card.click(timeout=1500)

                        try:
                            page.wait_for_selector("._fjltwx h1", timeout=2000)
                        except:
                            logger.warning(
                                f"[{i+1}] ⚠ Карточка не прогрузилась — пропуск")
                            continue

                        wrapper = page.locator("._fjltwx")
                        if wrapper.count() == 0:
                            logger.warning(
                                f"[{i+1}] ❌ Карточка пуста — пропуск")
                            continue

                        preview_name = wrapper.locator(
                            "h1").text_content().strip()

                        if preview_name in existing_names:
                            logger.debug(
                                f"[{i + 1}] ⏩ {preview_name} уже в базе {region}, пропуск")
                            continue
                        else:
                            existing_names.add(preview_name)

                        if wrapper.count() == 0:
                            logger.warning(
                                f"[{i + 1}] ❌ Карточка в {region} не загрузилась")
                            continue

                        name, rating, count_reviews = get_header(wrapper)
                        address, email, phones, website = get_data_card(
                            wrapper)
                        socials = get_socials(wrapper)

                        close_button = page.locator('div._k1uvy >> svg')
                        if close_button.count() > 0:
                            close_button.nth(0).click()

                        logger.info(
                            f"[{i + 1}] ✅ {name} в {region} Успешно обработана за {round(time.time() - start_time_card, 2)} сек")

                        data_card = {
                            "name": clean_invisible(name),
                            "rating": clean_invisible(rating),
                            "count_reviews": clean_invisible(count_reviews),
                            "address": clean_invisible(address) if address else None,
                            "email": clean_invisible(email) if email else None,
                            "phones": [clean_invisible(phone) for phone in phones] if phones else None,
                            "website": clean_invisible(website) if website else None,
                            "socials": {k: v for k, v in socials.items()}
                        }

                        process_data(old_data, data_card)
                    except Exception as e:
                        logger.exception(
                            f"[{i + 1}] ❌ Ошибка при обработке карточки в {region}")

                logger.info(
                    f"📄 В {region} страница {page_index + 1}, собрано: {len(collect_data)}, время: {round(time.time() - start_time_page, 2)} сек")

                if page_index != count_pages:
                    next_buttons = page.locator(
                        'div._1x4k6z7 >> ._n5hmn94 >> svg')
                    try:
                        if next_buttons.count() > 1:
                            next_buttons.nth(1).click()
                        elif next_buttons.count() == 1:
                            next_buttons.first.click()
                        else:
                            raise Exception("Кнопка 'дальше' не найдена")

                        # Убедимся, что страница изменилась
                        # Можно подстраховаться паузой
                        page.wait_for_timeout(500)
                        logger.info(f"➡ Переход на следующую страницу успешен")
                        page_index += 1

                    except Exception as e:
                        logger.warning(
                            f"❌ Не удалось перейти на следующую страницу в {region}: {e}")
                        logger.info(
                            f"🚫 Останавливаем парсинг {region} на текущей странице")
                        break
                else:
                    logger.info(f"✅ Последняя страница {region} достигнута")
    except Exception as e:
        logger.error(
            f"❌ Ошибка при запуске браузера для региона {region}: {e}")
    finally:
        # Сохраняем только один раз
        write_json_data(
            old_data, Path("output") / region / f"{search_word}.json")
        logger.info("Программа завершена")
        logger.info(
            f"Всего собрано: {len(collect_data)}, время работы: {round(time.time() - start_time_program, 2)} сек")

        if not collect_data:
            if attempt == 1:
                logger.warning(
                    f"🔁 Повторный запуск региона '{region}' — первая попытка вернула 0 карточек")
                return run_parser_for_region(region, search_word, attempt=2)
            else:
                logger.warning(
                    f"⚠ Регион '{region}' дал 0 карточек — даже после повтора")
                with open("failed_regions.txt", "a", encoding="utf-8") as f:
                    f.write(f"{region}|{search_word}\n")


if __name__ == '__main__':
    num_processes = 3
    # сколько раз обходить все регионы (1 — минимум, 2 — твой случай)
    max_passes = 2

    start_time_all = time.time()  # общее время работы

    for current_pass in range(1, max_passes + 1):
        start_time_pass = time.time()  # время одного прохода

        logger.info(
            f"🚀 Запуск обхода #{current_pass} со {num_processes} процессами")

        with Pool(processes=num_processes) as pool:
            results = []
            for args in args_list:
                r = pool.apply_async(run_parser_for_region, args=args)
                results.append(r)

            for i, r in enumerate(results):
                try:
                    r.wait()  # Ждём завершения
                    logger.info(f"✅ Объект #{i + 1} завершён")
                except Exception as e:
                    logger.error(f"❌ Ошибка в объекте #{i + 1}: {e}")

        pass_time = round(time.time() - start_time_pass, 2)
        logger.info(f"🎉 Обход #{current_pass} завершён за {pass_time} сек")

    total_time = round(time.time() - start_time_all, 2)
    logger.info(f"🏁 Все обходы завершены за {total_time} сек")
