import re
import json
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
import time
import base64
from urllib.parse import unquote, urlparse
import logging
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


country = "kz"
search_word = "Автосервис"
region = "Павлодарская область"


def clean_invisible(text):
    return re.sub(r'\u2012|\u00a0|\u200b|\+7', '', text).strip()


def write_json_data(data, filename=f"output/{search_word}.json"):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ Данные успешно сохранены в {search_word}")
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении данных в {filename}: {e}")


def read_json_data(filename=f"output/{search_word}.json"):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении данных из {filename}: {e}")
        return []


def process_data(existing_data, new_card_data, index):
    names_existing_data = [x["name"] for x in existing_data]

    if new_card_data["name"] not in names_existing_data:
        collect_data.append(new_card_data)
        old_data.append(new_card_data)


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

    rating_elem = wrapper.locator("._y10azs")
    rating = rating_elem.inner_text() if rating_elem.count() > 0 else ""

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
    # Приоритет: сначала проверяем то, что чаще работает
    selectors = [
        "div._172gbf8 >> ._49kxlr >> ._13eh3hvq >> ._oqoid",               # резерв
        "div._172gbf8 >> ._49kxlr >> ._oqoid",                             # второй резерв
        # основной (раньше стоял первым)
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

    # Телефоны
    try:
        phones_info = wrapper.locator(
            'div._172gbf8 >> ._49kxlr >> div >> a[href^="tel:"]')
        for i in range(phones_info.count()):
            phone = phones_info.nth(i).inner_text().strip()
            phones.append(phone)
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
        count_cards_text = page.locator(selector).inner_text(timeout=500)
        count_cards = int(count_cards_text)
    except:
        count_cards = 0
    count_pages = count_cards // max_cards
    last_page_count_cards = count_cards % max_cards
    return count_cards, count_pages, last_page_count_cards


old_data = read_json_data()
existing_names = set(x["name"] for x in old_data)
collect_data = []

try:
    with sync_playwright() as p:
        start_time_program = time.time()
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        context.set_default_timeout(3000)
        page = context.new_page()
        page.set_default_timeout(3000)

        page.goto(
            f"https://2gis.{country}/search/{quote(region)}%20{quote(search_word)}",
            wait_until="domcontentloaded",
            timeout=5000
        )

        count_cards, count_pages, last_page_count_cards = get_pagination_info(
            page)

        for page_index in range(count_pages + 1):
            start_time_page = time.time()
            logger.info(
                f"Переход по странице {page_index + 1} из {count_pages + 1}")
            items = page.locator("._1kf6gff")
            count = items.count()

            for i in range(min(12, count if page_index != count_pages else last_page_count_cards)):
                try:
                    start_time_card = time.time()
                    item = items.nth(i)
                    if not item.is_visible():
                        continue
                    item.click()
                    wrapper = page.locator("._fjltwx")

                    preview_name = wrapper.locator("h1").text_content().strip()
                    if preview_name in existing_names:
                        logger.debug(
                            f"[{i + 1}] ⏩ {preview_name} уже в базе, пропуск")
                        continue
                    else:
                        existing_names.add(preview_name)

                    if wrapper.count() == 0:
                        logger.warning(f"[{i + 1}] ❌ Карточка не загрузилась")
                        continue

                    name, rating, count_reviews = get_header(wrapper)
                    address, email, phones, website = get_data_card(wrapper)
                    socials = get_socials(wrapper)

                    close_button = page.locator('div._k1uvy >> svg')
                    if close_button.count() > 0:
                        close_button.nth(0).click()

                    logger.info(
                        f"[{i + 1}] ✅ {name} Успешно обработана за {round(time.time() - start_time_card, 2)} сек")

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

                    process_data(old_data, data_card, i)
                except Exception as e:
                    logger.exception(
                        f"[{i + 1}] ❌ Ошибка при обработке карточки")

            logger.info(
                f"📄 Страница {page_index + 1}, собрано: {len(collect_data)}, время: {round(time.time() - start_time_page, 2)} сек")

            if page_index != count_pages:
                next_buttons = page.locator('div._1x4k6z7 >> ._n5hmn94 >> svg')
                if next_buttons.count() > 0:
                    try:
                        next_buttons.nth(1 if page_index > 0 else 0).click()
                    except Exception as e:
                        logger.warning(f"Не удалось нажать 'дальше': {e}")
            else:
                logger.info("✅ Последняя страница достигнута")

except Exception as e:
    logger.error(f"❌ Ошибка при запуске браузера: {e}")
finally:
    write_json_data(old_data)  # Сохраняем только один раз
    logger.info("Программа завершена")
    logger.info(
        f"Всего собрано: {len(collect_data)}, время работы: {round(time.time() - start_time_program, 2)} сек")
