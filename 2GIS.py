from playwright.sync_api import sync_playwright
from urllib.parse import quote
import logging
from urllib.parse import unquote, urlparse
import base64
import time
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
import json
import re


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("2GIS.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


search_word = quote("производство штор")
search_city = "moscow"


def clean_invisible(text):
    return re.sub(r'\u2012|\u00a0|\u200b|\+7', '', text).strip()


def save_to_json(data, filename="output.json"):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Данные успешно сохранены в {filename}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в {filename}: {e}")


def wait_for_count(locator, min_count=1, timeout=3000):
    start_time = time.time()
    while (time.time() - start_time) * 1000 < timeout:
        if locator.count() >= min_count:
            return True
        time.sleep(0.1)
    return False


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
    logger.debug(f"Название: {name}")

    rating_elem = wrapper.locator("._y10azs")
    rating = rating_elem.inner_text() if rating_elem.count() > 0 else ""
    logger.debug(f"Рейтинг: {rating if rating else '❌ Отсутствует'}")

    reviews_elem = wrapper.locator("._jspzdm")
    count_reviews = reviews_elem.text_content(
    ).split()[0] if reviews_elem.count() > 0 else ""
    logger.debug(
        f"Отзывы: {count_reviews if count_reviews else '❌ Отсутствуют'}")

    return name, rating, count_reviews


def get_data_card(wrapper):
    address = None
    website = None
    email = None
    phones = []

    # Адрес
    try:
        address_info = wrapper.locator(
            "div._172gbf8 >> ._49kxlr >> ._13eh3hvq >> ._14quei >> ._wrdavn").nth(0)
        address_info.wait_for(state="visible", timeout=5000)
        address = address_info.inner_text()
        logger.debug(f"Добавлен адрес: {address}")
    except PlaywrightTimeoutError:
        try:
            address_info = wrapper.locator(
                "div._172gbf8 >> ._49kxlr >> ._13eh3hvq >> ._oqoid").nth(0)
            address_info.wait_for(state="visible", timeout=5000)
            address = address_info.inner_text()
            logger.debug(f"Добавлен адрес (резервный селектор): {address}")
        except PlaywrightTimeoutError:

            try:
                address_info = wrapper.locator(
                    "div._172gbf8 >> ._49kxlr >> ._oqoid").nth(0)
                address_info.wait_for(state="visible", timeout=5000)
                address = address_info.inner_text()
                logger.debug(f"Добавлен адрес (резервный селектор): {address}")
            except Exception as e:
                logger.debug("Адрес ❌ не найден по обоим селекторам (таймаут)")
                logger.debug(f"❌ Ошибка при получении адреса (резерв): {e}")
    except Exception as e:
        logger.debug(f"❌ Ошибка при получении адреса: {e}")

    # Email
    try:
        email_info = wrapper.locator(
            'div._172gbf8 >> ._49kxlr >> div >> a[href^="mailto:"]')
        if email_info.count() > 0:
            email = email_info.first.inner_text().strip()
            logger.debug(f"Добавлен email: {email}")
        else:
            logger.debug("Email: ❌ Не найден")
    except Exception as e:
        logger.debug(f"❌ Ошибка при получении email: {e}")

    # Website
    try:
        website_info = wrapper.locator(
            'div._172gbf8 >> ._49kxlr >> div >> a[href^="https://"]')
        if website_info.count() > 0:
            website = website_info.first.inner_text().strip()
            logger.debug(f"Добавлен сайт: {website}")
        else:
            logger.debug("Website: ❌ Не найден")
    except Exception as e:
        logger.debug(f"❌ Ошибка при получении сайта: {e}")

    # Кнопка "Показать все телефоны"
    try:
        view_all_phones = wrapper.locator("._1tkj2hw")
        if view_all_phones.count() > 0:
            view_all_phones.first.wait_for(state="visible", timeout=5000)
            view_all_phones.first.click()
            logger.debug("Телефоны загружены")
    except Exception as e:
        logger.debug(f"❌ Ошибка при клике на кнопку телефонов: {e}")

    # Телефоны
    try:
        phones_info = wrapper.locator(
            'div._172gbf8 >> ._49kxlr >> div >> a[href^="tel:"]')
        for i in range(phones_info.count()):
            phone = phones_info.nth(i).inner_text().strip()
            phones.append(phone)
            logger.debug(f"Добавлен телефон: {phone}")
        if not phones:
            logger.debug("Номера телефонов: ❌ Не найдены")
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
    logger.debug(f"Найдено социальных ссылок: {count}")

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
            logger.debug(f"{name}: {value}")

    return data


def get_pagination_info(page, selector="div._jcreqo >> ._1xhlznaa", max_cards=12):
    count_cards_text = page.locator(selector).text_content()
    count_cards = int(count_cards_text)
    count_pages = count_cards // max_cards
    last_page_count_cards = count_cards % max_cards

    logger.debug(
        f"Количество страниц: {count_pages + 1}, количество карточек на последней странице: {last_page_count_cards}")
    logger.info(f"Количество карточек: {count_cards}")

    return count_cards, count_pages, last_page_count_cards


collect_data = []
try:
    with sync_playwright() as p:
        start_time_program = time.time()
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        logging.debug("Браузер запущен")
        page.goto(f"https://2gis.kz/{search_city}/search/{search_word}")
        logging.debug("Открыта страница поиска")

        count_cards, count_pages, last_page_count_cards = get_pagination_info(
            page)

        for page_index in range(count_pages + 1):
            start_time_page = time.time()
            logger.info(
                f"Переход по странице {page_index + 1} из {count_pages + 1}")
            items = page.locator("._1kf6gff")
            logger.debug(f"Получение общих данных")

            try:
                for i in range(12 if page_index != count_pages else last_page_count_cards):
                    start_time_card = time.time()
                    item = items.nth(i)
                    address = None
                    website = None
                    email = None
                    phones = []

                    logger.info(f"[{i + 1}] Обработка карточки")
                    if item.count() > 0:
                        item.wait_for(state="visible")
                        item.click()
                    wrapper = page.locator("._fjltwx")
                    wrapper.first.wait_for(
                        state="visible", timeout=7000)

                    name, rating, count_reviews = get_header(wrapper)
                    address, email, phones, website = get_data_card(
                        wrapper)
                    socials = get_socials(wrapper)

                    page.locator('div._k1uvy >> svg').nth(
                        0).click()  # Закрытие карточки

                    logger.info(
                        f"[{i+1}] ✅ Успешно обработана за {round(time.time() - start_time_card, 2)} сек")

                    collect_data.append({
                        "name": clean_invisible(name),
                        "rating": clean_invisible(rating),
                        "count_reviews": clean_invisible(count_reviews),
                        "address": clean_invisible(address) if address else None,
                        "email": clean_invisible(email) if email else None,
                        "phones": [clean_invisible(phone) for phone in phones] if phones else None,
                        "website": clean_invisible(website) if website else None,
                        "socials": {k: v for k, v in socials.items()}
                    })

            except Exception as e:
                logger.exception(
                    f"[{i + 1}] ❌ Ошибка при обработке карточки")
            finally:
                logger.info(
                    f"📄 Страница {page_index}, всего собрано: {len(collect_data)}, ✅ Успешно обработана за {round(time.time() - start_time_page, 2)} сек")
                logger.debug(
                    f"Страница: {page_index}, Осталось: {count_pages}")
                if page_index != count_pages:
                    next_buttons = page.locator(
                        'div._1x4k6z7 >> ._n5hmn94 >> svg')
                    if page_index == 0:
                        next_buttons.nth(0).click()
                    else:
                        next_buttons.nth(1).click()
                    logger.debug(
                        f"Нажатие на кнопку перехода на следующую страницу")

                else:
                    logger.info("Дошли до последней страницы, завершаем цикл")
                    logger.info("✅ Цикл завершен")
                    break
except Exception as e:
    logger.error(f"Ошибка при запуске браузера: {e}")
finally:
    logger.info("Программа прекращена")
    save_to_json(collect_data, "output.json")
    logger.info(
        f"Всего собрано: {len(collect_data)}, Выполнена за {round(time.time() - start_time_program, 2)} сек")
