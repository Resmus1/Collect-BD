import re
import json
import time
import base64
import logging
import os
import traceback
from pathlib import Path
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from multiprocessing import Pool, cpu_count
from urllib.parse import unquote, urlparse
from urllib.parse import quote
from playwright.sync_api import sync_playwright

COMPLETED_FILE = 'completed_regions.json'

file_handler = logging.FileHandler("2GIS.log", mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)  # Лог в файл только важнcое

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
    "Амурская область",
    "Архангельская область",
    "Астраханская область",
    "Белгородская область",
    "Брянская область",
    "Владимирская область",
    "Волгоградская область",
    "Вологодская область",
    "Воронежская область",
    "Ивановская область",
    "Иркутская область",
    "Калининградская область",
    "Калужская область",
    "Камчатская область",
    "Кемеровская область",
    "Кировская область",
    "Костромская область",
    "Курганская область",
    "Курская область",
    "Ленинградская область",
    "Липецкая область",
    "Магаданская область",
    "Московская область",
    "Мурманская область",
    "Нижегородская область",
    "Новгородская область",
    "Новосибирская область",
    "Омская область",
    "Оренбургская область",
    "Орловская область",
    "Пензенская область",
    "Пермская область",
    "Псковская область",
    "Ростовская область",
    "Рязанская область",
    "Самарская область",
    "Саратовская область",
    "Сахалинская область",
    "Свердловская область",
    "Смоленская область",
    "Тамбовская область",
    "Тверская область",
    "Томская область",
    "Тульская область",
    "Тюменская область",
    "Ульяновская область",
    "Челябинская область",
    "Ярославская область",
]

CATEGORY_MAP = {
    "Поесть": ["Кафе", "Рестораны", "Бары", "Доставка еды", "Суши-бары", "Быстрое питание", "Пиццерии", "Фудмолы", "Столовые", "Кофейни", "Точки Кафе", "Готовые блюда", "Кафе-кондитерские", "Кейтеринг", "Чайные клубы", "Рюмочные", "Кофейни автоматы", "Точки безалкогольных напитков"],
    # "Автосервис": ["Авторемонт", "Автомойки", "Шиномонтаж", "Авторазбор", "Заправки", "Кузовной ремонт", "Автосигнализация", "Ремонт грузовиков", "Пункты техосмотра", "Ремонт стартеров, генераторов", "Ремонт ходовой части", "Эвакуатор", "Автоэкспертиза", "Тонирование автостёкол", "Развал-схождение", "Ремонт стёкл", "Газовое оборудование для авто", "Автозвук", "Замена масла", "Тюнинг", "Стоянки", "Паркинг", "Паркоматы", "Сервис климатических систем", "Ремонт бензиновых двигателей", "Компьютерная диагностика автомобилей", "Аварийные комиссары", "Ремонт АКПП", "Диагностика, ремонт дизельных двигателей", "Ремонт спецтехники", "Ремонт топливной аппаратуры дизельных двигателей", "Выездной автосервис", "Ремонт электронных систем управления автомобилем, автосигнализаций", "Пошив ковров на автомобиль", "Ремонт глушителя и выхлопных систем", "Ремонт и тюнинг мототехники", "Детейлинг", "Отогрев автомобиля", "Ремонт, регулировка, промывка карбюраторов и инжекторов", "Сезонное хранение шин", "Ремонт МКПП", "Антикоррозийная обработка авто", "Установка и ремонт автооптики", "Переоборудование автомобилей", "Штрафстоянки", "Ремонт автобусов", "Аэрография на авто, транспорте", "Топливные (заправочные) карты", "Станции зарядки электромобилей", "Автосервис самообслуживания", "Ремонт гибридных автомобилей", "Мобильные АЗС", "Ремонт рулевых реек", "Ремонт турбин", "Ремонт карданных валов", "Устранение запахов сухим туманом и озонированием", "Ультразвуковая мойка деталей", "Резка и ремонт автозеркал", "Ремонт подушек безопасности", "Ремонт карбюраторов", "Пошив чехлов на автомобиль", "Ремонт электроавтомобилей", "Ремонт рефрижераторов", "Онлайн-заправки 2ГИС", "Русификация", "Станции зарядки электросамокатов"],
    # "Красота": [],
    "Развлечения": ["Бани и сауны", "Центры паровых коктейлей", "Кинотеатры", "Караоке-клубы / бары", "Ночные клубы", "Детские развлекательные центры, игровые залы, игротеки", "Квесты", "Пейнтбольные, страйкбольные и лазертаг клубы", "Бильярд (бильярдные клубы)", "Зоопарк", "Антикафе", "Боулинг", "Аквапарки", "Тиры, стрелковые клубы, стрельбища", "Аттракционы", "Аренда беседок", "Букмекеры, лотереи", "Билеты на мероприятия", "Пляжи", "Клубы видеоигр", "Киностудии", "Верёвочные парки", "Киноаттракционы", "Лотерейные билеты", "Парки для водных видов спорта", "Интернет-кафе / Игровые, компьютерные клубы", "Скейт-парки", "Центры виртуальной реальности", "Клубы настольных игр", "Заповедники", "Буккроссинг", "Водные прогулки", "Автоматы фотопечати", "Детские развлекательные автоматы"],
    "Организация праздников": ["Банкетные залы", "Организация и проведение праздников", "Фейерверки, салюты, пиротехника", "Оформление праздников", "Фотограф / видеосъемка на выезде", "Творческие и танцевальные коллективы", "Аренда шатров и конструкций", "Аренда мебели для мероприятий", "Изготовление фотокниг", "Видеосъёмка", "Фотостудии"],
    "Медицина": [],
    "Автотовары": [],
    "Продукты": [],
    "Недвижимость": [],
    "Товары": [],
    "Услуги": [],
    "Туризм": [],
    "Спецмагазины": [],
    "Спорт": [],
    "Образование": [],
    "Ремонт, стройка": [],
    "Пром. Товары": [],
    "В2В-услуги": [],
}


def get_main_category(subcategory: str) -> str:
    for main_cat, sub_cats in CATEGORY_MAP.items():
        if subcategory in sub_cats:
            return main_cat
    return subcategory  # если не нашли, используем подкатегорию как основную


args_list = [
    (region, main_cat, subcat)
    for region in regions
    for main_cat, subcats in CATEGORY_MAP.items()
    for subcat in subcats
]

country = "ru"


def log_failed_region(reason: str, region: str, category: str, extra: str = "", exc: Exception = None):
    """
    Логирует ошибку в failed_regions.txt с подробной информацией.

    :param reason: Причина (например, 'REGION ABORT', 'PAGE ABORT')
    :param region: Название региона
    :param category: Поисковый ключ (категория)
    :param extra: Дополнительные данные (например, страница, время)
    :param exc: Исключение, если есть (Exception)
    """
    path = Path("failed_regions.txt")
    error_info = ""

    if exc:
        error_info = f"{type(exc).__name__}: {str(exc)}"

    line = f"[{reason}] {region}|{category}"
    if extra:
        line += f"|{extra}"
    if error_info:
        line += f" | {error_info}"

    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(line.strip() + "\n")
    except Exception as e:
        print(f"❌ Ошибка при записи в failed_regions.txt: {e}")


def load_completed():
    if os.path.exists(COMPLETED_FILE):
        with open(COMPLETED_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_completed(completed_dict):
    with open(COMPLETED_FILE, 'w', encoding='utf-8') as f:
        json.dump(completed_dict, f, ensure_ascii=False, indent=2)


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


def run_parser_for_region(region, main_category, subcategory, attempt=1):
    logger.info(
        f"🚀 Запуск парсинга: регион = '{region}', категория = '{subcategory}'")

    search_word = subcategory
    old_data = read_json_data(f"output/{region}/{main_category}.json")
    existing_cards = {(x["name"], x["address"])
                       : x for x in old_data if x.get("address")}
    collect_data = []
    count_cards = 0

    def process_data(existing_data, new_card_data):
        name = new_card_data["name"]
        new_card_data["sub_categories"] = [search_word]
        key = (new_card_data["name"], new_card_data.get("address"))
        existing = existing_cards.get(key)

        if existing:
            # Добавим подкатегорию, если новой ещё нет
            existing.setdefault("sub_categories", [])
            if search_word not in existing["sub_categories"]:
                existing["sub_categories"].append(search_word)
                logger.info(
                    f"➕ Добавлена подкатегория '{search_word}' в '{name}'")

            # Обновим, если данные отличаются (упрощённо по ключевым полям)
            updated_fields = ["address", "email", "phones", "website"]
            for field in updated_fields:
                if new_card_data.get(field) and existing.get(field) != new_card_data.get(field):
                    old = existing.get(field)
                    existing[field] = new_card_data[field]
                    logger.debug(
                        f"♻ Обновлено поле '{field}' у '{name}': '{old}' -> '{new_card_data[field]}'")

            # Обновим social-сети (объединить по ключам)
                for net, values in new_card_data.get("socials", {}).items():
                    existing.setdefault("socials", {}).setdefault(net, [])
                    before = set(existing["socials"][net])
                    combined = list(before.union(values))
                    if before != set(combined):
                        existing["socials"][net] = combined
                        logger.debug(f"🔗 Обновлены соцсети '{net}' у '{name}'")

            return

        # Если карточка новая
        logger.info(f"🆕 Добавлена новая карточка '{name}'")
        new_card_data["sub_categories"] = [search_word]
        collect_data.append(new_card_data)
        existing_data.append(new_card_data)
        existing_cards[key] = new_card_data  # добавим в индекс

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

            total_cards = 0

            logger.info(f"🔎 Поиск подкатегории: {search_word}")

            try:
                page.goto(
                    f"https://2gis.{country}/search/{quote(region)}%20{quote(search_word)}",
                    wait_until="domcontentloaded",
                    timeout=5000
                )
            except Exception as e:
                logger.warning(
                    f"⚠ Не удалось открыть страницу с подкатегорией {search_word} в {region}: {e}")

            count_cards, count_pages = get_pagination_info(page)
            total_cards += count_cards

            logger.info(
                f"🔢 Найдено {count_cards} карточек по '{search_word}', страниц: {count_pages}")

            for page_index in range(count_pages + 1):
                start_time_page = time.time()
                logger.info(
                    f"Переход по странице {page_index + 1} из {count_pages + 1}")

                cards = page.locator("._1kf6gff")
                count = cards.count()

                for i in range(count):
                    try:
                        start_time_card = time.time()
                        card = cards.nth(i)

                        # Скролл к элементу
                        try:
                            card.scroll_into_view_if_needed(timeout=1500)
                        except:
                            logger.warning(
                                f"[{i+1}] ⚠ Не удалось проскроллить к карточке")
                            continue

                        if not card.is_visible():
                            logger.debug(
                                f"[{i+1}] ❌ Элемент не виден — пропуск")
                            continue

                        # Клик
                        try:
                            card.click(timeout=1500)
                        except Exception as e:
                            logger.warning(
                                f"[{i+1}] ⚠ Не удалось кликнуть по карточке — элемент перекрыт: {e}")
                            continue  # перейти к следующей карточке

                        # Ждём загрузки карточки
                        try:
                            page.wait_for_selector(
                                "._fjltwx h1", timeout=2000)
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
                            "socials": {k: v for k, v in socials.items()},
                            "main_category": main_category
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
                        logger.info(
                            f"➡ Переход на следующую страницу успешен")

                    except Exception as e:
                        logger.warning(
                            f"❌ Не удалось перейти на следующую страницу в {region}: {e}")
                        logger.info(
                            f"🚫 Останавливаем парсинг {region} на текущей странице и записываем")

                        log_failed_region(
                            reason="PAGE ABORT",
                            region=region,
                            category=subcategory,
                            extra=f"{page_index + 1}|{len(collect_data)}|{round(time.time() - start_time_page, 2)}",
                            exc=e
                        )
                        break
                else:
                    logger.info(
                        f"✅ Последняя страница {region} достигнута")
    except Exception as e:
        logger.error(
            f"❌ Ошибка при запуске браузера для региона {region}: {e}")
        log_failed_region(
            reason="BROWSER ERROR",
            region=region, category=subcategory
        )

    # Сохраняем все карточки из existing_cards
    write_json_data(list(existing_cards.values()), Path(
        "output") / region / f"{main_category}.json")

    logger.info("Программа завершена")
    logger.info(
        f"Всего собрано: {len(collect_data)}, время: {round(time.time() - start_time_program, 2)} сек")

    # Повторный запуск, если данных нет
    if total_cards == 0 and attempt == 1:
        logger.warning(
            f"🔁 Повторный запуск региона '{region}' — первая попытка вернула 0 карточек")
        return run_parser_for_region(region, main_category, subcategory, attempt=2)
    elif total_cards == 0:
        logger.warning(
            f"⚠ Регион '{region}' дал 0 карточек — даже после повтора")
        log_failed_region(
            reason="REGION ABORT",
            region=region, category=subcategory
        )

    logger.info(
        f"✅ Завершён регион '{region}', собрано {len(collect_data)} карточек")
    # Убедись, что всегда возвращается dict
    return {"count": len(collect_data)}


if __name__ == '__main__':
    start_time_all = time.time()
    num_processes = 3
    max_passes = 2
    completed_regions = load_completed()

    for current_pass in range(1, max_passes + 1):
        start_time_pass = time.time()
        logger.info(
            f"🚀 Запуск обхода #{current_pass} со {num_processes} процессами")

        filtered_args_list = [
            args for args in args_list
            if completed_regions.get(f"{args[0]}|{args[2]}", False) is not True
        ]

        with Pool(processes=num_processes) as pool:
            results = []

            for args in filtered_args_list:
                r = pool.apply_async(run_parser_for_region, args=args)
                results.append(r)

            for i, (r, args) in enumerate(zip(results, filtered_args_list)):
                region, category, subcategory = args
                key = f"{region}|{subcategory}"

                try:
                    result = r.get(timeout=600)

                    if result and isinstance(result, dict) and result.get("count", 0) > 0:
                        logger.info(
                            f"✅ Объект #{i + 1} завершён ({key}) — собрано: {result['count']}")
                        completed_regions[key] = True
                        # 💾 сохраняем сразу после успешной подкатегории
                        save_completed(completed_regions)
                    else:
                        logger.warning(
                            f"⚠️ Объект #{i + 1} ({key}) завершён, но данные не получены или пусты")
                except Exception as e:
                    logger.error(
                        f"❌ Ошибка в объекте #{i + 1} ({key}): {type(e).__name__}: {e}")
                    logger.debug(traceback.format_exc())
                    log_failed_region(
                        reason="PROCESS EXCEPTION",
                        region=region, category=subcategory, exc=e
                    )
                    # чтобы не перезапускать в следующем проходе
                    completed_regions[key] = False
                    save_completed(completed_regions)

        save_completed(completed_regions)  # сохраняем после каждого прохода

        pass_time = round(time.time() - start_time_pass, 2)
        logger.info(
            f"📊 Завершено подкатегорий: {sum(v is True for v in completed_regions.values())} из {len(args_list)}")
        logger.info(f"🎉 Обход #{current_pass} завершён за {pass_time} сек")

    total_time = round(time.time() - start_time_all, 2)
    logger.info(f"🏁 Все обходы завершены за {total_time} сек")
