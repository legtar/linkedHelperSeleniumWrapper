import os
import time
import math
import traceback
import requests  # Для Telegram
from datetime import datetime, timedelta
from glob import glob  # Для поиска файлов по шаблону
import logging # Используем logging для вывода

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException,
    ElementClickInterceptedException, StaleElementReferenceException,
    ElementNotInteractableException
)
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService

# --- ЛОГГИРОВАНИЕ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- КОНФИГУРАЦИЯ ---
# Рекомендуется вынести в переменные окружения или config-файл!
LINKEDHELPER_EMAIL = os.environ.get("LH_EMAIL", "aleksey.kuznetsof@gmail.com") # Пример получения из окружения
LINKEDHELPER_PASSWORD = os.environ.get("LH_PASSWORD", "14052201") # Пример получения из окружения
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "764asd9325049:AAEqazi3Dp_cIL-tJiznNb2RlAk7cCCmsjA") # Пример
CHAT_ID = os.environ.get("TG_CHAT_ID", "4572432783873") # Пример

# Динамические имена файлов и папок
CURRENT_DATE_STR = datetime.now().strftime("%d_%m_%Y")
PROFILES_INPUT_FILE = f'profile_urls_{CURRENT_DATE_STR}.csv' # Имя входного файла
PROCESSED_BATCH_LOG_FILE = 'processed_batches.log' # Файл для хранения индекса последнего обработанного батча
DOWNLOAD_DIR = os.getcwd() # Папка для скачивания (текущая директория)
# Паттерн для поиска скачанного файла (динамически создается позже)

# Настройки Selenium и WebDriver
CHROMEDRIVER_PATH = None  # Укажите путь, если chromedriver не в PATH
WEBDRIVER_TIMEOUT = 25 # Увеличенный таймаут для ожидания элементов
IFRAME_TIMEOUT = 60 # Таймаут для ожидания и переключения на iframe

# Настройки процесса
BATCH_SIZE = 50 # Размер батча для загрузки URL
MAX_BATCH_ADD_RETRIES = 3 # Попытки добавить один батч
BATCH_RETRY_WAIT_SECONDS = 5 # Пауза между попытками добавления батча
BATCH_PROCESS_WAIT_TIME = 1 # Пауза между успешными добавлениями батчей (сек)

SECONDS_PER_20_PROFILES_PROCESS = 10 # Коэффициент для расчета ожидания обработки
MIN_PROCESS_WAIT_SECONDS = 15 # Минимальное время ожидания обработки
DOWNLOAD_WAIT_SECONDS = 60  # Сколько секунд ждать скачивания файла перед проверкой
DOWNLOAD_CHECK_INTERVAL = 3 # Интервал проверки наличия файла (сек)
DOWNLOAD_CHECK_ADDITIONAL_TIME = 15 # Дополнительное время на поиск файла после DOWNLOAD_WAIT_SECONDS

# Настройки перезапуска
MAX_SCRIPT_RETRIES = 5 # Максимальное количество перезапусков всего скрипта
RETRY_DELAY_SECONDS = 10 # Пауза между перезапусками

# URL и XPath (Могут потребовать обновления при изменении UI Linked Helper!)
LH_LOGIN_URL = "https://www.linkedhelper.com/member/"
XPATHS = {
    "login_button_div": "//div[contains(text(), 'Log in')]",
    "login_button_button": "//button[contains(text(), 'Log in')]",
    "email_field": "//input[@name='email']",
    "password_field": "//input[@name='password']",
    "instance_status_idle": "//span[contains(text(), 'idle')]",
    "launch_button_idle": "//div[@data-tour-step-name='LaunchInstance']/button[1]",
    "instance_status_stopped": "//span[contains(text(), 'stopped')]",
    "launch_button_stopped": "//div[@data-tour-step-name='LaunchInstance']/span/button[1]",
    "start_remote_button": "//span[contains(text(), 'Start account on remote machine')]", # Не всегда есть
    "iframe_instance": "instance",
    "campaigns_menu_button": "//span[contains(text(), 'Campaigns')]//../div/button",
    "campaign_link_hr_emails": "//a[contains(text(), 'hr emails')]",
    "add_profile_button": "//span[(text()='Add')]",
    "upload_urls_tab": "//div[contains(text(), 'Upload Profiles URLs')]",
    "profile_urls_textarea": "//textarea[@placeholder='Text with LinkedIn profiles URLs']",
    "import_button": "//span[(text()='Import')]",
    "expand_campaign_button": "((//a[contains(text(), 'hr emails')])[1]//../div/button)[2]", # Хрупкий XPath
    "new_queue_link": "((//a[contains(text(), 'hr emails')])[1]//..//..//div[2]//div[2]//div[1])[2]/a", # Хрупкий XPath
    "select_all_new_checkbox": "(//div[(text()='Select rows to see more features')])[2]/../../div[1]/label/input", # Хрупкий XPath
    "add_tag_button": "//div[@style='--table-grid-template-columns-default: min-content;']/div[1]/div[2]/button[5]", # Хрупкий XPath
    "tag_input_field": "(//input[@placeholder='Tag name'])[3]", # Хрупкий XPath
    "save_tag_button": "//span[contains(text(), 'Save')]",
    "start_campaign_button": "(//a[contains(text(), 'hr emails')])[1]//../div/button[1]", # Хрупкий XPath
    "successful_list_link": "(//a[contains(text(), 'hr emails')])[1]//..//..//div[2]//div[4]", # Хрупкий XPath
    "tag_filter_input": "(//input[@placeholder='Tag name'])[1]", # Хрупкий XPath
    "select_all_successful_checkbox": "(//div[(text()='Select rows to see more features')])[2]/../../div[1]/label/input", # Хрупкий XPath
    "download_button": "//div[@style='--table-grid-template-columns-default: min-content;']/div[1]/div[2]/button[2]", # Хрупкий XPath
    "confirm_download_button": "//span[contains(text(), 'Download')]",
    "no_profiles_indicator": "//div[contains(text(), 'No profiles yet')] | //div[contains(text(), 'No items found')]",
}

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def send_telegram_message(message):
    """Отправляет сообщение в Telegram чат."""
    if not BOT_TOKEN or not CHAT_ID:
        logging.warning("[Telegram] BOT_TOKEN или CHAT_ID не заданы. Уведомление не отправлено.")
        return False
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        logging.info("[Telegram] Сообщение успешно отправлено.")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"[Telegram] Ошибка отправки сообщения: {e}")
        return False
    except Exception as e:
        logging.error(f"[Telegram] Непредвиденная ошибка при отправке: {e}")
        return False

def write_last_processed_batch_index(index):
    """Записывает индекс последнего успешно обработанного батча."""
    try:
        with open(PROCESSED_BATCH_LOG_FILE, 'w') as f:
            f.write(str(index))
        logging.info(f"Индекс последнего обработанного батча ({index}) записан в {PROCESSED_BATCH_LOG_FILE}")
    except Exception as e:
        logging.critical(f"Не удалось записать прогресс батчей в {PROCESSED_BATCH_LOG_FILE}: {e}")
        send_telegram_message(f"🚨 КРИТИЧЕСКАЯ ОШИБКА: Не удалось записать прогресс батчей в {PROCESSED_BATCH_LOG_FILE}. Возможна повторная обработка!")
        # raise # Раскомментировать, если нужно остановить скрипт при невозможности записи лога

def read_last_processed_batch_index():
    """Читает индекс последнего успешно обработанного батча."""
    if not os.path.exists(PROCESSED_BATCH_LOG_FILE):
        logging.info(f"Файл лога {PROCESSED_BATCH_LOG_FILE} не найден. Начинаем с первого батча.")
        return -1
    try:
        with open(PROCESSED_BATCH_LOG_FILE, 'r') as f:
            content = f.read().strip()
            if content:
                index = int(content)
                logging.info(f"Найден индекс последнего обработанного батча: {index}")
                return index
            else:
                logging.warning(f"Файл лога {PROCESSED_BATCH_LOG_FILE} пуст. Начинаем с первого батча.")
                return -1
    except ValueError:
        logging.warning(f"Содержимое файла {PROCESSED_BATCH_LOG_FILE} не является числом. Начинаем с первого батча.")
        return -1
    except Exception as e:
        logging.error(f"Ошибка чтения лог-файла {PROCESSED_BATCH_LOG_FILE}: {e}. Начинаем с первого батча.")
        return -1

def read_profile_urls(filename):
    """Читает URL профилей из файла."""
    urls = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip() and line.startswith('http')]
        if not urls:
            logging.warning(f"Файл '{filename}' пуст или не содержит действительных URL.")
        else:
             logging.info(f"Прочитано {len(urls)} URL из файла '{filename}'.")
    except FileNotFoundError:
        logging.error(f"Файл '{filename}' не найден.")
        raise # Прекращаем выполнение, если входной файл не найден
    except Exception as e:
        logging.error(f"Ошибка при чтении файла '{filename}': {e}")
        raise # Прекращаем выполнение при других ошибках чтения
    return urls

def split_into_batches(urls, size):
    """Разделяет список URL на батчи заданного размера."""
    if not urls:
        return []
    return [urls[i:i + size] for i in range(0, len(urls), size)]

def generate_tag_name():
    """Генерирует имя тега на основе текущей даты."""
    # Формат даты с "2" в конце, как в оригинальном скрипте. Убедитесь, что это верно.
    return datetime.now().strftime("%d %m %Y") + "2"

def get_download_file_pattern():
    """Генерирует паттерн для поиска скачанного файла на СЕГОДНЯ."""
    today_date_str = datetime.now().strftime("%Y-%m-%d")
    # Формат: 'Profiles downloaded from lh-* at ГГГГ-ММ-ДД T *.csv'
    return f"Profiles downloaded from lh-* at {today_date_str}T*.csv"

def check_downloaded_file(download_trigger_time, wait_duration, check_interval, file_pattern):
    """
    Проверяет наличие нового CSV файла в папке DOWNLOAD_DIR,
    созданного ПОСЛЕ download_trigger_time.
    """
    logging.info(f"Проверка наличия нового CSV файла по шаблону '{file_pattern}' (ожидание до {wait_duration} сек)...")
    cutoff_time = download_trigger_time - timedelta(seconds=5) # Небольшой запас
    end_wait = time.time() + wait_duration

    while time.time() < end_wait:
        try:
            # Ищем файлы в папке для скачивания
            found_files = glob(os.path.join(DOWNLOAD_DIR, file_pattern))

            for file_path in found_files:
                if os.path.isfile(file_path):
                    try:
                        mod_time_ts = os.path.getmtime(file_path)
                        mod_time_dt = datetime.fromtimestamp(mod_time_ts)

                        if mod_time_dt > cutoff_time:
                            file_name = os.path.basename(file_path)
                            logging.info(f"Найден подходящий новый файл: '{file_name}' (изменен: {mod_time_dt:%Y-%m-%d %H:%M:%S})")
                            return file_name
                    except OSError as e:
                        logging.warning(f"Не удалось получить время модификации для файла '{file_path}': {e}")
        except Exception as e:
            logging.error(f"Ошибка при поиске файла по шаблону: {e}")

        time.sleep(check_interval) # Проверяем каждые N секунд

    logging.warning(f"Новый CSV файл по шаблону '{file_pattern}', измененный после {cutoff_time:%H:%M:%S}, не найден за {wait_duration} сек.")
    return None


# --- КЛАСС АВТОМАТИЗАЦИИ ---

class LinkedHelperAutomator:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.profile_urls = []
        self.batches = []
        self.total_profiles = 0
        self.num_batches = 0
        self.download_trigger_time = None
        self.tag_name = generate_tag_name()

    def _setup_driver(self):
        """Настраивает и инициализирует WebDriver."""
        logging.info("Настройка опций Chrome...")
        chrome_options = ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=en-US") # Попробуем задать язык

        logging.info(f"Файлы будут сохраняться в: {DOWNLOAD_DIR}")
        prefs = {
            "download.default_directory": DOWNLOAD_DIR,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safeBrowse.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        logging.info("Инициализация драйвера Chrome...")
        try:
            if CHROMEDRIVER_PATH and os.path.exists(CHROMEDRIVER_PATH):
                service = ChromeService(executable_path=CHROMEDRIVER_PATH)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                # Пытаемся найти в PATH
                service = ChromeService() # Попробуем так для Selenium 4+
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logging.info("Драйвер Chrome успешно запущен.")
            self.wait = WebDriverWait(self.driver, WEBDRIVER_TIMEOUT)
        except WebDriverException as e:
            logging.critical(f"Ошибка инициализации WebDriver: {e}. Убедитесь, что chromedriver установлен и доступен (или указан CHROMEDRIVER_PATH).")
            raise

    def _login(self):
        """Выполняет вход в Linked Helper."""
        logging.info(f"Переход на страницу входа: {LH_LOGIN_URL}")
        self.driver.get(LH_LOGIN_URL)

        logging.info("Попытка клика 'Log in'...")
        try:
            # Пробуем сначала div
            login_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["login_button_div"])))
            login_button.click()
        except (TimeoutException, ElementNotInteractableException):
             logging.warning("Кнопка 'Log in' (div) не найдена или не кликабельна, пробуем button...")
             try:
                 # Пробуем button
                 login_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["login_button_button"])))
                 login_button.click()
             except Exception as e:
                 logging.error(f"Не удалось найти или кликнуть ни одну из кнопок 'Log in': {e}")
                 raise # Критическая ошибка на этапе логина

        logging.info("Ввод email...")
        email_field = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["email_field"])))
        # email_field.click() # Клик часто не нужен перед send_keys
        email_field.send_keys(LINKEDHELPER_EMAIL)

        logging.info("Ввод пароля...")
        password_field = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["password_field"])))
        password_field.send_keys(LINKEDHELPER_PASSWORD)

        logging.info("Отправка формы входа (Enter)...")
        password_field.send_keys(Keys.ENTER)
        logging.info("Ожидание после входа...")
        # Ожидаем исчезновения поля пароля или появления элемента дашборда
        self.wait.until(EC.staleness_of(password_field)) # Ждем пока поле пароля не исчезнет
        # Или можно ждать появления элемента на следующей странице, например:
        # self.wait.until(EC.visibility_of_element_located((By.XPATH, "XPATH_ПОСЛЕ_ЛОГИНА")))
        logging.info("Вход выполнен успешно.")


    def _launch_instance(self):
        """Запускает инстанс Linked Helper."""
        logging.info("Запуск инстанса...")
        try:
            # Сначала пробуем через 'idle'
            logging.info("Ищем статус 'idle'...")
            idle_status = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["instance_status_idle"])))
            idle_status.click() # Клик по статусу может открыть меню
            logging.info("Клик по кнопке 'Launch'...")
            launch_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["launch_button_idle"])))
            launch_button.click()
            logging.info("Инстанс запущен через 'idle'.")
        except (TimeoutException, ElementNotInteractableException, NoSuchElementException) as e:
            logging.warning(f"Не удалось запустить через 'idle' ({e.__class__.__name__}), пробуем через 'stopped'.")
            try:
                # Пробуем через 'stopped'
                logging.info("Ищем статус 'stopped'...")
                stopped_status = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["instance_status_stopped"])))
                stopped_status.click() # Клик по статусу
                logging.info("Клик по кнопке 'Launch' (для stopped)...")
                launch_button_stopped = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["launch_button_stopped"])))
                launch_button_stopped.click()

                # Иногда появляется доп. кнопка "Start account on remote machine"
                try:
                    logging.info("Проверка наличия кнопки 'Start account on remote machine'...")
                    start_remote_button = WebDriverWait(self.driver, 5).until( # Короткое ожидание
                        EC.element_to_be_clickable((By.XPATH, XPATHS["start_remote_button"]))
                    )
                    logging.info("Клик 'Start account on remote machine'...")
                    start_remote_button.click()
                    # Можно добавить ожидание смены статуса после этого клика, если нужно
                    # time.sleep(5) # Небольшая пауза на всякий случай
                except TimeoutException:
                    logging.info("'Start account on remote machine' не найдена, продолжаем.")

                logging.info("Инстанс запущен через 'stopped'.")

            except Exception as nested_e:
                logging.error(f"Не удалось запустить инстанс ни через 'idle', ни через 'stopped': {nested_e}")
                raise # Критическая ошибка

        logging.info("Ожидание инициализации инстанса...")
        # Вместо sleep, ждем появления iframe или элемента внутри него
        self._switch_to_iframe() # Попытка переключиться сразу

    def _switch_to_iframe(self):
        """Переключается на iframe инстанса."""
        try:
            logging.info(f"Ожидание и переключение на iframe '{XPATHS['iframe_instance']}' (до {IFRAME_TIMEOUT} сек)...")
            self.wait = WebDriverWait(self.driver, IFRAME_TIMEOUT) # Увеличиваем таймаут для iframe
            self.wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, XPATHS["iframe_instance"])))
            self.wait = WebDriverWait(self.driver, WEBDRIVER_TIMEOUT) # Возвращаем обычный таймаут
            logging.info("Успешно переключились на iframe.")
            # Дополнительное ожидание элемента внутри iframe для уверенности
            self.wait.until(EC.visibility_of_element_located((By.XPATH, XPATHS["campaigns_menu_button"])))
            logging.info("Iframe загружен (найден элемент Campaigns).")
        except TimeoutException:
            logging.error(f"Не удалось найти или переключиться на iframe '{XPATHS['iframe_instance']}' за {IFRAME_TIMEOUT} секунд.")
            raise

    def _navigate_to_campaign(self):
        """Навигация к нужной кампании внутри iframe."""
        logging.info("Открытие меню Campaigns...")
        open_campaigns_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["campaigns_menu_button"])))
        open_campaigns_btn.click()

        logging.info("Переход к кампании 'hr emails'...")
        open_emails_link = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["campaign_link_hr_emails"])))
        open_emails_link.click()
        # Ожидание загрузки страницы кампании (например, кнопки Add)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, XPATHS["add_profile_button"])))
        logging.info("Успешно перешли в кампанию 'hr emails'.")

    def _process_one_batch(self, batch_urls, batch_num_display, total_batches):
        """Обрабатывает один батч URL с логикой повторных попыток."""
        batch_text = "\n".join(batch_urls)
        num_profiles = len(batch_urls)
        logging.info(f"Обработка батча {batch_num_display}/{total_batches} ({num_profiles} профилей)...")

        for attempt in range(MAX_BATCH_ADD_RETRIES):
            logging.info(f"  Попытка {attempt + 1}/{MAX_BATCH_ADD_RETRIES}...")
            try:
                # --- Начало блока добавления батча ---
                logging.info("  Вставка URL профилей...")
                # Находим поле ввода КАЖДЫЙ раз перед попыткой
                input_textarea = self.wait.until(
                     EC.visibility_of_element_located((By.XPATH, XPATHS["profile_urls_textarea"]))
                )
                input_textarea.clear() # Очищаем на всякий случай
                input_textarea.send_keys(batch_text)
                # time.sleep(0.5) # Минимальная пауза после вставки, если нужно

                logging.info("  Клик 'Import'...")
                import_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["import_button"])))
                # Дополнительная проверка на видимость перед кликом
                WebDriverWait(self.driver, 3).until(EC.visibility_of(import_button))
                import_button.click()

                # --- Ожидание результата импорта ---
                # Вместо sleep, ждем индикатора успеха или просто короткую паузу,
                # если нет явного индикатора и процесс быстрый.
                # Пример: Ждем, пока кнопка Import снова станет доступна или исчезнет
                try:
                    # Ждем, пока кнопка Import исчезнет ИЛИ текст в поле ввода очистится (выбрать что надежнее)
                     WebDriverWait(self.driver, 15).until(EC.staleness_of(import_button))
                     logging.info(f"  Батч {batch_num_display} - Попытка {attempt + 1} УСПЕШНА.")
                     return True
                except TimeoutException:
                     # Если кнопка не исчезла, возможно, импорт прошел, но UI не обновился как ожидалось
                     # Или можно ждать появления текста "Imported: N" если он есть
                     logging.warning(f"  Не удалось подтвердить успех импорта батча {batch_num_display} по исчезновению кнопки. Считаем успешным.")
                     # time.sleep(1) # Короткая пауза перед след. батчем на всякий случай
                     return True
                # --- Конец блока добавления батча ---

            except (TimeoutException, ElementClickInterceptedException, StaleElementReferenceException, NoSuchElementException, ElementNotInteractableException) as e:
                logging.warning(f"  Ошибка обработки батча {batch_num_display} на попытке {attempt + 1}: {e.__class__.__name__}")
                # traceback.print_exc() # Раскомментировать для детальной отладки
                if attempt < MAX_BATCH_ADD_RETRIES - 1:
                    logging.info(f"  Ожидание {BATCH_RETRY_WAIT_SECONDS} сек перед повтором...")
                    time.sleep(BATCH_RETRY_WAIT_SECONDS)
                    # Попытка обновить страницу может сбросить состояние модального окна, не стоит здесь.
                    # Возможно, нужно закрыть какие-то оверлеи, если они есть.
                else:
                    logging.error(f"  Не удалось обработать батч {batch_num_display} после {MAX_BATCH_ADD_RETRIES} попыток.")
                    return False # Неудача этого батча
            except Exception as e: # Ловим другие возможные ошибки
                  logging.error(f"  Непредвиденная ошибка при обработке батча {batch_num_display} на попытке {attempt + 1}: {e}")
                  traceback.print_exc()
                  if attempt < MAX_BATCH_ADD_RETRIES - 1:
                      logging.info(f"  Ожидание {BATCH_RETRY_WAIT_SECONDS} сек перед повтором...")
                      time.sleep(BATCH_RETRY_WAIT_SECONDS)
                  else:
                      logging.error(f"  Не удалось обработать батч {batch_num_display} после {MAX_BATCH_ADD_RETRIES} попыток из-за непредвиденной ошибки.")
                      return False # Неудача
        # Если цикл завершился без успеха (хотя return должен сработать раньше)
        return False

    def _add_profiles_in_batches(self):
        """Добавляет профили батчами."""
        last_processed_index = read_last_processed_batch_index()
        start_batch_index = last_processed_index + 1

        if start_batch_index >= self.num_batches:
            logging.info("Все батчи уже были обработаны ранее. Пропускаем этап добавления.")
            return True # Считаем этот этап успешным, т.к. делать нечего

        logging.info(f"Начинаем обработку батчей с {start_batch_index + 1} по {self.num_batches}")

        # Открываем окно добавления один раз перед циклом
        logging.info("Клик 'Add' для добавления профилей...")
        add_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["add_profile_button"])))
        add_button.click()

        logging.info("Ожидание окна добавления и клик 'Upload Profiles URLs'...")
        input_profiles_tab = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["upload_urls_tab"])))
        input_profiles_tab.click()
        # Ожидание появления текстового поля после клика на таб
        self.wait.until(EC.visibility_of_element_located((By.XPATH, XPATHS["profile_urls_textarea"])))
        logging.info("Окно добавления URL готово.")


        successful_batches_count = 0
        failed_batches = []

        for i in range(start_batch_index, self.num_batches):
            current_batch_index = i
            batch = self.batches[current_batch_index]
            batch_num_display = current_batch_index + 1

            success = self._process_one_batch(batch, batch_num_display, self.num_batches)

            if success:
                successful_batches_count += 1
                write_last_processed_batch_index(current_batch_index) # Записываем ИНДЕКС успешно обработанного
                if current_batch_index < self.num_batches - 1:
                    if BATCH_PROCESS_WAIT_TIME > 0:
                       logging.debug(f"Пауза {BATCH_PROCESS_WAIT_TIME} сек перед следующим батчем...")
                       time.sleep(BATCH_PROCESS_WAIT_TIME)
            else:
                failed_batches.append(batch_num_display)
                logging.warning(f"Батч {batch_num_display} не был добавлен. Пропускаем его.")
                # Отправка уведомления в Telegram о проблеме с батчем
                send_telegram_message(
                    f"⚠️ Не удалось добавить батч {batch_num_display} кампании 'hr emails' после {MAX_BATCH_ADD_RETRIES} попыток."
                )
                # Продолжаем со следующим батчем, не обновляя лог-файл для этого

        logging.info(f"--- Добавление батчей завершено ---")
        logging.info(f"Успешно добавлено: {successful_batches_count} батчей.")
        if failed_batches:
            logging.warning(f"Не удалось добавить батчи: {', '.join(map(str, failed_batches))}")
            # Можно решить, является ли пропуск батчей критической ошибкой
            # return False # Если пропуск недопустим
        return True # Считаем этап завершенным, даже если были пропуски

    def _tag_profiles_in_queue(self):
        """Выбирает все профили в 'New' (Queue) и добавляет тег."""
        logging.info("Разворачивание деталей кампании...")
        # XPath для разворачивания может быть нестабилен
        try:
            expand_campaign = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["expand_campaign_button"])))
            expand_campaign.click()
            # time.sleep(1) # Короткая пауза после клика, если нужно
        except Exception as e:
            logging.warning(f"Не удалось кликнуть кнопку разворачивания кампании (возможно, уже развернута): {e}")

        logging.info("Переход в 'New' (Queue)...")
        # XPath для New/Queue может быть нестабилен
        new_profile_link = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["new_queue_link"])))
        new_profile_link.click()

        logging.info("Ожидание загрузки списка 'New'...")
        # Ждем появления чекбокса "Выбрать все" или индикатора "No profiles"
        try:
             WebDriverWait(self.driver, 15).until(
                 EC.any_of(
                     EC.element_to_be_clickable((By.XPATH, XPATHS["select_all_new_checkbox"])),
                     EC.visibility_of_element_located((By.XPATH, XPATHS["no_profiles_indicator"]))
                 )
             )
        except TimeoutException:
             logging.warning("Список 'New' не загрузился или не найдены ожидаемые элементы (чекбокс / 'No profiles').")
             # Решаем, продолжать ли. Возможно, стоит сделать скриншот.
             # return False # Если это критично

        # Проверяем, есть ли вообще профили для тегирования
        try:
            self.driver.find_element(By.XPATH, XPATHS["no_profiles_indicator"])
            logging.info("Список 'New' пуст. Пропускаем тегирование.")
            return True # Тегировать нечего, этап успешен
        except NoSuchElementException:
             logging.info("Найдены профили в 'New'. Приступаем к тегированию...")
             # Профили есть, продолжаем

        try:
            logging.info("Выбор всех профилей в 'New'...")
            select_all_new = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["select_all_new_checkbox"])))
            # Иногда нужен JS click
            self.driver.execute_script("arguments[0].click();", select_all_new)
            # time.sleep(1) # Пауза после выбора

            logging.info("Клик 'Add tag'...")
            add_tag_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["add_tag_button"])))
            add_tag_btn.click()

            logging.info(f"Ввод тега: {self.tag_name}...")
            input_tag_field = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["tag_input_field"])))
            input_tag_field.send_keys(self.tag_name)
            # time.sleep(0.5) # Пауза после ввода

            logging.info("Клик 'Save' для тега...")
            save_tag_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["save_tag_button"])))
            save_tag_btn.click()

            logging.info("Ожидание закрытия окна тегирования...")
            # Ждем, пока кнопка Save станет невидимой/неактивной
            self.wait.until(EC.invisibility_of_element_located((By.XPATH, XPATHS["save_tag_button"])))
            logging.info("Тег успешно добавлен.")
            return True

        except (TimeoutException, NoSuchElementException, ElementNotInteractableException) as tag_error:
            logging.error(f"Не удалось добавить тег: {tag_error}")
            # traceback.print_exc() # Для отладки
            return False # Ошибка тегирования

    def _start_campaign_and_wait(self):
        """Запускает кампанию и ожидает ее обработки."""
        logging.info("Клик 'Start campaign'...")
        try:
            start_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["start_campaign_button"])))
            start_btn.click()
            logging.info("Кампания запущена.")
            # Можно добавить ожидание смены статуса кнопки на "Stop campaign" если нужно
        except Exception as e:
            logging.error(f"Не удалось нажать 'Start campaign': {e}")
            return False # Ошибка запуска

        # --- Динамическое ожидание обработки ---
        # Расчет времени ожидания на основе количества *всех* профилей, добавленных в этом запуске
        # Возможно, лучше считать по количеству в 'New' очереди перед запуском?
        profiles_to_process = self.total_profiles # Используем общее количество для расчета

        if profiles_to_process > 0:
            # Формула из оригинального скрипта, уточнена для ясности
            process_wait_time_float = (profiles_to_process / 20.0) * SECONDS_PER_20_PROFILES_PROCESS
            process_wait_time = max(MIN_PROCESS_WAIT_SECONDS, math.ceil(process_wait_time_float))
            logging.info(f"Динамическое ожидание обработки ({profiles_to_process} профилей): {process_wait_time} сек...")
            time.sleep(process_wait_time) # Оставляем sleep, т.к. нет явного индикатора завершения обработки
            logging.info("Динамическое ожидание обработки завершено.")
        else:
            logging.info("Профили для обработки отсутствуют. Пропускаем динамическое ожидание.")
            # time.sleep(MIN_PROCESS_WAIT_SECONDS) # Или минимальная пауза все равно нужна?

        return True

    def _download_successful_profiles(self):
        """Переходит в 'Successful', фильтрует по тегу и скачивает."""
        logging.info("Переход в 'Successful'...")
        try:
            successful_link = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["successful_list_link"])))
            successful_link.click()
            logging.info("Ожидание загрузки списка 'Successful'...")
            # Ждем появления поля фильтра тегов или индикатора "No profiles"
            WebDriverWait(self.driver, 15).until(
                 EC.any_of(
                     EC.visibility_of_element_located((By.XPATH, XPATHS["tag_filter_input"])),
                     EC.visibility_of_element_located((By.XPATH, XPATHS["no_profiles_indicator"]))
                 )
            )
        except Exception as e:
             logging.error(f"Не удалось перейти в 'Successful' или загрузить список: {e}")
             return None # Возвращаем None, если не удалось даже перейти

        # Проверяем, есть ли вообще профили в Successful перед фильтрацией
        try:
            self.driver.find_element(By.XPATH, XPATHS["no_profiles_indicator"])
            logging.info("Список 'Successful' пуст. Скачивание не требуется.")
            return "no_profiles_to_download" # Специальное значение, что скачивать нечего
        except NoSuchElementException:
             logging.info("Найдены профили в 'Successful'. Приступаем к фильтрации и скачиванию...")
             # Профили есть, продолжаем

        try:
            logging.info(f"Фильтрация по тегу: {self.tag_name}...")
            tag_filter_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["tag_filter_input"])))
            tag_filter_input.send_keys(self.tag_name)
            tag_filter_input.send_keys(Keys.ENTER)

            logging.info("Ожидание применения фильтра...")
            # Сложно надежно дождаться фильтрации. Можно подождать обновления таблицы
            # или просто сделать паузу. Или ждать появления чекбокса/no items found
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.any_of(
                         EC.element_to_be_clickable((By.XPATH, XPATHS["select_all_successful_checkbox"])),
                         EC.visibility_of_element_located((By.XPATH, XPATHS["no_profiles_indicator"])) # Тот же индикатор?
                    )
                )
            except TimeoutException:
                 logging.warning("Не удалось подтвердить применение фильтра (не найден чекбокс или 'No items found').")
                 # Продолжаем с осторожностью

            # Проверяем, есть ли профили ПОСЛЕ фильтрации
            try:
                self.driver.find_element(By.XPATH, XPATHS["no_profiles_indicator"])
                logging.info(f"Профили с тегом '{self.tag_name}' не найдены в 'Successful'. Скачивание не требуется.")
                return "no_profiles_to_download" # Скачивать нечего
            except NoSuchElementException:
                 logging.info(f"Найдены профили с тегом '{self.tag_name}'. Приступаем к скачиванию...")
                 # Профили есть, продолжаем

            logging.info("Выбор всех профилей в 'Successful' (после фильтра)...")
            select_all_succ = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["select_all_successful_checkbox"])))
            self.driver.execute_script("arguments[0].click();", select_all_succ)
            # time.sleep(1) # Пауза после выбора

            logging.info("Клик 'Download'...")
            download_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["download_button"])))
            download_btn.click()

            logging.info("Клик подтверждения 'Download'...")
            confirm_download_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, XPATHS["confirm_download_button"])))
            confirm_download_btn.click()

            self.download_trigger_time = datetime.now() # Запоминаем время начала скачивания
            logging.info(f"Скачивание инициировано в {self.download_trigger_time:%H:%M:%S}. Ожидание {DOWNLOAD_WAIT_SECONDS} сек...")
            time.sleep(DOWNLOAD_WAIT_SECONDS) # Оставляем базовое ожидание

            # --- Проверка скачанного файла ---
            file_pattern = get_download_file_pattern()
            downloaded_file_name = check_downloaded_file(
                self.download_trigger_time,
                DOWNLOAD_CHECK_ADDITIONAL_TIME, # Дополнительное время на поиск
                DOWNLOAD_CHECK_INTERVAL,
                file_pattern
            )

            if downloaded_file_name:
                logging.info(f"Успех! Файл '{downloaded_file_name}' найден в {DOWNLOAD_DIR}")
                return downloaded_file_name # Возвращаем имя файла
            else:
                logging.error("ОШИБКА: Скачанный файл не найден после ожидания!")
                return None # Ошибка - файл не найден

        except (TimeoutException, NoSuchElementException, ElementNotInteractableException) as dl_error:
            logging.error(f"Не удалось инициировать скачивание или найти элементы: {dl_error}")
            # traceback.print_exc() # Для отладки
            # Проверим еще раз, может список пуст
            try:
                self.driver.find_element(By.XPATH, XPATHS["no_profiles_indicator"])
                logging.warning("Список 'Successful' пуст или стал пуст во время процесса. Скачивание не удалось, но это ожидаемо.")
                return "no_profiles_to_download" # Считаем, что скачивать нечего
            except NoSuchElementException:
                logging.error("Список 'Successful' НЕ пуст, но произошла ошибка при скачивании.")
                return None # Ошибка скачивания

    def _quit_driver(self):
        """Безопасно закрывает WebDriver."""
        if self.driver:
            try:
                logging.info("Закрытие браузера...")
                self.driver.quit()
                self.driver = None
                self.wait = None
                logging.info("Браузер закрыт.")
            except Exception as e:
                logging.error(f"Ошибка при закрытии браузера: {e}")

    def run(self):
        """Основной метод, запускающий весь процесс автоматизации."""
        downloaded_file_name = None
        try:
            # --- Подготовка ---
            self.profile_urls = read_profile_urls(PROFILES_INPUT_FILE)
            self.total_profiles = len(self.profile_urls)
            if self.total_profiles == 0:
                logging.info("Нет профилей для обработки во входном файле. Завершение.")
                return "no_profiles_in_input" # Успех, но делать нечего

            self.batches = split_into_batches(self.profile_urls, BATCH_SIZE)
            self.num_batches = len(self.batches)
            logging.info(f"Разделено {self.total_profiles} профилей на {self.num_batches} батчей (размер: {BATCH_SIZE}).")

            # --- Запуск WebDriver и логин ---
            self._setup_driver()
            self._login()
            self._launch_instance() # Включает переключение на iframe

            # --- Работа внутри iframe ---
            self._navigate_to_campaign()

            # Добавление профилей (если нужно)
            if not self._add_profiles_in_batches():
                 # Если добавление батчей критично и были ошибки (зависит от логики _add_profiles_in_batches)
                 # logging.error("Этап добавления профилей завершился с ошибками. Прерывание.")
                 # raise Exception("Failed to add profile batches") # Пример прерывания
                 logging.warning("Были ошибки при добавлении некоторых батчей, но продолжаем.")


            # Тегирование профилей в 'New'
            if not self._tag_profiles_in_queue():
                 # Если тегирование критично
                 # logging.error("Этап тегирования завершился с ошибками. Прерывание.")
                 # raise Exception("Failed to tag profiles")
                 logging.warning("Не удалось добавить тег (возможно, список New был пуст или произошла ошибка). Пропускаем запуск кампании и ожидание.")
            else:
                 # Запуск кампании и ожидание (только если тегирование успешно)
                 if not self._start_campaign_and_wait():
                      # Если запуск или ожидание критичны
                      # raise Exception("Failed to start campaign or wait")
                      logging.warning("Не удалось запустить кампанию или дождаться обработки.")


            # Скачивание результатов
            download_result = self._download_successful_profiles()

            if download_result == "no_profiles_to_download":
                logging.info("Скачивание не производилось (профили не найдены в Successful или после фильтра).")
                downloaded_file_name = "skipped" # Специальный маркер успеха без файла
            elif download_result is None:
                 logging.error("Этап скачивания завершился с ошибкой.")
                 # Решаем, критично ли это
                 # raise Exception("Failed to download results")
                 downloaded_file_name = None # Явно указываем на ошибку
            else:
                 downloaded_file_name = download_result # Имя скачанного файла


            # --- Выход из iframe ---
            logging.info("Выход из iframe (возврат к основному контенту)...")
            self.driver.switch_to.default_content()
            logging.info("Работа с iframe завершена.")

            return downloaded_file_name # Возвращаем имя файла или "skipped" или None

        except Exception as e:
            logging.error(f"Произошла ошибка в основном потоке выполнения: {e.__class__.__name__} - {e}")
            traceback.print_exc()
            # Попытка сделать скриншот перед падением
            # self._save_debug_screenshot("error_state") # Нужно реализовать _save_debug_screenshot
            raise # Передаем ошибку выше для обработки в цикле перезапуска
        finally:
            self._quit_driver()


# --- Основной блок выполнения с перезапусками ---
if __name__ == "__main__":
    retries = 0
    final_success = False
    final_message = ""
    result_file = None
    last_exception = None

    while retries < MAX_SCRIPT_RETRIES:
        logging.info(f"\n--- Запуск автоматизации: Попытка {retries + 1} из {MAX_SCRIPT_RETRIES} ---")
        automator = LinkedHelperAutomator()
        try:
            # Запускаем основную логику
            result = automator.run()

            if result == "no_profiles_in_input":
                 final_success = True
                 final_message = f"✅ Скрипт завершен (Попытка {retries + 1}): Входной файл {PROFILES_INPUT_FILE} пуст или не содержит URL."
                 logging.info(final_message)
                 break
            elif result == "skipped": # Успех, но файл не скачивался (не было профилей)
                final_success = True
                final_message = f"✅ Скрипт успешно завершен (Попытка {retries + 1}). Профили в 'Successful' с нужным тегом отсутствовали, файл не скачивался."
                logging.info(final_message)
                break
            elif result: # Успех, файл скачан
                final_success = True
                result_file = result
                final_message = f"✅ Скрипт успешно завершен (Попытка {retries + 1}). Файл '{result_file}' скачан в {DOWNLOAD_DIR}."
                logging.info(final_message)
                break
            else: # Ошибка: run() вернул None (вероятно, ошибка скачивания или другая неустранимая ошибка внутри run)
                final_success = False
                # Сообщение об ошибке уже должно быть в логе из automator.run()
                final_message = f"⚠️ Ошибка Попытки {retries + 1}: Выполнение метода run() завершилось с ошибкой (возможно, скачивание не удалось). Смотрите логи выше."
                logging.error(final_message)
                # last_exception = ? # Сложно определить конкретное исключение здесь
                # Продолжаем попытки

        except FileNotFoundError as e: # Критическая ошибка - нет входного файла
             final_success = False
             last_exception = e
             final_message = f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}. Невозможно продолжить."
             logging.critical(final_message)
             send_telegram_message(final_message)
             break # Прерываем перезапуски

        except WebDriverException as e: # Ошибка Selenium/WebDriver
            final_success = False
            last_exception = e
            error_details = str(e).split('\n')[0] # Краткое описание
            final_message = f"⚠️ Ошибка WebDriver (Попытка {retries + 1}): {e.__class__.__name__}. {error_details}"
            logging.error(final_message, exc_info=True) # Логгируем с traceback
            send_telegram_message(f"{final_message}\nПроблема с chromedriver или браузером.")
            # Продолжаем попытки

        except Exception as e: # Другие непредвиденные ошибки
            final_success = False
            last_exception = e
            final_message = f"⚠️ Непредвиденная ошибка выполнения (Попытка {retries + 1}): {e.__class__.__name__} - {e}"
            logging.error(final_message, exc_info=True) # Логгируем с traceback
            send_telegram_message(f"{final_message}\n{str(e)[:200]}") # Отправляем кратко в телеграм
            # Продолжаем попытки

        # Если дошли сюда без break, значит была ошибка и нужно повторить
        retries += 1
        if retries < MAX_SCRIPT_RETRIES:
            logging.info(f"Пауза {RETRY_DELAY_SECONDS} секунд перед перезапуском...")
            time.sleep(RETRY_DELAY_SECONDS)
        elif not final_success: # Если это была последняя попытка и она не успешна
            final_message = f"❌ Скрипт НЕ завершился успешно после {MAX_SCRIPT_RETRIES} попыток."
            logging.error(final_message)
            # Уведомление об окончательной неудаче отправится ниже

    # Финальное уведомление об общем итоге
    if final_message:
         # Отправляем финальный статус, если это не была критическая ошибка FileNotFoundError
         # или если это был успех
         if final_success or not isinstance(last_exception, FileNotFoundError):
              send_telegram_message(final_message)
         elif isinstance(last_exception, FileNotFoundError):
              # Сообщение о FileNotFoundError уже было отправлено как критическое
              pass

    logging.info("\n--- Завершение работы скрипта ---")