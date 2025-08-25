import os
import logging
import requests
from bs4 import BeautifulSoup
from config import TARGET_URLS

log_file = "downloader.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def download_latest_files(base_url, download_folder, num_files_to_download=5):
    """
    Скачивает указанное количество последних файлов с веб-сервера.

    Args:
        base_url (str): Базовый URL-адрес сайта.
        download_folder (str): Путь к папке для сохранения файлов.
        num_files_to_download (int): Количество последних файлов для скачивания.
    """
    logger.info(f"Начинаю обработку URL: {base_url}")

    # Создаем папку для скачивания, если её нет
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        logger.info(f"Создана папка: {download_folder}")

    # Получаем список уже скачанных файлов
    try:
        local_files = set(os.listdir(download_folder))
        logger.info(f"Найдено локальных файлов: {len(local_files)}")
    except FileNotFoundError:
        logger.warning(f"Папка {download_folder} не найдена. Проверьте путь.")
        return

    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при загрузке страницы {base_url}: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    pre_tag = soup.find('pre')
    if not pre_tag:
        logger.warning(f"На странице {base_url} не найден тег <pre> с файлами.")
        return

    # Находим все ссылки на файлы, исключая ссылку на родительский каталог
    all_file_links = [a['href'] for a in pre_tag.find_all('a', href=True) if a['href'] != '../']

    if not all_file_links:
        logger.warning(f"На странице {base_url} не найдено ссылок на файлы.")
        return

    # Выбираем N последних файлов
    latest_files = all_file_links[-num_files_to_download:]
    logger.info(f"Будут скачаны {len(latest_files)} последних файлов: {latest_files}")

    # Обрабатываем каждую найденную ссылку
    downloaded_count = 0
    for file_name in latest_files:
        if file_name not in local_files:
            file_url = os.path.join(base_url, file_name)
            file_path = os.path.join(download_folder, file_name)

            logger.info(f"Скачиваю новый файл: {file_name}")
            try:
                with requests.get(file_url, stream=True) as file_response:
                    file_response.raise_for_status()
                    with open(file_path, 'wb') as f:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            f.write(chunk)
                logger.info(f"Файл успешно скачан: {file_name}")
                downloaded_count += 1
            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка при скачивании файла {file_name}: {e}")
        else:
            logger.info(f"Файл {file_name} уже существует, пропускаю.")

    logger.info(f"\nВсего скачано новых файлов: {downloaded_count}")
    logger.info("-" * 50)


if __name__ == "__main__":
    for target_url, target_folder in TARGET_URLS.items():
        download_latest_files(target_url, target_folder, num_files_to_download=5)
