import requests #type: ignore
import zipfile
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def downloader() -> int:
    DATA_DIR = os.environ.get("DATA_DIR")
    os.makedirs(f"{DATA_DIR}/temp", exist_ok=True)

    responce = requests.get("https://cloud-api.yandex.net/v1/disk/public/resources/download?",
                                params={
                                    "public_key": "https://disk.yandex.ru/d/TXZjQ6bbuWCo_g"
                                }
                        ) \
                        .json()
    
    DOWNLOAD_API = responce.get("href")
    
    downloading = requests.get(DOWNLOAD_API, stream=True)
    if downloading.status_code == 200:
        with open(f"{DATA_DIR}/temp/archive.zip", "wb") as archiive:
            for chunk in downloading.iter_content(chunk_size=8192):
                archiive.write(chunk)
    else:
        print(f"Ошибка скачивания: {downloading.status_code}")
    
    print("Распаковка...")
    with zipfile.ZipFile(f"{DATA_DIR}/temp/archive.zip", 'r') as zip_ref:
        zip_ref.extractall(f"{DATA_DIR}")

    print("\nПоиск CSV файлов:")
    base_path = Path(DATA_DIR)

    csv_files = list(base_path.rglob('*.csv')) 
    
    if csv_files:
        for csv_path in csv_files:
            print(f"Найден файл: {csv_path}")
            os.system(f"mv -n {csv_path} {DATA_DIR}/aviadata/{str(csv_path).split('/')[-1]}")
    else:
        print("CSV файлы во вложенных папках не найдены.")
        
    os.remove(f"{DATA_DIR}/temp/archive.zip")
    return len(csv_files)
    

print(downloader())