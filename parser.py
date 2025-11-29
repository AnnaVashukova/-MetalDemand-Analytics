import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import time
import hashlib
import pandas as pd


def download_customs_data():
    configs = [
        {
            'name': 'export',
            'base_url': 'https://customs.gov.ru/statistic/eksport-rossii-vazhnejshix-tovarov',
            'download_dir': 'export_data',
            'pages': range(1, 4)
        },
        {
            'name': 'import',
            'base_url': 'https://customs.gov.ru/folder/515',
            'download_dir': 'import_data',
            'pages': range(1, 4)
        }
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    all_downloaded_files = {}
    
    try:
    
        response = session.get("https://customs.gov.ru/", timeout=10)
        
        time.sleep(2)
        
        for config in configs:
            
            if not os.path.exists(config['download_dir']):
                os.makedirs(config['download_dir'])
            
            downloaded_files = []
            
            for page in config['pages']:
                if page == 1:
                    url = config['base_url']
                else:
                    url = f"{config['base_url']}?page={page}"
                
                try:
                    response = session.get(url, timeout=15)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    links = soup.find_all('a', href=True)
                    excel_links = []
                    
                    for link in links:
                        href = link['href']
                        if any(pattern in href for pattern in ['document_statistics_file', '.xlsx', '.xls']):
                            full_url = urljoin('https://customs.gov.ru', href)
                            if full_url not in excel_links:
                                excel_links.append(full_url)
                    
                    for i, excel_url in enumerate(excel_links):
                        try:
                            url_hash = hashlib.md5(excel_url.encode()).hexdigest()[:12]
                            
                            if '.xls' in excel_url.lower():
                                if excel_url.lower().endswith('.xlsx'):
                                    extension = '.xlsx'
                                elif excel_url.lower().endswith('.xls'):
                                    extension = '.xls'
                                else:
                                    extension = '.xlsx'
                            else:
                                extension = '.xlsx'
                            
                            filename = f"{config['name']}_{url_hash}{extension}"
                            filepath = os.path.join(config['download_dir'], filename)
                            
                            if os.path.exists(filepath):
                                file_size = os.path.getsize(filepath)
                                print(f"Файл уже существует: {filename} ({file_size} байт)")
                                downloaded_files.append(filepath)
                                continue
                            
                            file_headers = headers.copy()
                            file_headers.update({
                                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
                                'Referer': url,
                                'Sec-Fetch-Dest': 'document',
                                'Sec-Fetch-Mode': 'navigate',
                                'Sec-Fetch-Site': 'same-origin'
                            })
                            
                            file_response = session.get(excel_url, headers=file_headers, timeout=30)
                            content = file_response.content
                    
                            if len(content) >= 8:
                                if content[:2] == b'PK':
                                    actual_extension = '.xlsx'
                                elif content[:8] == b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1':
                                    actual_extension = '.xls'
                                else:
                                    actual_extension = extension 
                                
                                if actual_extension != extension:
                                    print(f"  Обнаружен реальный формат: {actual_extension}")
                                    new_filename = f"{config['name']}_{url_hash}{actual_extension}"
                                    new_filepath = os.path.join(config['download_dir'], new_filename)
                                    
                                    if os.path.exists(new_filepath):
                                        print(f"Файл уже существует: {new_filename}")
                                        downloaded_files.append(new_filepath)
                                        continue
                                    
                                    filename = new_filename
                                    filepath = new_filepath
                            
                            with open(filepath, 'wb') as f:
                                f.write(content)
                            
                            file_size = os.path.getsize(filepath)
                            if file_size == 0:
                                print(f"Файл пустой - {filename}")
                                os.remove(filepath)
                                continue
                            
                            with open(filepath, 'rb') as f:
                                file_start = f.read(8)
                            
                            is_valid_excel = False
                            if file_start[:2] == b'PK':  # XLSX
                                is_valid_excel = True
                            elif file_start[:8] == b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1':  # XLS
                                is_valid_excel = True
                            
                            if is_valid_excel:
                                downloaded_files.append(filepath)
                            else:
                                print(f"Файл не является валидным Excel: {filename}")
                                try:
                                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                                        content_start = f.read(200)
                                        if '<html' in content_start.lower() or '<!doctype' in content_start.lower():
                                            print("Обнаружена HTML страница вместо файла")
                                except:
                                    pass
                                os.remove(filepath)
                            
                            time.sleep(2)  
                            
                        except Exception as e:
                            print(f"Ошибка при скачивании: {str(e)}")
                    
                except Exception as e:
                    print(f"Ошибка при обработке страницы {page}: {str(e)}")
            
            all_downloaded_files[config['name']] = downloaded_files
        
        total_files = 0
        for data_type, files in all_downloaded_files.items():
            print(f"\n{data_type.upper()} данные:")
            if files:
                for file in files:
                    if os.path.exists(file):
                        size = os.path.getsize(file)
                        total_files += 1
                    else:
                        print(f"  - {os.path.basename(file)} (файл был удален)")
            else:
                print("  - Файлы не скачаны")
        
            
    except Exception as e:
        print(f"Критическая ошибка: {str(e)}")


def download_rosstat_electricity():
    url = "https://rosstat.gov.ru/storage/mediabank/elbalans_2024.xlsx"
    
    session = requests.Session()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://rosstat.gov.ru/',
        'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }
    
    try:
        session.get("https://rosstat.gov.ru/", verify=False, timeout=60)
        
        response = session.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        
        filename = "elbalans_2024.xlsx"
        with open(filename, 'wb') as f:
            f.write(response.content)
        
        df = pd.read_excel(filename, sheet_name=21, header=None)
    
        df = df.iloc[2:].reset_index(drop=True)
        selected_columns = [0] + list(range(13, 21)) 
        
        df = df.iloc[:, selected_columns]
        
        df.columns = ['Регион', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
    
        year_cols = ['2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
        
        for col in year_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['Регион'] = df['Регион'].astype(str).str.strip()
        
        regions_to_remove = [
            'Российская Федерация',
            'Центральный федеральный округ',
            'Северо-Западный федеральный округ', 
            'Южный федеральный округ',
            'Архангельская область',
            'Северо-Кавказский федеральный округ',
            'Приволжский федеральный округ',
            'Уральский федеральный округ',
            'Сибирский федеральный округ',
            'Дальневосточный федеральный округ',
            'Тюменская область'
        ]
        
        df = df[~df['Регион'].isin(regions_to_remove)]
        merged_data = {}
        
        for idx, row in df.iterrows():
            region = row['Регион']
            
            if pd.isna(region) or region == 'nan' or region == '':
                continue
                
            if region not in merged_data:
                merged_data[region] = row.copy()
            else:
                for year in year_cols:
                    current_value = merged_data[region][year]
                    new_value = row[year]
                    
                    if pd.isna(current_value) and not pd.isna(new_value):
                        merged_data[region][year] = new_value
                    elif not pd.isna(current_value) and not pd.isna(new_value):
                        merged_data[region][year] = current_value + new_value
        
        df_merged = pd.DataFrame(list(merged_data.values()))
        
        mask = df_merged[year_cols].notna().any(axis=1) & (df_merged[year_cols] != 0).any(axis=1)
        df_filtered = df_merged[mask].reset_index(drop=True)
        
        if len(df_filtered) > 1:
            second_row_region = df_filtered.loc[1, 'Регион']
            if pd.isna(second_row_region) or second_row_region == 'nan' or second_row_region == '':
                df_filtered = df_filtered.drop(1).reset_index(drop=True)
            else:
                second_row_data = df_filtered.loc[1, year_cols]
                if second_row_data.isna().all() or (second_row_data == 0).all():
                    df_filtered = df_filtered.drop(1).reset_index(drop=True)
        
        df_percent = df_filtered.copy()
        
        for year in year_cols:
            total = df_filtered[year].sum()
            if total > 0:  
                df_percent[year] = (df_filtered[year] / total) * 100
            else:
                df_percent[year] = 0
        
        for year in year_cols:
            df_percent[year] = df_percent[year].round(4)
        
        output_filename = "electricity_consumption_2017-2024_percent.xlsx"
        df_percent.to_excel(output_filename, index=False)
        
        try:
            os.remove(filename)
        except:
            print(f"Не удалось удалить временный файл {filename}")
        
        return output_filename
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return None
    

def download_rosstat_table():
    
    url = "https://rosstat.gov.ru/storage/mediabank/Proizvodstvo_mes_2017-2024.xlsx"
    
    session = requests.Session()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://rosstat.gov.ru/',
        'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }
    
    try:
        
        session.get("https://rosstat.gov.ru/", verify=False, timeout=60)
        
        response = session.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        
        filename = "Proizvodstvo_mes_2017-2024.xlsx"
        with open(filename, 'wb') as f:
            f.write(response.content)
        
        target_sheet = "24"
        
        df = pd.read_excel(filename, sheet_name=target_sheet)
        
        df = df.iloc[2:].reset_index(drop=True)
        
        for i in range(3, len(df)):
            for col in df.columns[1:]: 
                try:
                    value = df.at[i, col]
                    if pd.notna(value) and str(value).replace(',', '').replace('.', '').isdigit():
                        numeric_value = float(str(value).replace(',', '.'))
                        df.at[i, col] = numeric_value / 1000
                except (ValueError, TypeError):
                    pass
        
        metallurgy_filename = "metallurgy_data_processed.xlsx"
        df.to_excel(metallurgy_filename, index=False)
        
    except Exception as e:
        print(f"Ошибка: {e}")
        return None



if __name__ == "__main__":
    download_customs_data()
    download_rosstat_electricity()
    download_rosstat_table()