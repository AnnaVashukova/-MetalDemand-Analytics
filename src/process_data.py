import pandas as pd
import os
import glob
import re
import warnings

warnings.filterwarnings("ignore")

BASE_DIR = 'data' 
OUTPUT_FILE = 'final_demand_metallurgy_clean.xlsx'

MONTHS_RU = {
    'январь': 1, 'января': 1,
    'февраль': 2, 'февраля': 2,
    'март': 3, 'марта': 3,
    'апрель': 4, 'апреля': 4,
    'май': 5, 'мая': 5,
    'июнь': 6, 'июня': 6,
    'июль': 7, 'июля': 7,
    'август': 8, 'августа': 8,
    'сентябрь': 9, 'сентября': 9,
    'октябрь': 10, 'октября': 10,
    'ноябрь': 11, 'ноября': 11,
    'декабрь': 12, 'декабря': 12
}


BRIDGE_MAP = {
    "Руда железная товарная необогащенная, тыс.т": ["2601"],
    "Концентрат железорудный, тыс.т": ["260111"], 
    "Чугун зеркальный и передельный в чушках, болванках или в прочих первичных формах, тыс.т": ["7201"],
    "Сталь нелегированная в слитках или в прочих первичных формах и полуфабрикаты из нелегированной стали, т": ["7206", "7207"],
    "Сталь нержавеющая в слитках или прочих первичных формах и полуфабрикаты из нержавеющей стали,т": ["7218"],
    "Сталь легированная прочая в слитках или в прочих первичных формах и полуфабрикаты из прочей легированной стали,т": ["7224"],
    "Прокат готовый, т": ["7208", "7209", "7210", "7211", "7212", "7213", "7214", "7215", "7216", "7219", "7220", "7221", "7222", "7225", "7226", "7227", "7228"],
    "Трубы, профили пустотелые и их фитинги стальные, т": ["7304", "7305", "7306", "7307"]
}


def clean_product_name(name):
    clean = re.sub(r',?\s*(тыс\.?)?\s*т\.?$', '', str(name), flags=re.IGNORECASE)
    return clean.strip()

def read_excel_robust(filepath):
    engines = [None, 'xlrd', 'openpyxl']
    for eng in engines:
        try:
            return pd.read_excel(filepath, header=None, engine=eng)
        except:
            continue
    return None

def extract_date_from_header(df_head):
    text_blob = " ".join(df_head.astype(str).sum().tolist()).lower()
    
    year_matches = re.findall(r'20(1[7-9]|2[0-9])', text_blob)
    if not year_matches:
        return None, None
    year = int("20" + year_matches[-1])
    found_months = []
    clean_text = re.sub(r'[^\w\s]', ' ', text_blob)
    for word in clean_text.split():
        if word in MONTHS_RU:
            found_months.append(MONTHS_RU[word])
    
    if not found_months:
        return None, None
    
    return year, found_months[-1]

def find_header_row_and_code_col(df):
    for idx, row in df.head(40).iterrows():
        row_str = row.astype(str).str.lower().tolist()
        
        has_code = any("код" in s for s in row_str)
        has_tn = any("тн" in s and "вэд" in s for s in row_str)
        has_name = any("наименование" in s for s in row_str)
        
        if (has_code and has_tn) or (has_code and has_name):
            col_idx = -1
            for c_i, val in enumerate(row_str):
                if "код" in val:
                    col_idx = c_i
                    break
            return idx, col_idx
    return None, None

def find_strict_thousand_tonnes_col(df, header_row_idx):
    candidates = []
    num_cols = df.shape[1]
    rows_to_scan = [header_row_idx, header_row_idx + 1, header_row_idx + 2]
    
    for c in range(num_cols):
        col_text = ""
        for r in rows_to_scan:
            if r < len(df):
                val = str(df.iloc[r, c]).lower()
                if val != 'nan':
                    col_text += " " + val
        
        has_thous = "тыс" in col_text
        has_ton = "тонн" in col_text or " т." in col_text or " т " in col_text or re.search(r'\d\s*т$', col_text)
        has_bad = any(x in col_text for x in ["долл", "usd", "руб", "стоим", "цена", "%", "темп", "рост", "млн"])
        
        if has_thous and has_ton and not has_bad:
            candidates.append(c)

    if not candidates:
        return None
    return candidates[-1]

def is_code_match(row_code_str, target_codes_list):
    raw = str(row_code_str).strip()
    if "(" in raw or "кроме" in raw.lower():
        return False
        
    range_match = re.match(r'^(\d{4})\s*-\s*(\d{4})$', raw)
    if range_match:
        start_code = int(range_match.group(1))
        end_code = int(range_match.group(2))
        for t in target_codes_list:
            if len(t) >= 4 and t.isdigit():
                t_val = int(t[:4])
                if start_code <= t_val <= end_code:
                    return True
        return False
    
    clean_digits = "".join(filter(str.isdigit, raw))
    if len(clean_digits) < 2: 
        return False 
    
    for t in target_codes_list:
        if clean_digits.startswith(t):
            return True
            
    return False

def process_customs_file(filepath, type_name):
    filename = os.path.basename(filepath)
    df_raw = read_excel_robust(filepath)
    
    if df_raw is None:
        print(f"ERROR: Не читается {filename}")
        return []

    year, month = extract_date_from_header(df_raw.head(20))
    if not year or not month:
        print(f"SKIP {filename}: Нет даты.")
        return []

    header_res = find_header_row_and_code_col(df_raw)
    if not header_res or header_res[0] is None:
        print(f"SKIP {filename}: Нет колонки Код.")
        return []
    
    header_row_idx, code_col_idx = header_res

    weight_col_idx = find_strict_thousand_tonnes_col(df_raw, header_row_idx)
    
    if weight_col_idx is None:
        print(f"SKIP {filename}: Нет колонки 'тыс. тонн'.")
        return []

    results = []
    start_data_idx = header_row_idx + 1
    
    for i in range(start_data_idx, len(df_raw)):
        row = df_raw.iloc[i]
        
        code_cell = row[code_col_idx]
        if pd.isna(code_cell): continue
        code_str = str(code_cell).strip()
        
        val_cell = row[weight_col_idx]
        try:
            val_str = str(val_cell).replace('\xa0', '').replace(' ', '').replace(',', '.')
            val_float = float(val_str)
        except:
            continue

        if val_float <= 0: continue

        for prod_name, target_codes in BRIDGE_MAP.items():
            if is_code_match(code_str, target_codes):
                results.append({
                    'Year': year,
                    'Month': month,
                    'Product': prod_name, 
                    type_name: val_float
                })

    return results

def process_production_folder(folder_path):
    files = glob.glob(os.path.join(folder_path, "*.xlsx"))
    if not files:
        print("Файл производства не найден.")
        return pd.DataFrame()
    
    filepath = files[0]

    try:
        df = pd.read_excel(filepath)
    except:
        return pd.DataFrame()

    start_row = 0
    name_col_idx = 0
    
    for r in range(min(20, len(df))):
        row_vals = df.iloc[r].astype(str).tolist()
        if any("Руда" in s or "Чугун" in s for s in row_vals):
            start_row = r
            for c, val in enumerate(row_vals):
                if "Руда" in val or "Чугун" in val:
                    name_col_idx = c
                    break
            break
            
    df_data = df.iloc[start_row:].copy()
    result_data = []
    
    start_year = 2017
    start_month = 1
    
    for _, row in df_data.iterrows():
        prod_name_raw = str(row.iloc[name_col_idx]).strip()
        matched_key = None
        
        if prod_name_raw in BRIDGE_MAP:
            matched_key = prod_name_raw
        else:
            for k in BRIDGE_MAP.keys():
                if k.replace(" ", "") == prod_name_raw.replace(" ", ""):
                    matched_key = k
                    break
                if k in prod_name_raw:
                    matched_key = k
                    break
        
        if not matched_key:
            continue
            
        current_y = start_year
        current_m = start_month
        data_cols = row.iloc[name_col_idx+1:]
        
        for val in data_cols:
            if current_y > 2024: break
            try:
                val_str = str(val).replace(',', '.').replace('\xa0', '').strip()
                if val_str in ['-', '', 'nan', 'None', '...']:
                    val_float = None
                else:
                    val_float = float(val_str)
            except:
                val_float = None
                
            result_data.append({
                'Year': current_y,
                'Month': current_m,
                'Product': matched_key,
                'Production': val_float
            })
            
            current_m += 1
            if current_m > 12:
                current_m = 1
                current_y += 1
                
    return pd.DataFrame(result_data)


def main():
    
    df_prod = process_production_folder(os.path.join(BASE_DIR, 'production'))
    
    imp_data = []
    imp_files = [f for f in glob.glob(os.path.join(BASE_DIR, 'import', "*.*")) if f.endswith(('.xls', '.xlsx'))]
    for f in imp_files:
        res = process_customs_file(f, 'Import')
        if res: imp_data.extend(res)
            
    df_imp = pd.DataFrame(imp_data)
    if not df_imp.empty:
        df_imp = df_imp.groupby(['Year', 'Month', 'Product'], as_index=False)['Import'].sum()

    exp_data = []
    exp_files = [f for f in glob.glob(os.path.join(BASE_DIR, 'export', "*.*")) if f.endswith(('.xls', '.xlsx'))]
    for f in exp_files:
        res = process_customs_file(f, 'Export')
        if res: exp_data.extend(res)

    df_exp = pd.DataFrame(exp_data)
    if not df_exp.empty:
        df_exp = df_exp.groupby(['Year', 'Month', 'Product'], as_index=False)['Export'].sum()

    dfs = [d for d in [df_prod, df_imp, df_exp] if not d.empty]
    
    if not dfs:
        print("Данные не найдены.")
        return

    df_final = dfs[0]
    for df_next in dfs[1:]:
        df_final = pd.merge(df_final, df_next, on=['Year', 'Month', 'Product'], how='outer')

    if 'Production' not in df_final.columns: df_final['Production'] = None
    if 'Import' not in df_final.columns: df_final['Import'] = None
    if 'Export' not in df_final.columns: df_final['Export'] = None
    
    df_final['Demand'] = df_final['Production'] + df_final['Import'] - df_final['Export']

    df_final['Product'] = df_final['Product'].apply(clean_product_name)

    df_final = df_final[df_final['Year'].between(2017, 2024)]
    df_final = df_final.sort_values(by=['Product', 'Year', 'Month'])
    
    cols = ['Year', 'Month', 'Product', 'Production', 'Import', 'Export', 'Demand']
    df_final = df_final[cols]

    df_final.to_excel(OUTPUT_FILE, index=False)
    print(f"Готово! Результат: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
