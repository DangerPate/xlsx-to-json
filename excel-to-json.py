import pandas as pd
import json
from datetime import datetime


def excel_to_json_custom(file_path, column_mapping=None, output_path=None):
    # Читаем файл без заголовков, чтобы контролировать все самостоятельно
    df_raw = pd.read_excel(file_path, header=None)

    data_start_row = 12
    df_data = df_raw.iloc[data_start_row:].reset_index(drop=True)

    # Применяем маппинг к столбцам
    # Сначала даем столбцам временные названия
    df_data.columns = [f'col_{i}' for i in range(len(df_data.columns))]

    # Преобразуем в словари с правильными названиями
    result = []

    for _, row in df_data.iterrows():
        # Сначала собираем все значения
        temp_values = {}
        for col_idx, col_name in column_mapping.items():
            value = row[f'col_{col_idx}']
            # Заменяем NaN на None
            if pd.isna(value):
                value = None
            temp_values[col_name] = value

        # Обработка периода: разделяем на start_date и end_date
        regulation_start_date = None
        regulation_end_date = None

        if temp_values.get("regulation_period") is not None:
            period_value = temp_values["regulation_period"]
            if isinstance(period_value, str) and '-' in period_value:
                dates = period_value.split('-')
                if len(dates) >= 1:
                    try:
                        start_date_obj = datetime.strptime(dates[0].strip(), "%d.%m.%Y")
                        regulation_start_date = start_date_obj.strftime("%Y-%m-%d")
                    except ValueError:
                        regulation_start_date = dates[0].strip()
                if len(dates) >= 2:
                    try:
                        end_date_obj = datetime.strptime(dates[1].strip(), "%d.%m.%Y")
                        regulation_end_date = end_date_obj.strftime("%Y-%m-%d")
                    except ValueError:
                        regulation_end_date = dates[1].strip()
            else:
                # Если только одна дата
                try:
                    date_obj = datetime.strptime(str(period_value).strip(), "%d.%m.%Y")
                    regulation_start_date = date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    regulation_start_date = period_value

        item = {
            "regulation_quantity_hunting_resources": {
                "name": temp_values.get("name"),
                "regulation_decision": temp_values.get("regulation_decision"),
                "hunt_res_type": {
                    "name": temp_values.get("hunt_res_type")
                },
                "gender_hunt_res": {
                    "male_younger_1_year_quantity": temp_values.get("male_younger_1_year_quantity"),
                    "male_older_1_year_quantity": temp_values.get("male_older_1_year_quantity"),
                    "female_younger_1_year_quantity": temp_values.get("female_younger_1_year_quantity"),
                    "female_older_1_year_quantity": temp_values.get("female_older_1_year_quantity"),
                },
                "plan_mining_hunt_res_quantity": temp_values.get("plan_mining_hunt_res_quantity"),
                "regulation_start_date": regulation_start_date,
                "regulation_end_date": regulation_end_date,
                "regulation_basis_id": {
                    "name": temp_values.get("regulation_basis_id")
                }
            }
        }

        # Добавляем только если есть хотя бы одно непустое значение
        if any(v is not None for v in item.values()):
            result.append(item)

    # Если указан путь для сохранения, сохраняем в файл
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"Данные успешно сохранены в {output_path}")

    return result


if __name__ == "__main__":
    input_file = "testfile1.xlsx"
    output_file = "output.json"

    custom_mapping = {
        0: "id",  # № п/п
        1: "name",  # Наименование охотничьих угодий или иных территорий
        2: "regulation_basis_id",  # Причины регулирования численности
        3: "regulation_decision",  # Реквизиты решения
        4: "hunt_res_type",  # Вид охотничьих ресурсов
        5: "plan_mining_hunt_res_quantity",  # Количество добытых, особей (всего)
        6: "male_younger_1_year_quantity",  # самцы до 1 года
        7: "male_older_1_year_quantity",  # самцы старше 1 года
        8: "female_younger_1_year_quantity",  # самки до 1 года
        9: "female_older_1_year_quantity",  # самки старше 1 года
        10: "regulation_period"  # Сроки проведения мероприятий
    }
    try:
        # Парсим данные с кастомным маппингом
        json_data = excel_to_json_custom(input_file, custom_mapping, output_file)

    except FileNotFoundError:
        print(f"Ошибка: Файл '{input_file}' не найден.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
