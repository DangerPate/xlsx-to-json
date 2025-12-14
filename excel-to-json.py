import pandas as pd
import json
from datetime import datetime


# парсит excel файл в json с обработкой объединенных ячеек
def excel_to_json_custom(file_path, output_path=None, chunk_size=1000):
    try:
        print(f"Чтение файла {file_path}...")

        df = pd.read_excel(file_path, header=0, dtype=str)
        print(f"Файл прочитан. Строк: {len(df)}, Столбцов: {len(df.columns)}")

        original_columns = list(df.columns)
        df.columns = [str(col).strip() for col in df.columns]

        previous_values = {}
        result = []
        row_count = 0

        for idx, row in df.iterrows():
            row_count += 1

            if row_count % chunk_size == 0:
                print(f"Обработано строк: {row_count}")

            row_dict = {}

            for i, col_name in enumerate(df.columns):
                value = row[col_name]
                original_col = original_columns[i]

                if pd.isna(value) or value is None:
                    if col_name in previous_values:
                        value = previous_values[col_name]
                    else:
                        value = None
                else:
                    previous_values[col_name] = value

                if value is not None:
                    if isinstance(value, datetime):
                        value = value.strftime("%Y-%m-%d")
                    elif isinstance(value, pd.Timestamp):
                        value = value.strftime("%Y-%m-%d")
                    else:
                        try:
                            if str(value).replace('.', '', 1).isdigit():
                                if float(value).is_integer():
                                    value = int(float(value))
                                else:
                                    value = float(value)
                        except:
                            value = str(value).strip()

                row_dict[col_name] = value

            if all(v is None for v in row_dict.values()):
                continue

            json_structure = get_json_template()
            fill_structure_from_row_dict(json_structure, row_dict)

            if has_any_value(json_structure):
                result.append(json_structure)

        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"Данные успешно сохранены в {output_path}")
            print(f"Всего обработано строк: {row_count}")
            print(f"Успешно сохранено записей: {len(result)}")

        return result

    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")


# возвращает пустой шаблон структуры JSON
def get_json_template():
    return {
        "date_entry": None,
        "municipality": {
            "code": None,
            "name": None
        },
        "act_type": None,
        "decision_regul_quan_number": None,
        "decision_regul_quan_date": None,
        "hunting_ground_id": {
            "name": None,
            "municipalities": []
        },
        "detected_disease_id": {
            "hunting_ground_id": {
                "name": None,
                "municipalities": []
            },
            "animal_species": {
                "name": None,
                "name_lat": None
            },
            "diseases": {
                "name": None
            },
            "date_onset_disease": None
        },
        "bound_data": {
            "regulation_quantity_hunting_resources": [],
            "issued_permits_quantity": [],
            "extraction_hunting_resources_results": []
        }
    }


# заполнение структуры json
def fill_structure_from_row_dict(structure, row_data):
    def get_value(key, default=None):
        return row_data.get(key, default)

    simple_fields = {
        "date_entry": "date_entry",
        "act_type": "act_type",
        "decision_regul_quan_number": "decision_regul_quan_number",
        "decision_regul_quan_date": "decision_regul_quan_date"
    }

    for json_key, excel_key in simple_fields.items():
        value = get_value(excel_key)
        if value is not None:
            structure[json_key] = str(value)

    structure["municipality"]["code"] = get_value("municipality/code")
    structure["municipality"]["name"] = get_value("municipality/name")
    structure["hunting_ground_id"]["name"] = get_value("hunting_ground_id/name")

    municipalities = []
    for i in range(2):
        code_key = f"hunting_ground_id/municipalities/{i}/code"
        name_key = f"hunting_ground_id/municipalities/{i}/name"

        code = get_value(code_key)
        name = get_value(name_key)

        if code not in [None, '', 'nan'] or name not in [None, '', 'nan']:
            municipalities.append({
                "code": str(code) if code not in [None, '', 'nan'] else None,
                "name": str(name) if name not in [None, '', 'nan'] else None
            })

    if municipalities:
        structure["hunting_ground_id"]["municipalities"] = municipalities

    detected_disease_data = {
        "hunting_ground_id": {
            "name": get_value("detected_disease_id/hunting_ground_id/name")
        },
        "animal_species": {
            "name": get_value("detected_disease_id/animal_species/name"),
            "name_lat": get_value("detected_disease_id/animal_species/name_lat")
        },
        "diseases": {
            "name": get_value("detected_disease_id/diseases/name")
        },
        "date_onset_disease": get_value("detected_disease_id/date_onset_disease")
    }

    disease_municipalities = []
    for i in range(2):
        code_key = f"detected_disease_id/hunting_ground_id/municipalities/{i}/code"
        name_key = f"detected_disease_id/hunting_ground_id/municipalities/{i}/name"

        code = get_value(code_key)
        name = get_value(name_key)

        if code not in [None, '', 'nan'] or name not in [None, '', 'nan']:
            disease_municipalities.append({
                "code": str(code) if code not in [None, '', 'nan'] else None,
                "name": str(name) if name not in [None, '', 'nan'] else None
            })

    if disease_municipalities:
        detected_disease_data["hunting_ground_id"]["municipalities"] = disease_municipalities

    if has_any_value(detected_disease_data):
        structure["detected_disease_id"] = detected_disease_data

    regulation_item = create_regulation_item(row_data)
    if regulation_item:
        structure["bound_data"]["regulation_quantity_hunting_resources"].append(regulation_item)

    permit_item = create_permit_item(row_data)
    if permit_item:
        structure["bound_data"]["issued_permits_quantity"].append(permit_item)

    extract_item = create_extract_item(row_data)
    if extract_item:
        structure["bound_data"]["extraction_hunting_resources_results"].append(extract_item)


# создание regulation_quantity_hunting_resources
def create_regulation_item(row_data):
    def get_value(key):
        value = row_data.get(key)
        if value in ['nan', '', None]:
            return None
        return value

    regulation_item = {
        "hunt_res_type": {
            "name": None,
            "name_lat": None
        },
        "gender_hunt_res": {
            "name": None
        },
        "enum_age": {
            "name": None
        },
        "plan_mining_hunt_res_quantity": None,
        "regulation_start_date": None,
        "regulation_end_date": None,
        "features": None,
        "quantity_regulation_method_id": {
            "name": None
        },
        "regulation_basis_id": {
            "name": None
        },
        "bound_data": {
            "permitted_hunting_tools": [],
            "permission_using_hunting_products": []
        }
    }

    hunt_res_name = get_value("bound_data/regulation_quantity_hunting_resources/0/hunt_res_type/name")
    hunt_res_name_lat = get_value("bound_data/regulation_quantity_hunting_resources/0/hunt_res_type/name_lat")

    if hunt_res_name or hunt_res_name_lat:
        regulation_item["hunt_res_type"]["name"] = hunt_res_name
        regulation_item["hunt_res_type"]["name_lat"] = hunt_res_name_lat

    regulation_item["gender_hunt_res"]["name"] = get_value(
        "bound_data/regulation_quantity_hunting_resources/0/gender_hunt_res/name"
    )
    regulation_item["enum_age"]["name"] = get_value(
        "bound_data/regulation_quantity_hunting_resources/0/enum_age/name"
    )

    plan_mining = get_value("bound_data/regulation_quantity_hunting_resources/0/plan_mining_hunt_res_quantity")
    if plan_mining is not None:
        try:
            regulation_item["plan_mining_hunt_res_quantity"] = int(plan_mining)
        except (ValueError, TypeError):
            regulation_item["plan_mining_hunt_res_quantity"] = plan_mining

    regulation_item["regulation_start_date"] = get_value(
        "bound_data/regulation_quantity_hunting_resources/0/regulation_start_date"
    )
    regulation_item["regulation_end_date"] = get_value(
        "bound_data/regulation_quantity_hunting_resources/0/regulation_end_date"
    )
    regulation_item["features"] = get_value(
        "bound_data/regulation_quantity_hunting_resources/0/features"
    )
    regulation_item["quantity_regulation_method_id"]["name"] = get_value(
        "bound_data/regulation_quantity_hunting_resources/0/quantity_regulation_method_id/name"
    )
    regulation_item["regulation_basis_id"]["name"] = get_value(
        "bound_data/regulation_quantity_hunting_resources/0/regulation_basis_id/name"
    )

    tool_name = get_value(
        "bound_data/regulation_quantity_hunting_resources/0/bound_data/permitted_hunting_tools/0/hunting_tool_id/name"
    )
    if tool_name:
        regulation_item["bound_data"]["permitted_hunting_tools"].append({
            "hunting_tool_id": {
                "name": str(tool_name)
            }
        })

    product_name = get_value(
        "bound_data/regulation_quantity_hunting_resources/0/bound_data/permission_using_hunting_products/0/using_hunting_products_id/name"
    )
    if product_name:
        regulation_item["bound_data"]["permission_using_hunting_products"].append({
            "using_hunting_products_id": {
                "name": str(product_name)
            }
        })

    if not has_any_value(regulation_item):
        return None

    return regulation_item


# создание issued_permits_quantity
def create_permit_item(row_data):
    def get_value(key):
        value = row_data.get(key)
        if value in ['nan', '', None]:
            return None
        return value

    permit_item = {
        "hunting_permits_quantity": None,
        "bound_data": {
            "issued_permits": []
        }
    }

    permit_quantity = get_value("bound_data/issued_permits_quantity/0/hunting_permits_quantity")
    if permit_quantity is not None:
        try:
            permit_item["hunting_permits_quantity"] = int(permit_quantity)
        except (ValueError, TypeError):
            permit_item["hunting_permits_quantity"] = permit_quantity

    series = get_value(
        "bound_data/issued_permits_quantity/0/bound_data/issued_permits/0/hunting_permit_id/series_permission"
    )
    number = get_value(
        "bound_data/issued_permits_quantity/0/bound_data/issued_permits/0/hunting_permit_id/number_permission"
    )
    date_perm = get_value(
        "bound_data/issued_permits_quantity/0/bound_data/issued_permits/0/hunting_permit_id/date_permission"
    )

    if series is not None or number is not None or date_perm is not None:
        permit_item["bound_data"]["issued_permits"].append({
            "hunting_permit_id": {
                "series_permission": str(series) if series is not None else None,
                "number_permission": str(number) if number is not None else None,
                "date_permission": str(date_perm) if date_perm is not None else None
            }
        })

    if not has_any_value(permit_item):
        return None

    return permit_item


# создание extraction_hunting_resources_results
def create_extract_item(row_data):
    def get_value(key):
        value = row_data.get(key)
        if value in ['nan', '', None]:
            return None
        return value

    extract_item = {
        "hunt_res_type": {
            "name": None,
            "name_lat": None
        },
        "total_individuals_extracted": None,
        "male_younger_1_year_quantity": None,
        "female_younger_1_year_quantity": None,
        "male_older_1_year_quantity": None,
        "female_older_1_year_quantity": None
    }

    hunt_res_name = get_value("bound_data/extraction_hunting_resources_results/0/hunt_res_type/name")
    hunt_res_name_lat = get_value("bound_data/extraction_hunting_resources_results/0/hunt_res_type/name_lat")

    if hunt_res_name or hunt_res_name_lat:
        extract_item["hunt_res_type"]["name"] = hunt_res_name
        extract_item["hunt_res_type"]["name_lat"] = hunt_res_name_lat

    numeric_fields = [
        "total_individuals_extracted",
        "male_younger_1_year_quantity",
        "female_younger_1_year_quantity",
        "male_older_1_year_quantity",
        "female_older_1_year_quantity"
    ]

    for field in numeric_fields:
        excel_key = f"bound_data/extraction_hunting_resources_results/0/{field}"
        value = get_value(excel_key)
        if value is not None:
            try:
                extract_item[field] = int(value)
            except (ValueError, TypeError):
                extract_item[field] = value

    if not has_any_value(extract_item):
        return None

    return extract_item


# проверка на пустые значения
def has_any_value(structure):
    def check_value(v):
        if v is None:
            return False
        if isinstance(v, str) and v.strip() in ['', 'nan']:
            return False
        if isinstance(v, (int, float, bool)):
            return True
        if isinstance(v, list):
            return any(check_value(item) for item in v)
        if isinstance(v, dict):
            return any(check_value(val) for val in v.values())
        return True

    return check_value(structure)


if __name__ == "__main__":
    input_file = "xlsxfiles/testfile5.xlsx"
    output_file = "output.json"

    try:
        print(f"Начинаю обработку файла: {input_file}")
        json_data = excel_to_json_custom(input_file, output_file)
        print(f"Обработка завершена. Результат сохранен в: {output_file}")

    except FileNotFoundError:
        print(f"Ошибка: Файл '{input_file}' не найден.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        import traceback

        traceback.print_exc()
