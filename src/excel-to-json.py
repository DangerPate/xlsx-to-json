import pandas as pd
import json
from datetime import datetime
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import threading
import traceback

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD

    HAS_TKINTERDND2 = True
except ImportError:
    HAS_TKINTERDND2 = False
    DND_FILES = None
    TkinterDnD = None


# Парсит Excel файл в JSON с обработкой объединенных ячеек
def excel_to_json_custom(file_path, output_path=None, start_row=None, end_row=None):
    try:
        print(f"Чтение файла {file_path}...")

        df = pd.read_excel(file_path, header=0, dtype=str)
        total_rows = len(df)
        print(f"Файл прочитан. Всего строк: {total_rows}, Столбцов: {len(df.columns)}")

        original_columns = list(df.columns)
        df.columns = [str(col).strip() for col in df.columns]

        previous_values = {}
        result = []
        row_count = 0
        processed_count = 0

        # Определяем диапазон строк для обработки
        if start_row is None or start_row < 1:
            start_row = 1
        if end_row is None or end_row > total_rows:
            end_row = total_rows

        print(f"Обработка строк с {start_row} по {end_row}")

        for idx, row in df.iterrows():
            row_count += 1

            # Пропускаем строки вне диапазона
            if row_count < start_row or row_count > end_row:
                continue

            processed_count += 1

            row_dict = {}

            for i, col_name in enumerate(df.columns):
                value = row[col_name]

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

        return result, total_rows, len(result), processed_count

    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        traceback.print_exc()
        return None, 0, 0, 0


# Возвращает пустой шаблон структуры JSON
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


# Заполнение структуры json
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


# Создание regulation_quantity_hunting_resources
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


# Создание issued_permits_quantity
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


# Создание extraction_hunting_resources_results
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


# Проверка на пустые значения
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


# GUI Application
class ExcelToJSONConverter:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel → JSON Converter")
        self.root.geometry("700x600")
        self.root.resizable(False, False)

        # Переменные
        self.excel_path = ""
        self.output_path = ""
        self.convert_mode = tk.StringVar(value="all")
        self.total_rows = 0
        self.is_processing = False

        self.setup_ui()

    def setup_ui(self):
        # Main container
        main_frame = tk.Frame(self.root, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Drag & Drop Area
        self.setup_drag_drop(main_frame)

        # File selection frame
        self.setup_file_selection(main_frame)

        # Conversion options
        self.setup_conversion_options(main_frame)

        # Save directory selection
        self.setup_save_options(main_frame)

        # Progress bar
        self.setup_progress_bar(main_frame)

        # Convert button
        self.convert_btn = tk.Button(
            main_frame,
            text="Начать конвертацию",
            width=25,
            height=2,
            font=("Arial", 11, "bold"),
            bg="#4CAF50",
            fg="white",
            command=self.start_conversion_thread
        )
        self.convert_btn.pack(pady=20)

        # Status label
        self.status_label = tk.Label(
            main_frame,
            text="Готов к работе",
            fg="green",
            font=("Arial", 10)
        )
        self.status_label.pack()

    def setup_drag_drop(self, parent):
        drop_frame = tk.Frame(parent, bg="#f0f0f0", bd=2, relief=tk.SUNKEN, height=100)
        drop_frame.pack(fill=tk.X, pady=(0, 15))
        drop_frame.pack_propagate(False)

        drop_label = tk.Label(
            drop_frame,
            text="Перетащите Excel файл сюда",
            bg="#f0f0f0",
            font=("Arial", 11),
            pady=20
        )
        drop_label.pack(expand=True)

        # Configure drag and drop только если tkinterdnd2 доступен
        if HAS_TKINTERDND2 and DND_FILES is not None:
            drop_frame.drop_target_register(DND_FILES)
            drop_frame.dnd_bind('<<Drop>>', self.on_drop)
            drop_label.drop_target_register(DND_FILES)
            drop_label.dnd_bind('<<Drop>>', self.on_drop)
        else:
            # Если нет tkinterdnd2, делаем область кликабельной для выбора файла
            drop_frame.bind("<Button-1>", lambda e: self.choose_excel())
            drop_label.bind("<Button-1>", lambda e: self.choose_excel())
            drop_label.config(text="Кликните для выбора Excel файла\n(поддерживаются файлы .xlsx, .xls)")

    def setup_file_selection(self, parent):
        file_frame = tk.LabelFrame(parent, text="Выбор файла", padx=10, pady=10)
        file_frame.pack(fill=tk.X, pady=(0, 15))

        self.file_label = tk.Label(
            file_frame,
            text="Файл не выбран",
            fg="gray",
            wraplength=500,
            justify="left",
            anchor="w"
        )
        self.file_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        choose_btn = tk.Button(
            file_frame,
            text="Выбрать файл",
            width=15,
            command=self.choose_excel
        )
        choose_btn.pack(side=tk.RIGHT)

    def setup_conversion_options(self, parent):
        options_frame = tk.LabelFrame(parent, text="Настройки конвертации", padx=10, pady=10)
        options_frame.pack(fill=tk.X, pady=(0, 15))

        # Radio buttons
        all_radio = tk.Radiobutton(
            options_frame,
            text="Конвертировать весь файл",
            variable=self.convert_mode,
            value="all",
            command=self.toggle_row_entries
        )
        all_radio.grid(row=0, column=0, sticky="w", pady=5)

        partial_radio = tk.Radiobutton(
            options_frame,
            text="Конвертировать строки:",
            variable=self.convert_mode,
            value="partial",
            command=self.toggle_row_entries
        )
        partial_radio.grid(row=1, column=0, sticky="w", pady=5)

        # Row entries frame
        row_frame = tk.Frame(options_frame)
        row_frame.grid(row=1, column=1, columnspan=3, sticky="w", padx=(10, 0), pady=5)

        tk.Label(row_frame, text="с").pack(side=tk.LEFT)

        self.start_row_var = tk.StringVar(value="1")
        self.start_row_entry = tk.Entry(
            row_frame,
            textvariable=self.start_row_var,
            width=8,
            state="disabled"
        )
        self.start_row_entry.pack(side=tk.LEFT, padx=5)

        tk.Label(row_frame, text="по").pack(side=tk.LEFT, padx=5)

        self.end_row_var = tk.StringVar(value="1")
        self.end_row_entry = tk.Entry(
            row_frame,
            textvariable=self.end_row_var,
            width=8,
            state="disabled"
        )
        self.end_row_entry.pack(side=tk.LEFT, padx=5)

        self.row_count_label = tk.Label(row_frame, text=f"(всего строк: {self.total_rows})")
        self.row_count_label.pack(side=tk.LEFT, padx=5)

    def setup_save_options(self, parent):
        save_frame = tk.LabelFrame(parent, text="Сохранение результата", padx=10, pady=10)
        save_frame.pack(fill=tk.X, pady=(0, 15))

        self.save_label = tk.Label(
            save_frame,
            text="Путь сохранения не выбран",
            fg="gray",
            wraplength=500,
            justify="left",
            anchor="w"
        )
        self.save_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        save_btn = tk.Button(
            save_frame,
            text="Выбрать папку",
            width=15,
            command=self.choose_save_directory
        )
        save_btn.pack(side=tk.RIGHT)

    def setup_progress_bar(self, parent):
        progress_frame = tk.Frame(parent)
        progress_frame.pack(fill=tk.X, pady=(0, 15))

        self.progress_bar = ttk.Progressbar(
            progress_frame,
            mode='indeterminate',
            length=400
        )
        self.progress_bar.pack(pady=10)

    def toggle_row_entries(self):
        if self.convert_mode.get() == "partial":
            self.start_row_entry.config(state="normal")
            self.end_row_entry.config(state="normal")
        else:
            self.start_row_entry.config(state="disabled")
            self.end_row_entry.config(state="disabled")

    def on_drop(self, event):
        # Получаем путь к файлу из события перетаскивания
        file_path = event.data.strip('{}')
        if file_path:
            self.load_excel_file(file_path)

    def choose_excel(self, event=None):
        file_path = filedialog.askopenfilename(
            title="Выберите Excel файл",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )
        if file_path:
            self.load_excel_file(file_path)

    def load_excel_file(self, file_path):
        try:
            # Быстро читаем только количество строк
            df = pd.read_excel(file_path, nrows=0)
            self.total_rows = pd.read_excel(file_path).shape[0]

            self.excel_path = file_path
            self.file_label.config(
                text=f"{os.path.basename(file_path)} ({self.total_rows} строк)",
                fg="black"
            )

            # Обновляем информацию о количестве строк
            self.end_row_var.set(str(self.total_rows))
            self.row_count_label.config(text=f"(всего строк: {self.total_rows})")

            # Автоматически предлагаем путь сохранения
            if not self.output_path:
                dir_path = os.path.dirname(file_path)
                default_name = os.path.splitext(os.path.basename(file_path))[0] + ".json"
                self.output_path = os.path.join(dir_path, default_name)
                self.save_label.config(
                    text=self.output_path,
                    fg="black"
                )

            self.status_label.config(text="Файл загружен", fg="green")

        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить файл:\n{str(e)}")

    def choose_save_directory(self):
        file_path = filedialog.asksaveasfilename(
            title="Выберите место сохранения",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if file_path:
            self.output_path = file_path
            self.save_label.config(
                text=file_path,
                fg="black"
            )

    def start_conversion_thread(self):
        if self.is_processing:
            return

        if not self.excel_path:
            messagebox.showwarning("Ошибка", "Сначала выберите Excel файл")
            return

        if not self.output_path:
            messagebox.showwarning("Ошибка", "Выберите путь для сохранения")
            return

        # Определяем диапазон строк
        start_row = None
        end_row = None

        if self.convert_mode.get() == "partial":
            try:
                start_row = int(self.start_row_var.get())
                end_row = int(self.end_row_var.get())

                if start_row < 1:
                    messagebox.showwarning("Ошибка", "Начальная строка должна быть больше 0")
                    return
                if end_row > self.total_rows:
                    messagebox.showwarning("Ошибка", f"Конечная строка не может превышать {self.total_rows}")
                    return
                if start_row > end_row:
                    messagebox.showwarning("Ошибка", "Начальная строка не может быть больше конечной")
                    return

            except ValueError:
                messagebox.showwarning("Ошибка", "Введите корректные номера строк")
                return

        # Запускаем в отдельном потоке
        self.is_processing = True
        self.progress_bar.start()
        self.status_label.config(text="Идет конвертация...", fg="orange")
        self.convert_btn.config(state="disabled")

        thread = threading.Thread(
            target=self.perform_conversion,
            args=(start_row, end_row)
        )
        thread.daemon = True
        thread.start()

    def perform_conversion(self, start_row, end_row):
        try:
            result, total_rows, saved_records, processed_count = excel_to_json_custom(
                file_path=self.excel_path,
                output_path=self.output_path,
                start_row=start_row,
                end_row=end_row
            )

            if result is not None:
                self.root.after(0, self.conversion_complete, total_rows, saved_records, processed_count)
            else:
                self.root.after(0, self.conversion_failed)

        except Exception as e:
            self.root.after(0, self.conversion_error, str(e))

        finally:
            self.root.after(0, self.conversion_finished)

    def conversion_complete(self, total_rows, saved_records, processed_count):
        self.progress_bar.stop()
        self.status_label.config(text="Конвертация завершена успешно!", fg="green")
        self.convert_btn.config(state="normal")

        if self.convert_mode.get() == "all":
            mode_text = "весь файл"
        else:
            mode_text = f"строки с {self.start_row_var.get()} по {self.end_row_var.get()}"

        message = f"""
Конвертация завершена успешно!

Файл: {os.path.basename(self.excel_path)}
Режим: {mode_text}
Всего строк в файле: {total_rows}
Обработано строк: {processed_count}
Сохранено записей: {saved_records}
Сохранено в: {self.output_path}
        """

        messagebox.showinfo("Готово", message.strip())
        self.is_processing = False

    def conversion_failed(self):
        self.progress_bar.stop()
        self.status_label.config(text="Конвертация не удалась", fg="red")
        self.convert_btn.config(state="normal")
        messagebox.showerror("Ошибка", "Конвертация не удалась. Проверьте файл и попробуйте снова.")
        self.is_processing = False

    def conversion_error(self, error_msg):
        self.progress_bar.stop()
        self.status_label.config(text="Ошибка при конвертации", fg="red")
        self.convert_btn.config(state="normal")
        messagebox.showerror("Ошибка", f"Произошла ошибка:\n{error_msg}")
        self.is_processing = False

    def conversion_finished(self):
        self.is_processing = False


# Запуск приложения
if __name__ == "__main__":
    # Создаем корневое окно в зависимости от наличия tkinterdnd2
    if HAS_TKINTERDND2 and TkinterDnD is not None:
        root = TkinterDnD.Tk()
    else:
        root = tk.Tk()

    app = ExcelToJSONConverter(root)
    root.mainloop()
