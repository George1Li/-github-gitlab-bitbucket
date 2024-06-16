import sqlite3
from datetime import datetime

from datetime import datetime
import argparse

config_dir = "conf.txt"
#global files_dir, ext, log_format
def output(str):
    print(str)

def read_config_file(filename):
    try:
        with open(filename, 'r') as file:
            for line in file:
                key, value = line.strip().split('=')
                if key.strip() == 'files_dir':
                    files_dir = value.strip()
                elif key.strip() == 'ext':
                    ext = value.strip()
                elif key.strip() == 'format':
                    log_format = value.strip()
        return files_dir, ext, log_format, 
    except FileNotFoundError:
        print(f"Файл настроек не найден.")
        return None, None, None
    except UnboundLocalError:
        output(f"В файле настроек нехватает данных")
        return None, None, None

#LOGS
def read_logs_file(filename):
    try:
        with open(filename, 'r') as file:
            logs_information = []
            for line in file:
                parts = line.split()
                logs_information.append(parts)
        return logs_information
    except FileNotFoundError:
        print("Файл логов не найден")

#DATA BASE
def check_db():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        password TEXT NOT NULL,
        tabelID TEXT NOT NULL
    )
    ''')
    return conn, cursor

#FILTERS
def format_log(log, filter_str):
    # Разбираем лог на отдельные элементы
    ip, ident, user, raw_time, request, status, size = log
    
    # Форматируем время
    time = raw_time[1:]  # убираем начальный символ '['
    request = request[1:-1]  # убираем начальные и конечные кавычки
    
    # Заменяем пустые значения на '-'
    ident = ident if ident != '-' else '-'
    user = user if user != '-' else '-'
    
    # Словарь соответствия маркеров фильтра и значений из лога
    replacements = {
        '%h': ip,
        '%l': ident,
        '%u': user,
        '%t': f"[{time}",
        '%r': request,
        '%>s': status,
        '%b': str(size)
    }
    
    # Заменяем маркеры в фильтре на значения из лога
    for key, value in replacements.items():
        filter_str = filter_str.replace(key, value)
    
    return filter_str

def parse_log_date(log_date_str):
    '''Преобразует строку даты из логов в формат date'''
    log_date_str = log_date_str[1:].split()[0]
    return datetime.strptime(log_date_str, '%d/%b/%Y:%H:%M:%S')

def filter_logs_by_date(logs, date1, date2=None):

    datetime1 = parse_log_date(date1).time
    
    if date2:
        datetime2 = parse_log_date(date2)
    else:
        datetime2 = datetime1

    filtered_logs = []
    for log in logs:
        log_date = parse_log_date(log[2])
        if datetime1 <= log_date <= datetime2:
            filtered_logs.append(log)
    
    return filtered_logs



#USER
def user_exists(name, cursor):
    '''Проверка существования пользователя с этим ником в базе данных'''
    cursor.execute("SELECT 1 FROM users WHERE name = ?", (name,))
    return cursor.fetchone() is not None

class User:
    def __init__(self, conn, cursor):
        self.cursor = cursor
        self.conn = conn

    #Вход
    def create_user(self):
        name = input("Введите имя: ")
        password = input("Введите пароль: ")
        confirm_password = input("Подтвердите пароль: ")
        while True:
            if password != confirm_password:
                output("Пароли не совпадают")
                return None
            
            tabelID = f"{name}_id"
            self.cursor.execute("INSERT INTO users (name, password, tabelID) VALUES (?, ?, ?)", (name, password, tabelID))
            self.conn.commit()
            output(f"Пользователь {name} создан успешно")
            return tabelID

    def login(self):
        '''Вход пользователя в аккаунт'''
        self.load_user_from_file()
        if(self.name == ""):    
            while True:  
                name = input("Введите ник:")
                if(user_exists(name, self.cursor)):
                    password = input("Введите праоль:")
                    self.cursor.execute("SELECT * FROM users WHERE name = ? AND password = ?", (name, password))
                    user = self.cursor.fetchone()
                    if(user):
                        self.name = name
                        self.tabelID = (user[3])
                        break
                    else:
                        output("Пароль не верный")
                else:
                    output("Пользователь не найден")
                    c_u = input("Создать нового? (да/нет)")
                    if(c_u == "да"):
                        self.tabelID = self.create_user()
        self.save_user_to_file()
        output(f"Добро пожаловать, {self.name}!")
            
    def save_user_to_file(self, filename="user_data.txt"):
        '''Сохраняет данные пользователя в текстовый файл'''
        if self.name and self.tabelID:
            with open(filename, "w") as file:
                file.write(f"{self.name},{self.tabelID}\n")

    
    def load_user_from_file(self, filename="user_data.txt"):
        '''Загружает данные пользователя из текстового файла, если файл не пуст'''
        self.name = ""
        try: 
            with open(filename, "r") as file:
                line = file.readline().strip()
                if line:
                    self.name, self.tabelID = line.split(',')
                    self.tabelID = int(self.tabelID)  
                    print(f"Данные пользователя загружены: {self.name}, ID: {self.tabelID}")
        except:
            return None

            
    def save_information(self, logs):
            '''Сохранение логов в базу данных'''
            self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.tabelID} (
                        hostname TEXT,
                        remote_logname TEXT,
                        remote_user TEXT,
                        time TEXT,
                        first_line TEXT,
                        status TEXT,
                        size_of_response_in_bytes INTEGER
                    )''')
            for log in logs:
                if len(log) == 9:
                    first_line = f"{log[4]} {log[5]} {log[6]}"
                    formatted_log = (log[0], log[1], log[2], log[3], first_line, log[7], log[8])
                    self.cursor.execute(f'''INSERT INTO {self.tabelID}
                                    (hostname, remote_logname, remote_user, time, first_line, status, size_of_response_in_bytes)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)''', formatted_log)
                else:
                    print(f"Неверное количесвто элементов в логе: {log} , {len(log)}")


            self.conn.commit()

    def read_information(self):
        '''Чтение логов из базы данных'''
        try:
            self.cursor.execute(f"SELECT * FROM {self.tabelID}")

            rows = self.cursor.fetchall()

            return rows

        except sqlite3.Error as e:
            print("Ошибка при чтении данных из базы данных:", e)
            return None

def output_d(date_str1=None, date_str2=None):
    files_dir, ext,  log_format = read_config_file(config_dir)
    global user
    logs = user.read_information()
    if date_str1 and date_str2:
        date1 = datetime.strptime(date_str1, '%Y-%m-%d').date()
        date2 = datetime.strptime(date_str2, '%Y-%m-%d').date()
        f_logs = filter_logs_by_date(logs, date1, date2)
    elif date_str1:
        date1 = datetime.strptime(date_str1, '%Y-%m-%d').date()
        f_logs = filter_logs_by_date(logs, date1)
    else:
        f_logs = logs
    for log in f_logs:
        print(format_log(log, log_format))

def parse():
    global user
    files_dir, ext, log_format = read_config_file(config_dir)
    user.save_information(read_logs_file(files_dir))

def leave():
    with open("user_data.txt", 'w') as f:
        f.write('')


def reader():
    while True:
        func = input('Название функции (leave, parse, output_d): ')

        if func == "leave":
            leave()
            main()
            break
        elif func == "parse":
            parse()
        elif func == "output_d":
            date1 = input("Введите первую дату (день/месяц/год:час:минута:секунда) или ничего")
            date2 = input("Введите вторую дату (день/месяц/год:час:минута:секунда)")
            if(date2 == "" and date1 == ""):
                output_d()
            else:
                try:
                    date1 = parse_log_date(date1)
                    date2 = parse_log_date(date2)
                    output_d(date1, date2)
                except:
                    print("Неверный формат даты")

        else:
            print("Выбранной функции не существует")
def main():
    global user
    global files_dir, ext, log_format
    files_dir, ext, log_format = read_config_file(config_dir)
    conn, cursor = check_db()
    user = User(conn, cursor)
    user.login()

    reader()
    input()
    conn.close()

main()
