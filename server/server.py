import os
import asyncio
import asyncpg
from dotenv import load_dotenv

load_dotenv()


class VMServer:
    """Сервер для управления виртуальными машинами с хранением состояния в БД."""

    def __init__(self):
        # Инициализация атрибута db_pool
        self.db_pool = None

    async def start(self, host='0.0.0.0', port=8888):
        """
        Запуск сервера и инициализация базы данных.

        :param host: Адрес хоста для сервера
        :param port: Порт для подключения к серверу
        """
        try:
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT")
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_name = os.getenv("DB_NAME")

            self.db_pool = await asyncpg.create_pool(
                user=db_user,
                password=db_password,
                database=db_name,
                host=db_host,
                port=db_port
            )
            print("✅ Успешно подключено к базе данных")

            async with self.db_pool.acquire() as conn:
                await conn.execute('''CREATE TABLE IF NOT EXISTS vms (
                        id TEXT PRIMARY KEY,
                        ram INT NOT NULL,
                        cpu INT NOT NULL,
                        is_active BOOLEAN DEFAULT FALSE,
                        is_auth BOOLEAN DEFAULT FALSE
                    )
                ''')
                await conn.execute('''CREATE TABLE IF NOT EXISTS disks (
                        id TEXT PRIMARY KEY,
                        vm_id TEXT REFERENCES vms(id) ON DELETE CASCADE,
                        size INT NOT NULL
                    )
                ''')
                print("✅ Таблицы базы данных созданы")

            # Запуск сервера
            server = await asyncio.start_server(
                self.handle_client,
                host,
                port
            )
            print(f"🚀 Сервер запущен на {host}:{port}")

            async with server:
                await server.serve_forever()

        except Exception as e:
            print(f"🔥 Критическая ошибка: {e}")
            raise

    async def handle_client(self, reader, writer):
        """
        Обработка соединений клиентов.

        :param reader: Канал чтения для клиента
        :param writer: Канал записи для клиента
        """
        addr = writer.get_extra_info('peername')
        print(f"Новое подключение от {addr}")
        try:
            while True:
                data = await reader.read(100)
                if not data:
                    break
                message = data.decode().strip()
                print(f"Получено от {addr}: {message}")
                response = await self.process_command(message)
                writer.write(response.encode())
                await writer.drain()
        except Exception as e:
            print(f"Ошибка с {addr}: {e}")
        finally:
            print(f"Соединение с {addr} закрыто")
            writer.close()

    async def process_command(self, message):
        """
        Обработка команды клиента.

        :param message: Команда, полученная от клиента
        :return: Ответ на команду
        """
        parts = message.split()
        if not parts:
            return "Неверная команда"

        command = parts[0].upper()
        handlers = {
            "AUTH": self.authenticate_vm,
            "ADD_VM": self.add_vm,
            "LIST_VMS": self.list_vms,
            "LIST_AUTH_VMS": self.list_authenticated_vms,
            "UPDATE_VM": self.update_vm,
            "LOGOUT": self.logout_vm,
            "LIST_DISKS": self.list_disks,
            "REMOVE_VM": self.remove_vm,
            "ADD_DISK": self.add_disk,
            "REMOVE_DISK": self.remove_disk,
            "CHECK_ALL_VMS": self.check_all_vms,
        }

        handler = handlers.get(command)
        if not handler:
            return "Неизвестная команда"

        try:
            return await handler(parts[1:])
        except Exception as e:
            return f"Ошибка обработки команды: {str(e)}"

    async def authenticate_vm(self, args):
        """
        Аутентификация виртуальной машины.

        :param args: Аргументы команды
        :return: Результат аутентификации
        """
        if len(args) != 3:
            return "Неверная команда AUTH. Использование: AUTH <vm_id> <ram> <cpu>"

        vm_id, ram, cpu = args
        try:
            ram = int(ram)
            cpu = int(cpu)
        except ValueError:
            return "Неверные значения RAM или CPU"

        async with self.db_pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT ram, cpu FROM vms WHERE id = $1",
                vm_id
            )

            if existing:
                if existing['ram'] == ram and existing['cpu'] == cpu:
                    await conn.execute(
                        "UPDATE vms SET is_active = TRUE, is_auth = TRUE WHERE id = $1",
                        vm_id
                    )
                    return f"ВМ {vm_id} аутентифицирована"
                return "Ошибка аутентификации: неверные характеристики"

            # Создание новой записи
            try:
                await conn.execute(
                    "INSERT INTO vms (id, ram, cpu, is_active, is_auth) "
                    "VALUES ($1, $2, $3, TRUE, TRUE)",
                    vm_id, ram, cpu
                )
                return f"ВМ {vm_id} зарегистрирована и аутентифицирована"
            except asyncpg.UniqueViolationError:
                return f"ВМ {vm_id} уже существует"

    async def add_vm(self, args):
        """
        Добавление новой виртуальной машины.

        :param args: Аргументы команды
        :return: Результат добавления ВМ
        """
        if len(args) != 3:
            return "Неверная команда ADD_VM. Использование: ADD_VM <vm_id> <ram> <cpu>"

        vm_id, ram, cpu = args
        try:
            ram = int(ram)
            cpu = int(cpu)
        except ValueError:
            return "Неверные значения RAM или CPU"

        async with self.db_pool.acquire() as conn:
            try:
                await conn.execute(
                    "INSERT INTO vms (id, ram, cpu, is_active) "
                    "VALUES ($1, $2, $3, TRUE)",
                    vm_id, ram, cpu
                )
                return f"ВМ {vm_id} добавлена"
            except asyncpg.UniqueViolationError:
                return f"ВМ {vm_id} уже существует"

    async def list_vms(self, _=None):
        """
        Получение списка всех виртуальных машин.

        :return: Список ВМ
        """
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch("SELECT * FROM vms WHERE is_active = TRUE")
            if not records:
                return "Виртуальные машины не найдены"

            return "\n".join(
                [f"{r['id']}: {r['ram']}MB RAM, {r['cpu']}CPU" for r in records]
            )

    async def list_authenticated_vms(self, _=None):
        """
        Получение списка авторизованных виртуальных машин.

        :return: Список авторизованных ВМ
        """
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch(
                "SELECT * FROM vms WHERE is_auth = TRUE AND is_active = TRUE"
            )
            if not records:
                return "Авторизованные ВМ не найдены"

            return "\n".join(
                [f"{r['id']}: {r['ram']}MB RAM, {r['cpu']}CPU" for r in records]
            )

    async def update_vm(self, args):
        """
        Обновление параметров виртуальной машины.

        :param args: Аргументы команды
        :return: Результат обновления ВМ
        """
        if len(args) != 3:
            return "Неверная команда UPDATE_VM. Использование: UPDATE_VM <id> <ram> <cpu>"

        vm_id, ram, cpu = args
        try:
            ram = int(ram)
            cpu = int(cpu)
        except ValueError:
            return "Неверные значения RAM или CPU"

        async with self.db_pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE vms SET ram = $2, cpu = $3 WHERE id = $1",
                vm_id, ram, cpu
            )
            if result == "UPDATE 0":
                return "ВМ не найдена"
            return f"ВМ {vm_id} обновлена"

    async def logout_vm(self, args):
        """
        Выход из системы (деавторизация виртуальной машины).

        :param args: Аргументы команды
        :return: Результат выхода
        """
        if len(args) != 1:
            return "Неверная команда LOGOUT. Использование: LOGOUT <vm_id>"

        vm_id = args[0]
        async with self.db_pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE vms SET is_auth = FALSE WHERE id = $1",
                vm_id
            )
            if result == "UPDATE 0":
                return "ВМ не найдена"
            return f"ВМ {vm_id} деавторизована"

    async def list_disks(self, _=None):
        """
        Получение списка всех дисков.

        :return: Список всех дисков
        """
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch(
                "SELECT d.id, d.size, v.id as vm_id "
                "FROM disks d LEFT JOIN vms v ON d.vm_id = v.id"
            )
            if not records:
                return "Диски не найдены"

            return "\n".join(
                [f"Disk {r['id']} (VM {r['vm_id']}): {r['size']}GB" for r in records]
            )

    async def add_disk(self, args):
        """
        Добавление диска к виртуальной машине.

        :param args: Аргументы команды
        :return: Результат добавления диска
        """
        if len(args) != 3:
            return "Неверная команда ADD_DISK. Использование: ADD_DISK <disk_id> <vm_id> <size>"

        disk_id, vm_id, size = args
        try:
            size = int(size)
        except ValueError:
            return "Неверный размер диска"

        async with self.db_pool.acquire() as conn:
            vm_exists = await conn.fetchrow("SELECT id FROM vms WHERE id = $1", vm_id)
            if not vm_exists:
                return f"ВМ {vm_id} не найдена"

            await conn.execute(
                "INSERT INTO disks (id, vm_id, size) VALUES ($1, $2, $3)",
                disk_id, vm_id, size
            )
            return f"Диск {disk_id} добавлен к ВМ {vm_id}"

    async def remove_vm(self, args):
        """
        Удаление виртуальной машины.

        :param args: Аргументы команды
        :return: Результат удаления ВМ
        """
        if len(args) != 1:
            return "Неверная команда REMOVE. Использование: REMOVE <vm_id>"

        vm_id = args[0]

        async with self.db_pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE vms SET is_active = FALSE, is_auth = FALSE WHERE id = $1", vm_id
            )

            if result == "UPDATE 0":
                return f"ВМ {vm_id} не найдена"

            return f"ВМ {vm_id} удалена"

    async def remove_disk(self, args):
        """
        Удаление диска из виртуальной машины.

        :param args: Аргументы команды
        :return: Результат удаления диска
        """
        if len(args) != 1:
            return "Неверная команда REMOVE_DISK. Использование: REMOVE_DISK <disk_id>"

        disk_id = args[0]

        async with self.db_pool.acquire() as conn:
            result = await conn.execute("DELETE FROM disks WHERE id = $1", disk_id)

            if result == "DELETE 0":
                return f"Диск {disk_id} не найден"

            return f"Диск {disk_id} удален"

    async def check_all_vms(self, _=None):
        """
        Проверка всех виртуальных машин.

        :return: Список всех ВМ
        """
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch("SELECT * FROM vms")
            if not records:
                return "Виртуальные машины не найдены"

            return "\n".join(
                [f"{r['id']}: {r['ram']}MB RAM, {r['cpu']}CPU" for r in records]
            )


if __name__ == '__main__':
    try:
        print("⏳ Запуск сервера...")
        server = VMServer()
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")
