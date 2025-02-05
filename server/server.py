import asyncio
import asyncpg

from server.vm import VirtualMachine


class VMServer:
    """Сервер для управления виртуальными машинами."""

    def __init__(self):
        self.vms = {}
        self.authenticated_vms = {}
        self.db_pool = None

    async def start(self, host='0.0.0.0', port=8888):
        """Запуск сервера и инициализация БД."""
        try:
            # Подключение к базе данных
            self.db_pool = await asyncpg.create_pool(
                user='postgres',
                password='Vb24122003vb',
                database='vm_manager',
                host='localhost'
            )
            print("✅ Connected to database")

            # Создание таблиц
            async with self.db_pool.acquire() as conn:
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS vms (
                        id TEXT PRIMARY KEY,
                        ram INT,
                        cpu INT
                    )
                ''')
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS disks (
                        id TEXT PRIMARY KEY,
                        vm_id TEXT,
                        size INT,
                        FOREIGN KEY (vm_id) REFERENCES vms (id)
                    )
                ''')
                print("✅ Database tables initialized")

            # Запуск сервера
            server = await asyncio.start_server(
                self.handle_client,
                host,
                port
            )
            print(f"🚀 Server started on {host}:{port}")

            async with server:
                await server.serve_forever()

        except Exception as e:
            print(f"🔥 Critical error: {e}")
            raise

    async def handle_client(self, reader, writer):
        """Обработка клиентских соединений.

        Args:
            reader (StreamReader): Читатель данных из сокета.
            writer (StreamWriter): Записыватель данных в сокет.
        """
        addr = writer.get_extra_info('peername')
        print(f"New connection from {addr}")
        try:
            while True:
                data = await reader.read(100)
                if not data:
                    break
                message = data.decode().strip()
                print(f"Received from {addr}: {message}")
                response = await self.process_command(message)
                writer.write(response.encode())
                await writer.drain()
        except Exception as e:
            print(f"Error with {addr}: {e}")
        finally:
            print(f"Connection closed with {addr}")
            writer.close()

    async def process_command(self, message):
        """Обработка команд, полученных от клиента.

        Args:
            message (str): Команда, полученная от клиента.

        Returns:
            str: Ответ на команду.
        """
        parts = message.split()
        if not parts:
            return "Invalid command"
        command = parts[0]
        if command == "AUTH":
            return await self.authenticate_vm(parts[1:])
        elif command == "ADD_VM":
            return await self.add_vm(parts[1:])
        elif command == "LIST_VMS":
            return await self.list_vms()
        elif command == "LIST_AUTH_VMS":
            return await self.list_authenticated_vms()
        elif command == "UPDATE_VM":
            return await self.update_vm(parts[1:])
        elif command == "LOGOUT":
            return await self.logout_vm(parts[1:])
        elif command == "LIST_DISKS":
            return await self.list_disks()
        elif command == "REMOVE_VM":
            return await self.remove_vm(parts[1:])
        elif command == "ADD_DISK":
            return await self.add_disk(parts[1:])
        else:
            return "Unknown command"

    async def authenticate_vm(self, args):
        if len(args) != 3:
            return "Invalid AUTH command"

        vm_id, ram, cpu = args
        try:
            async with self.db_pool.acquire() as conn:
                # Проверяем существование ВМ в БД
                vm = await conn.fetchrow("SELECT * FROM vms WHERE id = $1", vm_id)
                if vm:
                    if vm["ram"] == int(ram) and vm["cpu"] == int(cpu):
                        self.authenticated_vms[vm_id] = VirtualMachine(vm_id, int(ram), int(cpu))
                        return f"VM {vm_id} authenticated"
                    return "Authentication failed"

                # Если не существует - создаем новую
                await conn.execute(
                    "INSERT INTO vms (id, ram, cpu) VALUES ($1, $2, $3)",
                    vm_id, int(ram), int(cpu)
                )
                self.authenticated_vms[vm_id] = VirtualMachine(vm_id, int(ram), int(cpu))
                return f"VM {vm_id} registered and authenticated"

        except asyncpg.UniqueViolationError:
            return f"VM {vm_id} already exists"
        except Exception as e:
            return f"Error: {str(e)}"

    async def add_vm(self, args):
        """Добавление новой виртуальной машины.

        Args:
            args (list): Список аргументов, содержащий идентификатор ВМ, RAM и CPU.

        Returns:
            str: Результат добавления ВМ.
        """
        if len(args) != 3:
            return "Invalid ADD_VM command"
        vm_id, ram, cpu = args
        vm = VirtualMachine(vm_id, int(ram), int(cpu))
        self.vms[vm_id] = vm
        return f"VM {vm_id} added"

    async def list_vms(self):
        """Список всех виртуальных машин.

        Returns:
            str: Список виртуальных машин.
        """
        # Объединяем все виртуальные машины, чтобы избежать дублирования
        all_vms = {**self.vms, **self.authenticated_vms}
        return "\n".join([str(vm) for vm in all_vms.values()])

    async def list_authenticated_vms(self):
        """Список авторизованных виртуальных машин.

        Returns:
            str: Список авторизованных виртуальных машин.
        """
        return "\n".join([str(vm) for vm in self.authenticated_vms.values()])

    async def update_vm(self, args):
        if len(args) != 3:
            return "Invalid UPDATE_VM command"
        vm_id, ram, cpu = args

        # Сначала смотрим в self.vms
        if vm_id in self.vms:
            self.vms[vm_id].update(int(ram), int(cpu))
            return f"VM {vm_id} updated"

        # Если нет — смотрим в self.authenticated_vms
        if vm_id in self.authenticated_vms:
            self.authenticated_vms[vm_id].update(int(ram), int(cpu))
            return f"VM {vm_id} updated"

        return f"VM {vm_id} not found"

    async def logout_vm(self, args):
        """Выход из авторизованной виртуальной машины.

        Args:
            args (list): Список аргументов, содержащий идентификатор ВМ.

        Returns:
            str: Результат выхода из ВМ.
        """
        if len(args) != 1:
            return "Invalid LOGOUT command"
        vm_id = args[0]
        if vm_id in self.authenticated_vms:
            del self.authenticated_vms[vm_id]
            return f"VM {vm_id} logged out"
        return f"VM {vm_id} not found"

    async def list_disks(self):
        """Список всех жестких дисков с параметрами ВМ."""
        try:
            async with self.db_pool.acquire() as conn:
                # Получаем диски с информацией о ВМ
                disks = await conn.fetch('''
                    SELECT d.id, d.size, v.id as vm_id, v.ram, v.cpu 
                    FROM disks d
                    LEFT JOIN vms v ON d.vm_id = v.id
                ''')

                if not disks:
                    return "No disks found"

                return "\n".join([
                    f"Disk ID: {d['id']}, Size: {d['size']}GB, "
                    f"VM: {d['vm_id'] or 'None'}, "
                    f"VM Specs: {d['ram']}MB RAM, {d['cpu']} CPU"
                    for d in disks
                ])
        except Exception as e:
            return f"Error fetching disks: {str(e)}"

    async def remove_vm(self, args):
        """Удаление виртуальной машины.

        Args:
            args (list): Список аргументов, содержащий идентификатор ВМ.

        Returns:
            str: Результат удаления ВМ.
        """
        if len(args) != 1:
            return "Invalid REMOVE_VM command"
        vm_id = args[0]
        if vm_id in self.vms:
            del self.vms[vm_id]
            if vm_id in self.authenticated_vms:
                del self.authenticated_vms[vm_id]
            return f"VM {vm_id} removed"
        return f"VM {vm_id} not found"

    async def add_disk(self, args):
        """Добавление жесткого диска."""
        if len(args) != 3:
            return "Invalid ADD_DISK command. Usage: ADD_DISK <disk_id> <vm_id> <size>"

        disk_id, vm_id, size = args
        try:
            async with self.db_pool.acquire() as conn:
                # Проверяем существование ВМ
                vm_exists = await conn.fetchval("SELECT 1 FROM vms WHERE id = $1", vm_id)
                if not vm_exists:
                    return f"VM {vm_id} does not exist"

                # Добавляем диск
                await conn.execute(
                    "INSERT INTO disks (id, vm_id, size) VALUES ($1, $2, $3)",
                    disk_id, vm_id, int(size)
                )
                return f"Disk {disk_id} added to VM {vm_id}"
        except asyncpg.UniqueViolationError:
            return f"Disk {disk_id} already exists"
        except Exception as e:
            return f"Error: {str(e)}"

    async def remove_disk(self, args):
        """Удаление жесткого диска.

        Args:
            args (list): Список аргументов, содержащий идентификатор диска.

        Returns:
            str: Результат удаления диска.
        """
        if len(args) != 1:
            return "Invalid REMOVE_DISK command"
        disk_id = args[0]
        async with self.db_pool.acquire() as conn:
            await conn.execute("DELETE FROM disks WHERE id = $1", disk_id)
        return f"Disk {disk_id} removed"


async def main():
    server = VMServer()
    await server.start()

if __name__ == "__main__":
    try:
        print("⏳ Starting server...")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Server stopped")

