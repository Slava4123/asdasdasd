from loguru import logger as log
import asyncpg
from .config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

class DatabaseManager:
    """Класс для управления соединением с базой данных и инициализацией таблиц."""

    def __init__(self):
        """Инициализация объекта DatabaseManager. На старте не создается соединение с БД."""
        self.db_pool = None

    async def initialize(self):
        """Инициализация пула соединений с базой данных и создание таблиц."""
        try:
            self.db_pool = await asyncpg.create_pool(
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                host=DB_HOST,
                port=DB_PORT
            )
            log.info("✅ Успешно подключено к базе данных")

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
                log.info("✅ Таблицы базы данных созданы")
        except Exception as e:
            log.critical(f"Ошибка подключения к базе данных: {e}")
            raise

    async def authenticate_vm(self, vm_id, ram, cpu):
        """Аутентификация виртуальной машины по ID, RAM и CPU."""
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
                    log.info(f"ВМ {vm_id} аутентифицирована")
                    return f"ВМ {vm_id} аутентифицирована"
                return "Ошибка аутентификации: неверные характеристики"

            try:
                await conn.execute(
                    "INSERT INTO vms (id, ram, cpu, is_active, is_auth) "
                    "VALUES ($1, $2, $3, TRUE, TRUE)",
                    vm_id, ram, cpu
                )
                log.info(f"ВМ {vm_id} зарегистрирована и аутентифицирована")
                return f"ВМ {vm_id} зарегистрирована и аутентифицирована"
            except asyncpg.UniqueViolationError:
                log.warning(f"ВМ {vm_id} уже существует")
                return f"ВМ {vm_id} уже существует"

    async def add_vm(self, vm_id, ram, cpu):
        """Добавление новой виртуальной машины."""
        async with self.db_pool.acquire() as conn:
            try:
                await conn.execute(
                    "INSERT INTO vms (id, ram, cpu, is_active) "
                    "VALUES ($1, $2, $3, TRUE)",
                    vm_id, ram, cpu
                )
                log.info(f"ВМ {vm_id} добавлена")
                return f"ВМ {vm_id} добавлена"
            except asyncpg.UniqueViolationError:
                log.warning(f"ВМ {vm_id} уже существует")
                return f"ВМ {vm_id} уже существует"

    async def list_vms(self):
        """Список активных виртуальных машин."""
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch("SELECT * FROM vms WHERE is_active = TRUE")
            if not records:
                log.info("Виртуальные машины не найдены")
                return "Виртуальные машины не найдены"
            return "\n".join([f"{r['id']}: {r['ram']}MB RAM, {r['cpu']}CPU" for r in records])

    async def list_authenticated_vms(self):
        """Список авторизованных виртуальных машин."""
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch(
                "SELECT * FROM vms WHERE is_auth = TRUE AND is_active = TRUE"
            )
            if not records:
                log.info("Авторизованные ВМ не найдены")
                return "Авторизованные ВМ не найдены"
            return "\n".join([f"{r['id']}: {r['ram']}MB RAM, {r['cpu']}CPU" for r in records])

    async def update_vm(self, vm_id, ram, cpu):
        """Обновление характеристик виртуальной машины."""
        async with self.db_pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE vms SET ram = $2, cpu = $3 WHERE id = $1",
                vm_id, ram, cpu
            )
            if result == "UPDATE 0":
                log.warning(f"ВМ {vm_id} не найдена для обновления")
                return "ВМ не найдена"
            log.info(f"ВМ {vm_id} обновлена")
            return f"ВМ {vm_id} обновлена"

    async def logout_vm(self, vm_id):
        """Деавторизация виртуальной машины."""
        async with self.db_pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE vms SET is_auth = FALSE WHERE id = $1",
                vm_id
            )
            if result == "UPDATE 0":
                log.warning(f"ВМ {vm_id} не найдена для деавторизации")
                return "ВМ не найдена"
            log.info(f"ВМ {vm_id} деавторизована")
            return f"ВМ {vm_id} деавторизована"

    async def remove_vm(self, vm_id):
        """Удаление виртуальной машины."""
        async with self.db_pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE vms SET is_active = FALSE, is_auth = FALSE WHERE id = $1", vm_id
            )
            if result == "UPDATE 0":
                log.warning(f"ВМ {vm_id} не найдена для удаления")
                return f"ВМ {vm_id} не найдена"
            log.info(f"ВМ {vm_id} удалена")
            return f"ВМ {vm_id} удалена"

    async def list_disks(self):
        """Список дисков."""
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch(
                "SELECT d.id, d.size, v.id as vm_id "
                "FROM disks d LEFT JOIN vms v ON d.vm_id = v.id"
            )
            if not records:
                log.info("Диски не найдены")
                return "Диски не найдены"
            return "\n".join([f"Disk {r['id']} (VM {r['vm_id']}): {r['size']}GB" for r in records])

    async def add_disk(self, disk_id, vm_id, size):
        """Добавление диска к виртуальной машине."""
        async with self.db_pool.acquire() as conn:
            vm_exists = await conn.fetchrow("SELECT id FROM vms WHERE id = $1", vm_id)
            if not vm_exists:
                log.warning(f"ВМ {vm_id} не найдена для добавления диска")
                return f"ВМ {vm_id} не найдена"

            await conn.execute(
                "INSERT INTO disks (id, vm_id, size) VALUES ($1, $2, $3)",
                disk_id, vm_id, size
            )
            log.info(f"Диск {disk_id} добавлен к ВМ {vm_id}")
            return f"Диск {disk_id} добавлен к ВМ {vm_id}"

    async def remove_disk(self, disk_id):
        """Удаление диска."""
        async with self.db_pool.acquire() as conn:
            result = await conn.execute("DELETE FROM disks WHERE id = $1", disk_id)
            if result == "DELETE 0":
                log.warning(f"Диск {disk_id} не найден")
                return f"Диск {disk_id} не найден"
            log.info(f"Диск {disk_id} удален")
            return f"Диск {disk_id} удален"

    async def check_all_vms(self):
        """Проверка всех виртуальных машин."""
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch("SELECT * FROM vms")
            if not records:
                log.info("Виртуальные машины не найдены")
                return "Виртуальные машины не найдены"
            return "\n".join([f"{r['id']}: {r['ram']}MB RAM, {r['cpu']}CPU" for r in records])
