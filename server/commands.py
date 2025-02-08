from abc import ABC, abstractmethod
from .db_manager import DatabaseManager


class Command(ABC):
    """Абстрактный класс для всех команд, которые должны быть выполнены."""

    @abstractmethod
    async def execute(self, *args) -> str:
        """Метод для выполнения команды."""
        pass


class AuthenticateVMCommand(Command):
    """Команда для аутентификации виртуальной машины."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self, vm_id: str, ram: str, cpu: str) -> str:
        """Аутентифицирует виртуальную машину."""
        try:
            return await self.db_manager.authenticate_vm(vm_id, int(ram), int(cpu))
        except ValueError:
            return "Неверные значения RAM или CPU"


class AddVMCommand(Command):
    """Команда для добавления виртуальной машины."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self, vm_id: str, ram: str, cpu: str) -> str:
        """Добавляет новую виртуальную машину."""
        try:
            return await self.db_manager.add_vm(vm_id, int(ram), int(cpu))
        except ValueError:
            return "Неверные значения RAM или CPU"


class ListVMsCommand(Command):
    """Команда для получения списка всех виртуальных машин."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self) -> str:
        """Возвращает список всех виртуальных машин."""
        return await self.db_manager.list_vms()


class ListAuthenticatedVMsCommand(Command):
    """Команда для получения списка авторизованных виртуальных машин."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self) -> str:
        """Возвращает список авторизованных виртуальных машин."""
        return await self.db_manager.list_authenticated_vms()


class UpdateVMCommand(Command):
    """Команда для обновления параметров виртуальной машины."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self, vm_id: str, ram: str, cpu: str) -> str:
        """Обновляет параметры виртуальной машины."""
        try:
            return await self.db_manager.update_vm(vm_id, int(ram), int(cpu))
        except ValueError:
            return "Неверные значения RAM или CPU"


class LogoutVMCommand(Command):
    """Команда для выхода виртуальной машины."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self, vm_id: str) -> str:
        """Деавторизует виртуальную машину."""
        return await self.db_manager.logout_vm(vm_id)


class RemoveVMCommand(Command):
    """Команда для удаления виртуальной машины."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self, vm_id: str) -> str:
        """Удаляет виртуальную машину."""
        return await self.db_manager.remove_vm(vm_id)


class ListDisksCommand(Command):
    """Команда для получения списка дисков."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self) -> str:
        """Возвращает список всех дисков."""
        return await self.db_manager.list_disks()


class AddDiskCommand(Command):
    """Команда для добавления диска виртуальной машине."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self, disk_id: str, vm_id: str, size: str) -> str:
        """Добавляет диск виртуальной машине."""
        try:
            return await self.db_manager.add_disk(disk_id, vm_id, int(size))
        except ValueError:
            return "Неверный размер диска"


class RemoveDiskCommand(Command):
    """Команда для удаления диска из виртуальной машины."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self, disk_id: str) -> str:
        """Удаляет диск из виртуальной машины."""
        return await self.db_manager.remove_disk(disk_id)


class CheckAllVMsCommand(Command):
    """Команда для проверки всех виртуальных машин."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def execute(self) -> str:
        """Проверяет все виртуальные машины."""
        return await self.db_manager.check_all_vms()

