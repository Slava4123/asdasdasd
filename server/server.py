import asyncio
from loguru import logger as log
from .db_manager import DatabaseManager
from .commands import (AuthenticateVMCommand, AddVMCommand, ListVMsCommand,
                       ListAuthenticatedVMsCommand, UpdateVMCommand, LogoutVMCommand,
                       RemoveVMCommand, ListDisksCommand, AddDiskCommand,
                       RemoveDiskCommand, CheckAllVMsCommand)

class VMServer:
    """Сервер для управления виртуальными машинами с хранением состояния в БД."""

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.commands = {
            "AUTH": AuthenticateVMCommand(self.db_manager),
            "ADD_VM": AddVMCommand(self.db_manager),
            "LIST_VMS": ListVMsCommand(self.db_manager),
            "LIST_AUTH_VMS": ListAuthenticatedVMsCommand(self.db_manager),
            "UPDATE_VM": UpdateVMCommand(self.db_manager),
            "LOGOUT_VM": LogoutVMCommand(self.db_manager),
            "REMOVE_VM": RemoveVMCommand(self.db_manager),
            "LIST_DISKS": ListDisksCommand(self.db_manager),
            "ADD_DISK": AddDiskCommand(self.db_manager),
            "REMOVE_DISK": RemoveDiskCommand(self.db_manager),
            "CHECK_ALL_VMS": CheckAllVMsCommand(self.db_manager),
        }

    async def start(self, host='0.0.0.0', port=8888):
        try:
            await self.db_manager.initialize()

            server = await asyncio.start_server(
                self.handle_client,
                host,
                port
            )
            log.info(f"🚀 Сервер запущен на {host}:{port}")

            async with server:
                await server.serve_forever()

        except Exception as e:
            log.critical(f"🔥 Критическая ошибка: {e}")
            raise

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        log.info(f"Новое подключение от {addr}")
        try:
            while True:
                data = await reader.read(100)
                if not data:
                    break
                message = data.decode().strip()
                log.info(f"Получено от {addr}: {message}")
                response = await self.process_command(message)
                writer.write(response.encode())
                await writer.drain()
        except Exception as e:
            log.error(f"Ошибка с {addr}: {e}")
        finally:
            log.info(f"Соединение с {addr} закрыто")
            writer.close()

    async def process_command(self, message):
        parts = message.split()
        if len(parts) < 1:
            return "Неверная команда"

        command_name = parts[0].upper()
        command = self.commands.get(command_name)

        if not command:
            return "Неизвестная команда"

        try:
            return await command.execute(*parts[1:])
        except Exception as e:
            return f"Ошибка обработки команды: {str(e)}"

if __name__ == '__main__':
    try:
        log.info("⏳ Запуск сервера...")
        server = VMServer()
        asyncio.run(server.start())
    except KeyboardInterrupt:
        log.info("\n🛑 Сервер остановлен")
