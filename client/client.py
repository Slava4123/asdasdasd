import asyncio
from loguru import logger as log

async def handle_input(reader, writer):
    """Обрабатывает ввод пользователя и отправляет команды на сервер, ожидая ответа."""
    try:
        while True:
            user_input = input("Введите команду: ")

            if user_input.lower() in ("exit", "quit"):
                log.info("Закрытие соединения с клиентом...")
                writer.write("exit\n".encode())
                await writer.drain()
                break

            writer.write((user_input + "\n").encode())
            await writer.drain()

            data = await reader.read(4096)
            if not data:
                log.info("Сервер закрыл соединение.")
                break

            response = data.decode().strip()
            log.info(f"Ответ от сервера: {response}")

    except KeyboardInterrupt:
        log.error("Прерывание клиента.")
    except Exception as e:
        log.error(f"Ошибка при обработке команды: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def main(host='localhost', port=8888):
    """Основная функция для подключения к серверу и обработки ввода пользователя."""
    log.info(f"Подключение к серверу {host}:{port}...")
    try:
        reader, writer = await asyncio.open_connection(host, port)
        log.info("Соединение с сервером установлено.")
        await handle_input(reader, writer)
    except Exception as e:
        log.error(f"Не удалось подключиться к серверу: {e}")

if __name__ == "__main__":
    asyncio.run(main())
