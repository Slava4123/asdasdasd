import asyncio

async def handle_input(reader, writer):
    """Обработчик команд, введённых пользователем и взаимодействие с сервером."""
    try:
        while True:
            user_input = input("Введите команду: ")

            if user_input.lower() in ("exit", "quit"):
                print("Закрытие соединения с клиентом...")
                writer.write("exit\n".encode())
                await writer.drain()
                break

            writer.write((user_input + "\n").encode())
            await writer.drain()

            data = await reader.read(4096)
            if not data:
                print("Сервер закрыл соединение.")
                break

            response = data.decode().strip()
            print(f"Ответ от сервера: {response}")

    except KeyboardInterrupt:
        print("Прерывание клиента.")
    finally:
        writer.close()
        await writer.wait_closed()

async def main(host='localhost', port=8888):
    """Основная функция для подключения к серверу и запуска обработки ввода."""
    print(f"Подключение к серверу {host}:{port}...")
    reader, writer = await asyncio.open_connection(host, port)
    print("Соединение с сервером установлено.")
    await handle_input(reader, writer)

if __name__ == "__main__":
    asyncio.run(main())
