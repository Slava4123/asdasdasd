import asyncio

async def handle_input(reader, writer):
    """Обработчик команд, введённых пользователем."""
    try:
        while True:
            # Считываем команду пользователя из консоли
            user_input = input("Enter command: ")

            # Если пользователь хочет выйти из клиента
            if user_input.lower() in ("exit", "quit"):
                print("Closing client connection...")
                break

            # Отправляем команду на сервер
            writer.write((user_input + "\n").encode())
            await writer.drain()

            # Ждём ответ сервера
            data = await reader.read(4096)
            if not data:
                print("Server closed connection.")
                break

            print(f"Response: {data.decode().strip()}")
    except KeyboardInterrupt:
        print("Client interrupted.")
    finally:
        writer.close()
        await writer.wait_closed()


async def main(host='localhost', port=8888):
    print(f"Connecting to server {host}:{port}...")
    reader, writer = await asyncio.open_connection(host, port)
    print("Connected to server.")
    await handle_input(reader, writer)

if __name__ == "__main__":
    asyncio.run(main())