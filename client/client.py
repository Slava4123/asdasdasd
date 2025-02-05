import asyncio


async def client(host='127.0.0.1', port=8888):
    try:
        reader, writer = await asyncio.open_connection(host, port)
        print("Connected to server")
        while True:
            command = input("Enter command: ")
            if command.lower() in ['exit', 'quit']:
                break

            writer.write(command.encode())
            await writer.drain()

            data = await reader.read(1024)
            print(f"Response: {data.decode()}")

    except (ConnectionResetError, asyncio.CancelledError):
        print("Connection lost")
    finally:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        print("Disconnected")

async def main():
    await client()

if __name__ == "__main__":
    asyncio.run(main())