import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
print(f'ENV path: {env_path}')
print(f'File exists: {os.path.exists(env_path)}')
result = load_dotenv(env_path)
print(f'load_dotenv result: {result}')

print(f'BYBIT_API_KEY after load: {repr(os.getenv("BYBIT_API_KEY"))}')
print(f'BYBIT_API_SECRET after load: {repr(os.getenv("BYBIT_API_SECRET"))}')
