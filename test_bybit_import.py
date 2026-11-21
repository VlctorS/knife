import os
print('Before any load_dotenv:')
print('  BYBIT_API_KEY:', repr(os.getenv('BYBIT_API_KEY')))

from dotenv import load_dotenv
import bybit as bybit_module
print('bybit.__file__:', bybit_module.__file__)
