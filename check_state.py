import asyncio, sys
sys.path.insert(0, r'C:\Users\Canon\Projects\Knife')
from app.state import state
asyncio.run(state.refresh())
st = state.symbols.get('ELSAUSDT')
if st:
    print(f'ELSAUSDT: last_price={st.last_price} ts={st.last_price_ts}')
else:
    print('ELSAUSDT not in state.symbols')
print(f'Total symbols: {len(state.symbols)}')
