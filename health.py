from app import app
from datetime import datetime

@app.get('/health')
def health():
    return {'status': 'ok', 'version': '1.0.0', 'time': str(datetime.now())}
