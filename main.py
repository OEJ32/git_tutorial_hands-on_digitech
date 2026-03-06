from fastapi import FastAPI

app = FastAPI()

@app.get('/')
def root():
    return {'message': 'Hola mundo!'}

# BUG: rompe producción
raise RuntimeError('critical error')
