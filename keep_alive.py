from flask import Flask
from waitress import serve
from threading import Thread

app = Flask(__name__)

@app.route('/')

def index():
  return "I'm alive"
  
def run():
  serve(app, host='0.0.0.0', port=8080, threads=1)
  
def keep_alive():
  t = Thread(target=run)
  t.start()