from flask import Flask, jsonify
from src.database.db_operations import fetch_data_from_db
from src.database.db_connection import get_engine

app = Flask(__name__)

@app.route('/stocks/<ticker>')
def get_stock(ticker):
    engine = get_engine()
    df = fetch_data_from_db(ticker, engine)
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)