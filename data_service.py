import functions_framework
from flask import jsonify

from firestore_manager import read_firestore_data


@functions_framework.http
def get_historical_data(request):
    query_results = read_firestore_data()

    data = [doc.to_dict() for doc in query_results]

    return jsonify(data), 200
