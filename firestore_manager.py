from google.cloud import firestore

# Create a Firestore client
db = firestore.Client(project='crypto-catfish-407213')


def insert_into_firestore(date, index_price, variation, index_target, recommendation):
    # Convert the Timestamp to a string
    date_str = date.date().isoformat()

    doc_ref = db.collection('index_data').document(date_str)
    doc_ref.set({
        'Date': date.date().isoformat(),
        'IndexPrice': round(index_price, 3),
        'Variation': round(variation, 3),
        'IndexTarget': round(index_target, 3),
        'Recommendation': recommendation
    })


def read_firestore_data():
    return db.collection('index_data').stream()
