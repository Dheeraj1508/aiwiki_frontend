import os
import json
from google.cloud import storage
import pandas as pd
from io import StringIO
from flask import Flask, request, jsonify
from flask_cors import CORS
app = Flask(__name__)
CORS(app)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'analog-button-421413-e0072d12a2ba.json'
from google.cloud import pubsub_v1


class GCSBucketReader:
    def __init__(self, bucket_name, blob_name):
        self.bucket_name = bucket_name
        self.blob_name = blob_name
        self.storage_client = storage.Client()
        self.df = None

    def read_data(self):
        """Reads data from a GCS bucket blob into a pandas DataFrame."""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.blob_name)
        
        # Read the blob's content into a string.
        data = blob.download_as_text()
        
        # Use StringIO to convert the string data into a file-like object so it can be read into a DataFrame.
        self.df = pd.read_csv(StringIO(data), header=None)

    def get_categories_list(self):
        """Returns the DataFrame if it is not None."""
        if self.df is not None:
            return self.df
        else:
            print("Data not loaded. Please call read_data() first.")
            return None



class PubSubPublisher:
    def __init__(self, project_id, topic_name):
        """Initialize the Pub/Sub client and set up the topic path."""
        
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)

    def publish_message(self, category_name):
        """Publish a message to the specified Pub/Sub topic with a variable."""
        # Create a dictionary with the category name
        message_dict = {"category_name": category_name}
        
        # Convert dictionary to JSON string
        message_json = json.dumps(message_dict)

        # Data must be a bytestring
        data = message_json.encode("utf-8")
        
        future = self.publisher.publish(self.topic_path, data=data)
        return future.result()  # Wait for the message to be published and return the result


CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})



@app.route('/add_category',methods=['POST'])
def add_category():
    data = request.json
    category = data.get('category')
    if not category:
        return jsonify({'error':'Category Not Valid'}),400
    project_id = "analog-button-421413"
    topic_name = "add-category"
    publisher = PubSubPublisher(project_id, topic_name)

    # category_name = "Organic_chemistry"
    publisher.publish_message(category)
    print(f"Published '{category}'")
    return jsonify({'message':'Published Category Successfully'}),200

@app.route('/get_categories', methods=['GET'])
def get_categories_list():
    bucket_name = 'ir-datastore'
    source_blob_name_processed = 'processed_categories.csv'
    source_blob_name_unprocessed = 'unprocessed_categories.csv'

    # Creating two instances for processed and unprocessed data
    processed_reader = GCSBucketReader(bucket_name, source_blob_name_processed)
    unprocessed_reader = GCSBucketReader(bucket_name, source_blob_name_unprocessed)

    # Reading data
    processed_reader.read_data()
    unprocessed_reader.read_data()

    # Getting categories lists
    processed_data = processed_reader.get_categories_list()
    unprocessed_data = unprocessed_reader.get_categories_list()

    processed_data =list(processed_data.iloc[:,0].values)
    unprocessed_data = list(unprocessed_data.iloc[:,0].values)
    # Example to display data
    # print("Processed Data:", processed_data.iloc[:,0].values)
    # print("Unprocessed Data:", unprocessed_data.iloc[:,0].values)
    return jsonify({'processed':processed_data,'unprocessed':unprocessed_data}),200


if __name__ == '__main__':
    app.run(debug=True) 