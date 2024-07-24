from flask import Flask, request, jsonify
import numpy as np
import pickle
import pandas as pd
from flasgger import Swagger

app = Flask(__name__)
Swagger(app)

# Load the model
with open("logreg.pkl", "rb") as pickle_in:
    model = pickle.load(pickle_in)

@app.route('/')
def home():
    return "Welcome to the Flask API!"

@app.route('/predict', methods=["GET"])
def predict_class():
    """Predict if Customer would buy the product or not.
    ---
    parameters:  
      - name: age 
        in: query 
        type: integer
        required: true 
      - name: new_user 
        in: query 
        type: integer 
        required: true 
      - name: total_pages_visited 
        in: query 
        type: integer 
        required: true 
    responses: 
      200: 
        description: Prediction 
        schema:
          type: object
          properties:
            prediction:
              type: integer
              description: The prediction result
    """
    try:
        age = int(request.args.get("age"))
        new_user = int(request.args.get("new_user"))
        total_pages_visited = int(request.args.get("total_pages_visited"))
        prediction = model.predict([[age, new_user, total_pages_visited]])
        return jsonify({'prediction': int(prediction[0])})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/predict_file', methods=["POST"])
def prediction_test_file():
    """Prediction on multiple input test file.
    ---
    parameters: 
      - name: file 
        in: formData 
        type: file 
        required: true 
    responses: 
      200: 
        description: Test file Prediction 
        schema:
          type: object
          properties:
            predictions:
              type: array
              items:
                type: integer
              description: List of predictions
    """
    try:
        file = request.files.get("file")
        if not file:
            return jsonify({'error': 'No file provided'}), 400
        
        df_test = pd.read_csv(file)
        predictions = model.predict(df_test)
        # Convert predictions to a list of native Python types
        predictions_list = [int(pred) for pred in predictions]
        return jsonify({'predictions': predictions_list})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

