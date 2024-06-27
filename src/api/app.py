import pandas as pd
import json
import os
import requests

def process_csv_to_json(file_path, output_path):
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Drop any completely empty rows
        df.dropna(how='all', inplace=True)

        # Strip leading and trailing spaces from the 'Date' column
        df['Date'] = df['Date'].str.strip()

        # Remove columns with names that start with 'Unnamed'
        df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

        # Explicitly cast float64 columns to object (string) dtype
        float_columns = df.select_dtypes(include=['float64']).columns
        df[float_columns] = df[float_columns].astype(object)

        # Replace NaN values with empty strings
        df.fillna('', inplace=True)

        # Filter out rows where 'Date' is empty or does not start with 'J'
        df = df[df['Date'].str.startswith('J')]

        # Group by 'Date' column
        grouped = df.groupby('Date')

        # Dictionary to store the grouped data
        grouped_data = {}

        # Process each group
        for date, group in grouped:
            # Convert group DataFrame to dictionary and store it in the grouped_data
            grouped_data[date] = group.to_dict(orient='records')

        # Convert the dictionary to a JSON object
        grouped_json = json.dumps(grouped_data, indent=4, ensure_ascii=False)

        # Save the JSON object to a file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(grouped_json)

        print(f"JSON data has been saved to {output_path}")

        return grouped_json

    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def send_post_requests(json_path):
    # Load the JSON data from the output file
    with open(json_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Define the URL for the POST API call
    url = 'http://localhost:11434/api/chat'

    # Define the body of the POST request
    def create_post_body(libelle_content):
        return {
            "model": "mistral",
            "stream": False,
            "messages": [
                {
                    "role": "system",
                    "content": "You now have to act as a decision-making expert. Your aim is to read the user's message and interpret whether there are several choices to be made. If so, create a JSON object of the choices to be made and add it to the result. You will return just one JSON object in your response and display all the data without truncating it. Return the output content in French and provide no explanation. The expected structure is as follows, you will need to replace the choices if you find any and remove unused ones: {'date': date, 'children': [{'name': 'question', 'attributes': {'answer': 'answer'}, 'children': [{'choice_1': 'choice_data', 'choice_2': 'choice_data', 'choice_3': 'choice_data'}]}, {'name': 'question', 'attributes': {'answer': 'answer'}, 'children': [{'choice_1': 'choice_data', 'choice_2': 'choice_data', 'choice_3': 'choice_data'}]}]}"
                },
                {
                    "role": "user",
                    "content": libelle_content
                }
            ]
        }

    # List to store the responses
    response_contents = []

    # Iterate through each entry in the JSON data
    for date, entries in data.items():
        for entry in entries:
            libelle_content = entry.get('Libelle', '')
            if libelle_content:  # Ensure there is content in Libelle
                post_body = create_post_body(libelle_content)

                # Make the POST request
                response = requests.post(url, json=post_body)

                # Print the message content from the response
                try:
                    response_content = response.json().get('message', {}).get('content', 'No content found')
                    response_contents.append({
                        "Date": date,
                        "Libelle": libelle_content,
                        "Response": response_content
                    })
                except json.JSONDecodeError:
                    print("Failed to decode JSON response")

    # Print all the collected response contents at the end
    for response_content in response_contents:
        print(f"Date: {response_content['Date']}")
        print(f"Libelle: {response_content['Libelle']}")
        print(f"Response: {response_content['Response']}\n")

# Usage example:
file_path = 'data/example.csv'
output_path = 'outputs/output.json'

grouped_json = process_csv_to_json(file_path, output_path)
if grouped_json is not None:
    send_post_requests(output_path)
