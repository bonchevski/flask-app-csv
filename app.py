import csv
from flask import Flask, request, render_template, send_file
import pandas as pd
import unidecode
from io import StringIO

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

def normalize_address(address):
    # Simple normalization: lowercase, remove punctuation, and accents
    # Consider more advanced normalization for real applications
    return unidecode.unidecode(address.lower().replace(',', '').replace('.', '').replace('"', ''))

def preprocess_csv_content(file_stream):
    content = file_stream.read()
    # Replace curly quotes with standard quotes
    content = content.replace('“', '"').replace('”', '"')
    return content

def process_csv(file_stream):
  # Read the CSV from a file stream
    df = preprocess_csv_content(file_stream)
    # Normalize the addresses
    df['Normalized_Address'] = df['Address'].apply(normalize_address)

    # Group by normalized address and sort
    grouped = df.groupby('Normalized_Address')['Name'].apply(lambda x: sorted(x.tolist())).reset_index(name='Names')

    # Sort the groups by address
    sorted_groups = grouped.sort_values(by='Normalized_Address')

    # Prepare the output
    output_lines = [', '.join(names) for names in sorted_groups['Names']]

    return '\n'.join(output_lines)

def process_dataframe(df):
    # Normalize the addresses
    df['Normalized_Address'] = df['Address'].apply(normalize_address)

    # Group by normalized address and sort
    grouped = df.groupby('Normalized_Address')['Name'].apply(lambda x: sorted(x.tolist())).reset_index(name='Names')

    # Sort the groups by address
    sorted_groups = grouped.sort_values(by='Normalized_Address')

    # Prepare the output
    output_lines = [', '.join(names) for names in sorted_groups['Names']]

    return '\n'.join(output_lines)

@app.route('/submit', methods=['POST'])
def handle_text():
    text_data = request.form['text_data']
    
    # Convert the string data to a file-like object for csv.reader
    text_data_io = StringIO(text_data)
    reader = csv.reader(text_data_io, delimiter=',')
    
    # Convert CSV reader to a list of tuples (Name, Address)
    data = [(row[0].strip(), row[1].strip()) for row in reader if row]  # Ensure row is not empty
    
    # Convert the list of tuples to a DataFrame
    df = pd.DataFrame(data, columns=['Name', 'Address'])
    
    # Process the DataFrame similarly to the CSV processing
    result_data = process_dataframe(df)  # Use the same processing function for DataFrame
    with open('output.txt', 'w', encoding='utf-8') as f:
        f.write(result_data)
    return render_template('results.html', result=result_data)

@app.route('/upload', methods=['POST'])
def handle_upload():
    file = request.files['file']
    if not file:
        return "No file"
    
    result_data = process_csv(file.stream)
    with open('output.txt', 'w', encoding='utf-8') as f:
        f.write(result_data)
    return render_template('results.html', result=result_data)

@app.route('/download')
def download_file():
    path = 'output.txt'
    return send_file(path, as_attachment=True)


if __name__ == '__main__':
    app.run(debug=True)
