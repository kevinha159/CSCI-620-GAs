from flask import Flask, jsonify, request
import time, mysql.connector, json
import pandas as pd

app = Flask(__name__)

races = ["All Races", "White Alone", "White Alone, Not Hispanic", "Asian Alone", "Hispanic (any race)", "White Alone or in Combination",
        "Black Alone or in Combination", "Asian Alone or in Combination"]

db = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "root",
    database = "CSCI_620"
    )

mycursor = db.cursor()

@app.route('/')
def home():
    return "<p>Hello, World!</p>"

# This is for getting the older processed age group for Table 1 tables,
# specifically for the dot diagrams. Supposed to simulate distributed
# computing by processing the data of the older age groups at the same time
# as the local repo code calling this API.
@app.route('/processedAgeGroup', methods=['GET'])
def get_processed_age_group():
    race = request.args.get('race', default = "white", type=str)
    idx = races.index(race) + 1 # the code that makes the tables is not 0 based
    sql_query = f"SELECT * FROM table_1_{idx}_both LIMIT 6"
    print(race)
    print(sql_query)
    mycursor.execute(sql_query)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    df.columns = [i[0] for i in mycursor.description]
    df = df.set_index('Age groups').transpose()
    df = df.iloc[2:,:]
    dfJSON = df.to_json(orient='split')
    time.sleep(2) # artificial delay to simulate actual processing of big data
    return jsonify(json.loads(dfJSON))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)