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

    # THIS CODE DID NOT WORK. The measurable difference is literally 0 I need 
    # higher precision to calculate it and even if I did get the number, that over 
    # main memory which was 93 MB (this is a t2.micro) is basically like 100K's of rows

    # max_in_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # max_in_mb = max_in_bytes / 1024
    # print(max_in_mb)

    # sql_query = f"SELECT * FROM table_1_{idx}_both LIMIT 1"
    # mycursor.execute(sql_query)
    # result = mycursor.fetchall()
    # one_row = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    # print(one_row)
    sql_query = f"SELECT * FROM table_1_{idx}_both LIMIT 6"
    print(race)
    print(sql_query)
    mycursor.execute(sql_query)
    result = mycursor.fetchall()
    time.sleep(2) # artificial delay to simulate actual processing of big data
    df = pd.DataFrame(result)
    df.columns = [i[0] for i in mycursor.description]
    df = df.set_index('Age groups').transpose()
    df = df.iloc[2:,:]
    dfJSON = df.to_json(orient='split')
    return jsonify(json.loads(dfJSON))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)