from flask import Flask, render_template
import pandas as pd
import json
import plotly
import plotly.express as px
from pymongo import MongoClient

app = Flask(__name__)

@app.route('/')
def index():

	client = MongoClient('mongodb+srv://ninad:i5oVP4hzNs4J9eEC@cluster0.h4zod.mongodb.net/natours?retryWrites=true&w=majority')
	db=client.get_database('tweetanalysis')
	tbl = db.hashtagcount

	output = tbl.find().sort("count",-1).limit(10)
	df = pd.DataFrame(output)
	df = df[['count','hashtag']]
	fig = px.bar(df, y="hashtag", x="count")
	graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
	return render_template('index.html', graphJSON=graphJSON)

if __name__ == "__main__":
        app.run()