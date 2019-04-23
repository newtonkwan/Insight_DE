import redis
from flask import Flask, render_template, request



app = Flask(__name__)
rdb = redis.Redis(host = "10.0.0.5", port = 6379, decode_responses=True)

@app.route('/', methods=['GET', 'POST'])
def display_from_redis():
	results = {}
	errors = []
	input_paper = {}
	if request.method == "POST":
		try:
			# get the title that the user has entered
			title_tag = request.form['url']
		except: 
			errors.append("Unable to get this title. Please make sure it's valid and try again")
			return render_template("index.html", errors=errors)
		if title_tag:

			try:
				id_tag = rdb.get(title_tag)
				if id_tag == None:
					added_period_to_title = title_tag + "."
					id_tag = rdb.get(added_period_to_title)
				input_paper[title_tag] = rdb.lrange(id_tag, 0,6)[-2]
				#results[title_tag] = rdb.lrange(id_tag, 0,6)[5]
				for tag_id in rdb.lrange(id_tag, 0, 4):
					if tag_id == title_tag:
						break
					title_tag = rdb.lrange(tag_id, 0, 1)[1]
					abstract = rdb.lrange(tag_id, 0, 1)[0]
					results[title_tag] = abstract 
				#results["Mastering the game of Go with deep neural networks and tree search"] = rdb.lrange("Mastering the game of Go with deep neural networks and tree search", 0, 0)[0]
			        #results["A general reinforcement learning algorithm that masters chess, shogi and Go through self-play"] = rdb.lrange("A general reinforcement learning algorithm that masters chess, shogi and Go through self-play", 0, 0)[0]
				#results["Playing Atari with Deep Reinforcement Learning"] = rdb.lrange("Playing Atari with Deep Reinforcement Learning", 0, 0)[0]
				#results["AlphaStar: Mastering the Real-Time Strategy Game StarCraft II"] = rdb.lrange("AlphaStar: Mastering the Real-Time Strategy Game StarCraft II", 0, 0)[0]
				#results["Deep Reinforcement Learning: An Overview"] = rdb.lrange("Deep Reinforcement Learning: An Overview", 0, 0)[0]
				
				#results["0bf6e21f6eaa4fec02f3c4573b0466b71c68814c"] = rdb.lrange("0bf6e21f6eaa4fec02f3c4573b0466b71c68814c", 0, 0)[0]
				#results["Mastering the game of Go without human knowledge"] = str(rdb.lrange("Mastering the game of Go without human knowledge", 0, 0)[0])
				#results["7177f1ed139b1dbb926f597ad13da2616abaccad"] = rdb.lrange("7177f1ed139b1dbb926f597ad13da2616abaccad", 0, 0)[0]
			except:
				errors.append("Unable to find paper. Please try a different one!")
	return render_template('index.html',input_paper=input_paper, results=results, errors=errors)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
