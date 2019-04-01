mport redis
from flask import Flask
app = Flask(__name__)

@app.route('/')

def display_from_redis():
        rdb = redis.Redis(host = "10.0.0.6", port = 6379)
        abs1 = rdb.get("cb61fc1ebdeb5835460c18044d331388d5b1067a")
        abs2 = rdb.get("50ef31b58a30dfefb624db6f72cda7bc242cde50")
        abs3 = rdb.get("4cbba8127c8747a3b2cfb9c1f48c43e5c15e323e")
        abs4 = rdb.get("3316b8b97c1e17ac93f220f4b64842905c40cd93")
        abs5 = rdb.get("0ff687d5cf1d59c8f63a6fbeac5ebd749c7f2b9d")
        return 'Welcome to Velma <br/> <br/> Abstract 1: {} <br/> <br/> Abstract 2: {} <br/> <br/> Abstract 3: {} <br/> <br/> Abstract 4: {} <br/> <br/> Abstract 5: {}'.format(abs1, abs2, abs3, abs4, abs5)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
