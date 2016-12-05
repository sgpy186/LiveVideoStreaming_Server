from flask import Flask, send_from_directory, request, jsonify
from threading import Timer
from subprocess import call

import mysql

app = Flask(__name__)


@app.route('/stream', methods=['GET', 'POST'])
def stream():
    if request.method == 'GET':
        streams = []
        sql = """SELECT * FROM Stream"""
        result = mysql.query(sql, 2)
        for res in result:
            stream = {
                'stream_id': res[0],
                'status': res[1],
                'playing_num': res[2],
                'cover_ready': res[3]
            }
            streams.append(stream)
        return jsonify(streams)
    elif request.method == 'POST':
        sql = """INSERT INTO Stream (Status) VALUE ('stop')"""
        stream_id = mysql.query(sql, 0)[0]
        response = {
            'stream_id': stream_id
        }
        return jsonify(response)



@app.route('/publish', methods=['POST'])
def on_publish():
    print(request.form)
    stream_id = request.form['name']
    sql = """UPDATE Stream SET Status = 'publishing' WHERE id = {}""".format(stream_id)
    mysql.query(sql, 1)
    Timer(5, generate_cover, (stream_id,)).start()
    return 'success', 200


@app.route('/play', methods=['POST'])
def on_play():
    print(request.form)
    remote_addr = request.form['addr']
    if 'unix' in remote_addr or remote_addr == '127.0.0.1':
        return 'success', 200
    stream_id = request.form['name']
    sql = """UPDATE Stream SET PlayingNum = PlayingNum+1 WHERE id = {}""".format(stream_id)
    mysql.query(sql, 1)
    return 'success', 200


@app.route('/play_done', methods=['POST'])
def on_play_done():
    print(request.form)
    remote_addr = request.form['addr']
    if 'unix' in remote_addr or remote_addr == '127.0.0.1':
        return 'success', 200
    stream_id = request.form['name']
    sql = """UPDATE Stream SET PlayingNum = PlayingNum-1 WHERE id = {}""".format(stream_id)
    mysql.query(sql, 1)
    return 'success', 200


@app.route('/publish_done', methods=['POST'])
def on_publish_done():
    print(request.form)
    stream_id = request.form['name']
    sql = """UPDATE Stream SET Status = 'stop' WHERE id = {}""".format(stream_id)
    mysql.query(sql, 1)
    return 'success', 200


@app.route('/images/<filename>')
def send_image(filename):
    return send_from_directory('/home/ubuntu/images', filename)


@app.route('/hello')
def hello_world():
    return 'Hello World!'


def generate_cover(stream_id):
    cmd = """ffmpeg -i rtmp://localhost:1935/live/{} -ss 2 -f image2 -vframes 1 /home/ubuntu/images/{}.jpg -y""".format(stream_id, stream_id)
    print(cmd)
    if call(cmd.split(' '), timeout=10) == 0:
        sql = """UPDATE Stream SET CoverReady = TRUE WHERE id = {}""".format(stream_id)
        mysql.query(sql, 1)
    else:
        # Try again after 5 seconds
        Timer(5, generate_cover, (stream_id,)).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)