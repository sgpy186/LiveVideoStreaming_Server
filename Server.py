from flask import Flask, send_from_directory, request, jsonify
from threading import Timer
from subprocess import call
import json
import requests

import mysql

app = Flask(__name__)


@app.route('/stream', methods=['GET', 'POST'])
def _stream():
    if request.method == 'GET':
        # Clean invalid streams
        sql = """DELETE FROM Stream WHERE CoverReady = 0 AND Status = 'stop'"""
        mysql.query(sql, 1)
        streams = []
        sql = """SELECT * FROM Stream ORDER BY id DESC"""
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
        print('stream_id: {}'.format(stream_id))
        return jsonify(response)


@app.route('/publish', methods=['POST'])
def on_publish():
    print(request.form)
    stream_id = request.form['name']
    print('stream_id: {}'.format(stream_id))
    sql = """SELECT * FROM Stream WHERE id = %s""" % stream_id
    result = mysql.query(sql, 1)
    if result is None or len(result) == 0:
        return 'Fail', 404
    sql = """UPDATE Stream SET Status = 'publishing' WHERE id = %s""" % (stream_id)
    mysql.query(sql, 1)
    Timer(5, generate_cover, (stream_id,)).start()
    return 'success', 200


@app.route('/play', methods=['POST'])
def on_play():
    remote_addr = request.form['addr']
    if 'unix' in remote_addr or remote_addr == '127.0.0.1':
        return 'success', 200
    print(request.form)
    stream_id = request.form['name']
    sql = """UPDATE Stream SET PlayingNum = PlayingNum+1 WHERE id = %s""" % (stream_id)
    mysql.query(sql, 1)
    return 'success', 200


@app.route('/play_done', methods=['POST'])
def on_play_done():
    remote_addr = request.form['addr']
    if 'unix' in remote_addr or remote_addr == '127.0.0.1':
        return 'success', 200
    print(request.form)
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


@app.route('/register', methods=['POST'])
def register_phone():
    data = request.get_json(force=True)
    print(data)
    reg_id = data['reg_id']
    sql = """INSERT IGNORE INTO RegID(RegID) VALUES ('{}')""".format(reg_id)
    mysql.query(sql, 1)
    return jsonify({'status': 'success'})


@app.route('/comment', methods=['POST'])
def comment_post():
    data = request.get_json(force=True)
    print(data)
    content = data['content']
    message = {'content': content}
    sql = """SELECT RegID FROM RegID"""
    result = mysql.query(sql, 2)
    registration_ids = []
    for res in result:
        registration_ids.append(res[0])
    push_ios(registration_ids, message)
    stream_id = data['stream_id']
    content = mysql.escape(content)
    sql = """INSERT INTO Comment (StreamID, Content) VALUES ({}, '{}')""".format(stream_id, content)
    mysql.query(sql, 1)
    return jsonify({'status': 'success'})


@app.route('/images/<filename>')
def send_image(filename):
    return send_from_directory('/home/ubuntu/images', filename)


@app.route('/hello')
def hello_world():
    return 'Hello World!'


def generate_cover(stream_id):
    sql = """SELECT Status FROM Stream WHERE id = {} AND Status != 'stop'""".format(stream_id)
    result = mysql.query(sql, 1)
    if result is None:
        print('Stream: {} is stopped or does not exist, cannot generate cover'.format(stream_id))
        return
    cmd = """ffmpeg -loglevel panic -i rtmp://localhost:1935/live/{} -ss 2 -f image2 -vframes 1 /home/ubuntu/images/{}.jpg -y""".format(stream_id, stream_id)
    print(cmd)
    if call(cmd.split(' '), timeout=10) == 0:
        print('Generating cover successfully for stream: {}'.format(stream_id))
        sql = """UPDATE Stream SET CoverReady = TRUE WHERE id = {}""".format(stream_id)
        mysql.query(sql, 1)
    else:
        # Try again after 5 seconds
        Timer(5, generate_cover, (stream_id,)).start()


def push_ios(device_id, message):
    """
    Send push notification to iOS device
    :param device_id: Target user device ID (looked up from androidRegID table)
    :param message: Message to send, must follow push notification data structure
    :return: True if success, otherwise False
    """
    headers = {'Content-type': 'application/json', 'Authorization': 'key=AIzaSyAZdTKPvXfHljIH1MC83wR1AeY0RTE_g3g'}
    url = 'https://fcm.googleapis.com/fcm/send'
    body = {
        "registration_ids": device_id,
        "notification": {
            "body": message['content'],
            "title": 'New comment',
            "icon": "myicon",
            'sound': 'default',
            'badge': 1,
            'content_available': True
        },
        "data": message,
        "sound": "default",
        "badge": 1,
        "priority": "high"
    }
    data = json.dumps(body)
    response = requests.post(url, data=data, headers=headers)
    print(response.json())
    return True

if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000, debug=True)
