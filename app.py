# -*- coding: utf-8 -*-
import csv
import datetime
import os
import threading
import time
from collections import namedtuple
from contextlib import contextmanager
from queue import Queue

import requests
from awsauth import S3Auth
from flask import Flask, json, request

app = Flask(__name__)


ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_KEY']
LOGGLY_TOKEN = os.environ['LOGGLY_TOKEN']
MAX_TRIES = os.environ.get('MAX_TRIES', 15)  # 15 means I will give up after ~9 hours
LOGGLY_TAG = os.environ.get('LOGGLY_TAG', 'elb')

loggly_url = 'http://logs-01.loggly.com/bulk/{}/{}/bulk/'.format(
    LOGGLY_TOKEN,
    LOGGLY_TAG,
)

s3ssion = requests.Session()
s3ssion.auth = S3Auth(ACCESS_KEY, SECRET_KEY)

Task = namedtuple('Task', ['url', 'not_before', 'tries'])


class MyQueue(Queue):
    @contextmanager
    def task(self):
        try:
            yield self.get()
        finally:
            self.task_done()

q = MyQueue()

header = [
    'timestamp',
    'elb',
    'client',
    'backend',
    'request_processing_time',
    'backend_processing_time',
    'response_processing_time',
    'elb_status_code',
    'backend_status_code',
    'received_bytes',
    'sent_bytes',
    'request',
    'user_agent',
    'ssl_cipher',
    'ssl_protocol'
]


class Dialect(csv.Dialect):
    delimiter = ' '
    quotechar = '"'
    lineterminator = '\n'
    escapechar = '\\'
    quoting = csv.QUOTE_ALL


def download_and_process(s3_object_url):
    response = s3ssion.get(s3_object_url, timeout=5)
    reader = csv.DictReader(response.text.splitlines(), fieldnames=header, restval=None, dialect=Dialect)
    output = []
    for row in reader:
        row['client_ip'], row['client_port'] = row.pop('client').split(':')
        row['http_method'], row['request_uri'], row['http_version'] = row.pop('request').split(' ')
        output.append(json.dumps(row))
    response = requests.post(
        loggly_url,
        data='\n'.join(output),
        headers={'content-type': 'application/json'},
        timeout=5,
    )


def worker():
    while True:
        with q.task() as task:
            url, not_before, tries = task
            if not_before <= datetime.datetime.now():
                tries += 1
                try:
                    download_and_process(task.url)
                    continue
                except Exception:
                    not_before = datetime.datetime.now() + datetime.timedelta(seconds=2**tries)
            if tries <= MAX_TRIES:
                q.put(Task(url, not_before, tries))
            time.sleep(10)

t = threading.Thread(target=worker)
t.daemon = True
t.start()


@app.route('/sns', methods=['GET', 'POST'])
def sns():
    headers = request.headers
    msg_type = headers.get('x-amz-sns-message-type')
    obj = request.get_json()

    if msg_type == 'SubscriptionConfirmation':
        subscribe_url = obj[u'SubscribeURL']
        response = requests.get(subscribe_url)

    elif msg_type == 'UnsubscribeConfirmation':
        pass

    elif msg_type == 'Notification':
        message = json.loads(obj['Message'])
        for record in message['Records']:
            url = 'http://{}.s3.amazonaws.com/{}'.format(
                record['s3']['bucket']['name'],
                record['s3']['object']['key']
            )
            q.put(Task(url, datetime.datetime.now(), 0))

    return '', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
