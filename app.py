# -*- coding: utf-8 -*-
import csv
import os
import threading
from contextlib import contextmanager
from queue import Queue
from flask import Flask, request, json
import requests
from awsauth import S3Auth

app = Flask(__name__)


ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_KEY']
LOGGLY_TOKEN = os.environ['LOGGLY_TOKEN']
LOGGLY_TAG = os.environ.get('LOGGLY_TAG', 'elb')

loggly_url = 'http://logs-01.loggly.com/bulk/{}/{}/bulk/'.format(
    LOGGLY_TOKEN,
    LOGGLY_TAG,
)

s3ssion = requests.Session()
s3ssion.auth = S3Auth(ACCESS_KEY, SECRET_KEY)


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
    response = s3ssion.get(s3_object_url)
    reader = csv.DictReader(response.text.splitlines(), fieldnames=header, restval=None, dialect=Dialect)
    output = []
    for row in reader:
        row['client_ip'], row['client_port'] = row.pop('client').split(':')
        row['http_method'], row['request_uri'], row['http_version'] = row.pop('request').split(' ')
        output.append(json.dumps(row))
    response = requests.post(loggly_url, data='\n'.join(output), headers={'content-type': 'application/json'})


def worker():
    while True:
        with q.task() as item:
            download_and_process(item)

t = threading.Thread(target=worker)
t.daemon = True
t.start()


@app.route('/sns', methods=['GET', 'POST'])
def sns():
    headers = request.headers
    arn = headers.get('x-amz-sns-subscription-arn')
    msg_type = headers.get('x-amz-sns-message-type')
    obj = json.loads(request.data)

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
            q.put(url)

    return '', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
