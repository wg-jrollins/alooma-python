import sys
import requests
from requests.exceptions import SSLError
from contextlib import contextmanager
import os
import datetime

def _obtain_cookie(hostname, username, password, port="8443", login_path="rest/login"):
    url = "https://{hostname}:{port}/{path}".format(
        hostname=hostname, port=port, path=login_path)
    login_data = {"email": username, "password": password}
    response = requests.post(url, json=login_data)
    if response.status_code == 200:
        return response.cookies
    return None


def obtain_cookie(hostname, username, password, port="8443", login_path="rest/login"):
    print ("Attempting to obtain cookie from {}...".format(hostname))
    cookie = None
    try:
        cookie = _obtain_cookie(hostname, username, password, port, login_path)
        if cookie:
            print ("OK!")
    except SSLError as e:
        print ("Failed to obtain a session cookie from {}. Reason {}".format(
            hostname, e))
    finally:
        return cookie


@contextmanager
def temp_dir(commit_id):
    """create a temp dir that will be cleaned up at the end of
    the with block
    """
    dir_name = os.path.abspath('/tmp/{dt.year}-{dt.month:02}-{dt.day:02}_'
                               '{dt.hour:02}-{dt.minute:02}-{dt.second:02}'
                               '__{commit}'.format(
                                   dt=datetime.datetime.now(),
                                   commit=commit_id))
    if not dir_name.startswith('/tmp/'):
        raise Exception('Bad dir name: ' + dir_name)

    os.mkdir(dir_name)

    yield dir_name

    try:
        for filename in os.listdir(dir_name):
            os.unlink(os.path.join(dir_name, filename))
        os.rmdir(dir_name)
    except OSError:
        print('failed to clean up {}'.format(dir_name))


@contextmanager
def no_output(ignore=False):
    if ignore:
        yield
        return

    stdout_orig = None
    stderr_orig = None
    dev_null = None

    stdout_orig = sys.stdout
    stderr_orig = sys.stderr
    dev_null = open(os.devnull, 'w')
    sys.stdout = dev_null
    sys.stderr = dev_null

    yield

    dev_null.close()
    dev_null = None
    sys.stdout = stdout_orig
    sys.stderr = stderr_orig
