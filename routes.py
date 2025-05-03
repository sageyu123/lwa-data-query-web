#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

from flask import Flask
from core import eovsa_bundle
import socket
import os

hostname = socket.gethostname()

if hostname == "ovsa":
    app = Flask(__name__, static_folder='/var/www/html/lwaquery/static', static_url_path='/lwaquery/static')
else:
    app = Flask(__name__)

# secret_key_hex = os.getenv('FLARE_FLASK_SECRET_KEY')
# app.secret_key = bytes.fromhex(secret_key_hex)

app.secret_key = b'this-very-secret-aa??yyiiooccvvppoosswwqq'

bundles = eovsa_bundle.set_bundles(app)

#include blueprints below
from blueprints.example import example
app.register_blueprint(example)


# from flask_assets import Environment
# assets_env = Environment(app)
# assets_env.cache = False
