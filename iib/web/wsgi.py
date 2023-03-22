# SPDX-License-Identifier: GPL-3.0-or-later
from iib.web.app import create_app
from flask import request

app = create_app()


@app.before_request
def log_request_info():
    app.logger.debug('Headers: %s', request.headers)
    app.logger.debug('Body: %s', request.get_data())
