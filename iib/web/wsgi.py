# SPDX-License-Identifier: GPL-3.0-or-later
from iib.web.app import create_app
from flask import Flask
from opentelemetry.instrumentation.flask import FlaskInstrumentor

app = create_app()
FlaskInstrumentor().instrument_app(app)