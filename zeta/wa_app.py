import pandas as pd
from flask import Flask, request, render_template, jsonify

app = Flask(__name__, static_url_path="")

@app.route('/')
def index():
    """Return the main page."""
    return render_template('index.html')

@app.route('/services')
def services():
    """Return the main page."""
    return render_template('services.html')

@app.route('/elements')
def elements():
    """Return the main page."""
    return render_template('elements.html')

@app.route('/blog')
def blog():
    """Return the main page."""
    return render_template('blog.html')

@app.route('/contact')
def contact():
    """Return the main page."""
    return render_template('contact.html')