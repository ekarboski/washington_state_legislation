import pandas as pd
from flask import Flask, request, render_template, jsonify
from web_functions import select_one_bill_from_label_df, get_bill_text, calculate_lean_percentages

app = Flask(__name__, static_url_path="")

@app.route('/')
def wa():
    """Return the main page."""
    # Interesting bills to showcase: 2145 - split votes, 2265 - split sponsors
    bill_id = 'HB 2145'
    selected_senate_table, selected_house_table, rep_score, dem_score, bill_url = select_one_bill_from_label_df(bill_id)
    senate_table = selected_senate_table.to_dict(orient='records')
    house_table = selected_house_table.to_dict(orient='records')
    bill_text = get_bill_text(bill_id)
    s_leaning_yea, s_leaning_nay, s_undecided = calculate_lean_percentages(selected_senate_table)
    h_leaning_yea, h_leaning_nay, h_undecided = calculate_lean_percentages(selected_house_table)
    linkedin = "https://www.linkedin.com/in/emilykarboski"
    return render_template('wa.html', senate_table=senate_table, house_table=house_table, 
                            rep_score=rep_score, dem_score=dem_score, bill_text=bill_text,
                            s_leaning_nay=s_leaning_nay, s_leaning_yea=s_leaning_yea, s_undecided=s_undecided, 
                            h_leaning_nay=h_leaning_nay, h_leaning_yea=h_leaning_yea, h_undecided=h_undecided, 
                            bill_url=bill_url, bill_id=bill_id, linkedin=linkedin)

@app.route('/about')
def about():
    """Return the main page."""
    return render_template('wa-about.html')


@app.route('/contact')
def contact():
    """Return the main page."""
    return render_template('wa-contact.html')





# OLD, FROM TEMPLATE
@app.route('/main')
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