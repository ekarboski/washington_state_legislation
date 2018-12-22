import pandas as pd
import urllib
from flask import Flask, request, render_template, jsonify
from web_functions import select_one_bill_from_label_df, calculate_lean_percentages, select_one_leg_from_label_df

app = Flask(__name__, static_url_path="")
# Interesting bills to showcase: 2145 - split votes, 2265 - split sponsors

@app.route('/')
def wa_bill():
    """Return the main page."""
    kwargs = get_info_by_bill()
    return render_template('wa.html', **kwargs)

def get_info_by_bill(bill_id='HB 2145'):
    """Selects bill"""
    selected_senate_table, selected_house_table, rep_score, dem_score, bill_url = select_one_bill_from_label_df(bill_id)
    senate_table = selected_senate_table.to_dict(orient='records')
    house_table = selected_house_table.to_dict(orient='records')
    # bill_text = get_bill_text(bill_id)
    s_leaning_yea, s_leaning_nay, s_undecided = calculate_lean_percentages(selected_senate_table)
    h_leaning_yea, h_leaning_nay, h_undecided = calculate_lean_percentages(selected_house_table)
    house_table_html = render_template('house_table.html', house_table=house_table)
    senate_table_html = render_template('senate_table.html', senate_table=senate_table)
    return dict( rep_score=rep_score, dem_score=dem_score,
                            s_leaning_nay=s_leaning_nay, s_leaning_yea=s_leaning_yea, s_undecided=s_undecided, 
                            h_leaning_nay=h_leaning_nay, h_leaning_yea=h_leaning_yea, h_undecided=h_undecided, 
                            bill_url=bill_url, bill_id=bill_id, house_table_html=house_table_html, senate_table_html=senate_table_html)


@app.route('/bill/<bill_id>')
def get_bill_info_page(bill_id):
    kwargs = get_info_by_bill(bill_id)
    return render_template('wa.html', **kwargs)


@app.route('/leg')
def wa_leg():
    """Return the main page."""
    kwargs = get_info_by_leg()
    return render_template('wa_leg.html', **kwargs)

def get_info_by_leg(last_name='Appleton'):
    """Selects legislator"""
    selected_leg_table = select_one_leg_from_label_df(last_name)
    leg_table = selected_leg_table.to_dict(orient='records')
    leg_table_html = render_template('leg_table.html', leg_table=leg_table)
    return dict(leg_table_html=leg_table_html, leg_table=leg_table)


@app.route('/leg/<last_name>')
def get_leg_info_page(last_name):
    kwargs = get_info_by_leg(last_name)
    return render_template('wa_leg.html', **kwargs)











@app.route('/bill_data/<bill_id>')
def get_bill_info_json(bill_id):
    """Return the main page."""
    return jsonify(get_bill_info(bill_id))


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