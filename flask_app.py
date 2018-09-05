#!/usr/bin/env python3
from flask import Flask, jsonify
import numpy as np
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, load_only
from sqlalchemy import create_engine, func, inspect, and_


# database ORM setup
engine = create_engine("sqlite:///resources/hawaii.sqlite?check_same_thread=False")
Base = automap_base()
Base.prepare(engine, reflect=True)
Measurement = Base.classes.measurement
Station = Base.classes.station
session = Session(engine)

# for dates
fake_today = parse("2017-08-14")
dt = relativedelta(years=1)
year_ago = fake_today - dt
year_ago_str = year_ago.strftime("%Y-%m-%d")

# get start and end data, we'll need them later;
# ordered descending
xx = session.query(Measurement)\
            .order_by(Measurement.date.desc()).all()
data_start_date = xx[-1].date
data_end_date = xx[0].date

app = Flask(__name__)

@app.route('/')
def index():

    msg = "You may use the following API endpoints:"
    msg += "<br><br> /api/v1.0/precipitation"
    msg += "<br> /api/v1.0/stations"
    msg += "<br> /api/v1.0/tobs"
    msg += "<br> /api/v1.0/<start>"
    msg += "<br> pi/v1.0/&lang;start&rang;/&lang;end&rang;"
    msg += "<br> <br> Enjoy!"
    
    return msg

@app.route('/api/v1.0/precipitation')
def prec():

    # the instructions on this don't make sense; they
    # say to return temperature observations here.
    # I'll return rain observations instead.
    
    query = session.query(Measurement)
    rain = query.order_by(Measurement.date)\
                .filter(and_(Measurement.date >= year_ago_str, \
                             Measurement.date < fake_today))
    df = pd.read_sql(rain.options(load_only("date","prcp"))\
                 .statement,session.bind)

    del df['id']
    df.fillna(0.0,inplace=True)
    dfg = df.groupby("date").mean()
    dfg.head(5)
    
    md = dfg.to_dict()['prcp']

    return jsonify(md)

@app.route('/api/v1.0/stations')
def stations():

    df = pd.read_sql(session.query(Station).statement,session.bind)
    del df['id']

    resdict = df.to_dict(orient='records')

    return jsonify(resdict)


@app.route('/api/v1.0/tobs')
def tobs():

    query = session.query(Measurement.station,
                          Measurement.date,
                          Measurement.tobs)
    temp = query.order_by(Measurement.date)\
                .filter(and_(Measurement.date >= year_ago_str, \
                             Measurement.date < fake_today))
    df = pd.read_sql(temp.statement,session.bind)

    md = df.to_dict(orient='records')
    
    return jsonify(md)


def calc_temps(start_date, end_date):

    res = session.query(func.min(Measurement.tobs),\
                        func.avg(Measurement.tobs),\
                        func.max(Measurement.tobs))\
                 .filter(Measurement.date >= start_date)\
                 .filter(Measurement.date <= end_date).all()

    return res[0]

@app.route('/api/v1.0/<start_date>/<end_date>')
def temp_data(start_date,end_date):
    
    msg = f"Please pick valid dates between {data_start_date}"\
          f" and {data_end_date}!"
    err_date = jsonify({'error': msg})
    
    try:
        xstart = parse(start_date)
        xend = parse(end_date)
    except:
        return err_date
    
    print(start_date,end_date)

    if xstart < parse(data_start_date) or \
       xstart > parse(data_end_date):
        return err_date

    if xend < parse(data_start_date) or \
       xend > parse(data_end_date):
        return err_date
    
    res = calc_temps(start_date,end_date)

    # the instructions say json list, but
    # I like this better
    
    rd = {}
    rd['TMIN'] = f"{res[0]:.1f}"
    rd['TAVG'] = f"{res[1]:.1f}"
    rd['TMAX'] = f"{res[2]:.1f}"
    rd['start_date'] = start_date
    rd['end_date'] = end_date
    
    return jsonify(rd)


@app.route('/api/v1.0/<start_date>')
def temp_data_from_start(start_date):

    # this is really just a special case of the
    # more general function defined above
    return temp_data(start_date,data_end_date)
    

if __name__ == '__main__':
    app.run(debug=True)



