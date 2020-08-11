from _datetime import datetime

import ibm_db

from flask import Flask, render_template, request

app = Flask(__name__)

APP_ENV = {
    'application_name': 'Assignment2'
}


def db_connect():
    connect_str = "DATABASE=BLUDB;" \
                  "hostname=dashdb-txn-sbox-yp-dal09-03.services.dal.bluemix.net;" \
                  "port=50000;" \
                  "protocol=tcpip;" \
                  "uid=qgd93857;" \
                  "pwd=c660zwb3ch@kcxsz"
    return ibm_db.connect(
        connect_str,
        "",
        ""
    )


def is_night(dt):
    local_dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ').time().hour
    if local_dt >= 22 or local_dt <= 7:
        return True
    else:
        return False


def greater_than_mag(mag=None):
    conn = db_connect()
    if conn:
        statement = ibm_db.prepare(
            conn,
            'select * from earthquake where "mag">=?'
        )
        ibm_db.bind_param(statement, 1, mag)
        ibm_db.execute(statement)

        rows = []
        res = ibm_db.fetch_assoc(statement)
        while res:
            rows.append(res.copy())
            res = ibm_db.fetch_assoc(statement)

        ibm_db.close(conn)
        return render_template('results.html', rows=rows)

    else:
        return 'Failed to connect DB', 500


def min_max_mag_given_duration(min_mag, max_mag, start_date, end_date):
    conn = db_connect()
    if conn:
        statement = ibm_db.prepare(
            conn,
            'select * from earthquake where "mag" > ? and "mag" < ? and "time" > ? and "time" < ?'
        )
        ibm_db.bind_param(statement, 1, min_mag)
        ibm_db.bind_param(statement, 2, max_mag)
        ibm_db.bind_param(statement, 3, start_date)
        ibm_db.bind_param(statement, 4, end_date)
        ibm_db.execute(statement)

        rows = []
        res = ibm_db.fetch_assoc(statement)
        while res:
            rows.append(res.copy())
            res = ibm_db.fetch_assoc(statement)

        ibm_db.close(conn)
        return render_template('results.html', rows=rows)

    else:
        return 'Failed to connect DB', 500


def find_in_radius(lat, lng, radius):
    conn = db_connect()
    if conn:
        lng1 = lng - (radius * 0.014)
        lng2 = lng + (radius * 0.014)
        lat1 = lat - radius
        lat2 = lat + radius

        statement = ibm_db.prepare(
            conn,
            'select * from earthquake where "latitude" > ? and "latitude" < ? and "longitude" > ? and "longitude" < ?'
        )
        ibm_db.bind_param(statement, 1, str(lat1))
        ibm_db.bind_param(statement, 2, str(lat2))
        ibm_db.bind_param(statement, 3, str(lng1))
        ibm_db.bind_param(statement, 4, str(lng2))
        ibm_db.execute(statement)

        rows = []
        res = ibm_db.fetch_assoc(statement)
        while res:
            rows.append(res.copy())
            res = ibm_db.fetch_assoc(statement)

        ibm_db.close(conn)
        return render_template('results.html', rows=rows)

    else:
        return 'Failed to connect DB', 500


def day_night(mag):
    conn = db_connect()
    if conn:
        statement = ibm_db.prepare(
            conn,
            'select "time" from earthquake where "mag" > ?'
        )
        ibm_db.bind_param(statement, 1, mag)
        ibm_db.execute(statement)

        # fetch the results conver the time to local time and count the day and night results
        count_n = 0  # counter for earthquakes at night
        count_t = 0  # counter for total earthquakes
        res = ibm_db.fetch_assoc(statement)
        while res:
            row = res.copy()
            # check if the earthquake occured at night and increment the counter accordingly
            if is_night(row['time']):
                count_n = count_n + 1

            count_t = count_t + 1
            res = ibm_db.fetch_assoc(statement)

        ibm_db.close(conn)
        return render_template('day_night.html', night=count_n, day=count_t - count_n, total=count_t)
    else:
        return 'Failed to connect DB', 500


# -----------------------------------------ROUTES------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html', app=APP_ENV)


@app.route('/greater-than-mag', methods=['GET'])
def greater_than_mag_route():
    name = request.args.get('mag', '')
    return greater_than_mag(name)


@app.route('/min-max-mag', methods=['GET'])
def min_max_mag_route():
    min_mag = request.args.get('min', '')
    max_mag = request.args.get('max', '')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    return min_max_mag_given_duration(min_mag, max_mag, start_date, end_date)


@app.route('/find-in-radius', methods=['GET'])
def find_in_radius_route():
    lat = request.args.get('lat', '')
    lng = request.args.get('lng', '')
    radius = request.args.get('radius', '')
    return find_in_radius(float(lat), float(lng), int(radius))


@app.route('/day_night', methods=['GET'])
def day_night_route():
    mag = request.args.get('mag', '')
    return day_night(mag)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
