from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from tasks import make_celery, calculate_rent_task

db = SQLAlchemy()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user_main:123@localhost/main'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
db.init_app(app)
app.app_context().push()
celery = make_celery(app)

class House(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    address = db.Column(db.String(100), nullable=False)
    apartments = db.relationship('Apartment', backref='house', lazy=True)

class Apartment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    area = db.Column(db.Float, nullable=False)
    house_id = db.Column(db.Integer, db.ForeignKey('house.id'), nullable=False)
    water_meters = db.relationship('WaterMeter', backref='apartment', lazy=True)

class WaterMeter(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    apartment_id = db.Column(db.Integer, db.ForeignKey('apartment.id'), nullable=False)
    readings = db.relationship('WaterReading', backref='water_meter', lazy=True)

class WaterReading(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    meter_id = db.Column(db.Integer, db.ForeignKey('water_meter.id'), nullable=False)
    month = db.Column(db.Integer, nullable=False)
    year = db.Column(db.Integer, nullable=False)
    value = db.Column(db.Float, nullable=False)

class Tariff(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String(50), nullable=False)
    price = db.Column(db.Float, nullable=False)

@app.route('/house/<int:house_id>', methods=['GET'])
def get_house(house_id):
    house = House.query.get(house_id)
    if not house:
        return jsonify({'message': 'House not found'}), 404
    house_data = {
        'id': house.id,
        'address': house.address,
        'apartments': [{
            'id': apartment.id,
            'area': apartment.area,
            'water_meters': [{
                'id': meter.id,
                'readings': [{'month': reading.month, 'year': reading.year, 'value': reading.value} for reading in meter.readings]
            } for meter in apartment.water_meters]
        } for apartment in house.apartments]
    }
    return jsonify(house_data)

@app.route('/calculate-rent/<int:house_id>/<int:year>/<int:month>', methods=['POST'])
def calculate_rent(house_id, year, month):
    task = calculate_rent_task.apply_async(args=[house_id, year, month])
    return jsonify({'task_id': task.id}), 202

@app.route('/progress/<task_id>', methods=['GET'])
def get_progress(task_id):
    task = calculate_rent_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'progress': 0
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'progress': task.info.get('progress', 0),
            'result': task.info.get('result', {})
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        response = {
            'state': task.state,
            'progress': 0,
            'result': str(task.info)  # this is the exception raised
        }
    return jsonify(response)

@celery.task(bind=True)
def calculate_rent_task(self, house_id, year, month):
    house = House.query.get(house_id)
    if not house:
        raise ValueError('House not found')

    water_tariff = Tariff.query.filter_by(type='water').first()
    maintenance_tariff = Tariff.query.filter_by(type='maintenance').first()
    print(f'++++++++++++++++++ {water_tariff}')


    if not water_tariff or not maintenance_tariff:
        raise ValueError('Tariffs not found')

    total_apartments = len(house.apartments)
    completed_apartments = 0

    results = []

    for apartment in house.apartments:
        water_consumption = 0
        for meter in apartment.water_meters:
            readings = WaterReading.query.filter_by(meter_id=meter.id).order_by(WaterReading.year, WaterReading.month).all()
            if len(readings) >= 2:
                current_reading = next((r for r in readings if r.year == year and r.month == month), None)
                previous_reading = next((r for r in readings if r.year == year and r.month == month-1), None)
                if current_reading and previous_reading:
                    water_consumption += current_reading.value - previous_reading.value

        water_cost = water_tariff.price * water_consumption
        maintenance_cost = maintenance_tariff.price * apartment.area
        total_cost = water_cost + maintenance_cost

        results.append({
            'apartment_id': apartment.id,
            'water_cost': water_cost,
            'maintenance_cost': maintenance_cost,
            'total_cost': total_cost
        })

        completed_apartments += 1
        self.update_state(state='PROGRESS', meta={'progress': (completed_apartments / total_apartments) * 100})
    return {'progress': 100, 'result': results}

if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
