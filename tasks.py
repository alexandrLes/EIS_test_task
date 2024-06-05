from app import db, House, Apartment, WaterMeter, WaterReading, Tariff, celery
from sqlalchemy import func
from celery import Celery

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)
    return celery


@celery.task(bind=True)
def calculate_rent_task(self, house_id, year, month):
    try:
        house = House.query.get(house_id)
        if not house:
            raise ValueError('House not found')

        water_tariff = Tariff.query.filter_by(type='water').first()
        maintenance_tariff = Tariff.query.filter_by(type='maintenance').first()

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
    except Exception as e:
        logger.error(f"Error in task: {e}")
        raise self.retry(exc=e)
