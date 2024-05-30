from celery_app import celery
from models import db, House, Apartment, WaterMeter, WaterReading, Tariff
from sqlalchemy import func

@celery.task(bind=True)
def calculate_rent(self, house_id, month, year):
    house = House.query.get(house_id)
    if not house:
        return {'status': 'failed', 'message': 'House not found'}

    # Получаем тарифы
    water_tariff = Tariff.query.filter_by(type='Водоснабжение').first().price
    maintenance_tariff = Tariff.query.filter_by(type='Содержание общего имущества').first().price

    total_apartments = len(house.apartments)
    processed_apartments = 0

    for apartment in house.apartments:
        # Рассчитываем плату за водоснабжение
        total_water_cost = 0
        for meter in apartment.water_meters:
            current_reading = WaterReading.query.filter_by(meter_id=meter.id, month=month, year=year).first()
            previous_reading = WaterReading.query.filter(
                WaterReading.meter_id == meter.id,
                func.date_part('month', WaterReading.date) == month - 1,
