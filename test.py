import requests

# Запуск расчета квартплаты
url = "http://127.0.0.1:5000/calculate-rent/1/2024/1"
response = requests.post(url)

if response.status_code == 202:
    task_id = response.json().get('task_id')
    print(f"Task ID: {task_id}")

    # Проверка прогресса задачи
    progress_url = f"http://127.0.0.1:5000/progress/{task_id}"
    progress_response = requests.get(progress_url)
    print(progress_response.json())
else:
    print(f"Failed to start calculation: {response.text}")
