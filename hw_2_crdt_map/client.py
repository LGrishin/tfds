import requests

class Client:
    def __init__(self):
        pass

    def get(self, key, node_port):
        """Запрашивает значение по ключу у узла."""
        url = f'http://localhost:{node_port}/{key}'
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                # print(f"Получено значение: {data['value']} для ключа: {data['key']}")
                return data['value']
            else:
                print(f"Ошибка: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе: {e}")
        
    def patch(self, operations, node_port):
        """Выполняет последовательный список операций над хранилищем узла."""
        url = f'http://localhost:{node_port}/operations'
        payload = {'operations': operations}
        try:
            response = requests.patch(url, json=payload)
            if response.status_code == 200:
                data = response.json()
                # print(f"Успешно выполнены операции: {len(data['data'])}")
            else:
                print(f"Ошибка: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при выполнении операций: {e}")
