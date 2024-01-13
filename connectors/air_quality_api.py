import requests
import flatdict

class AirQualityApiClient:
    def __init__(self, api_key: str):
        self.base_url = "https://api.waqi.info/feed"
        if api_key is None:
            raise Exception("API key cannot be set to None.")
        self.api_key = api_key

    def get_city(self, city_name: str) -> dict:
        """
        Get the latest air quality data for a specified city.

        Args:
            city_name: the name of the city in english.

        Returns:
            City temperature dictionary

        Raises:
            Exception if response code is not 200.
        """
        response = requests.get(f"{self.base_url}/{city_name}/?token={self.api_key}")
        if response.status_code == 200:
            res = response.json()
            if res['status'] == "ok":
                data = {
                    'aqi': res['data']['aqi']
                }
                weather_data = res['data']['iaqi']
                data.update(flatdict.FlatDict(weather_data, delimiter='.'))
                time_data = res['data']['time']
                data.update(time_data)
                return data
        else:
            raise Exception(
                f"Failed to extract data from Open Weather API. Status Code: {response.status_code}. Response: {response.text}"
            )
