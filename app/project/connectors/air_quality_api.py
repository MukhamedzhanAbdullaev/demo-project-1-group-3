import requests
import flatdict
from project.assets.pipeline_logging import PipelineLogging

class AirQualityApiClient:
    def __init__(self, api_key: str, pipeline_logging: PipelineLogging):
        
        """
        Initialize AirQualityApiClient instance.

        Args:
        - api_key (str): API key for accessing the Air Quality API.
        - pipeline_logging (PipelineLogging): Pipeline logging instance.
        """
        self.base_url = "https://api.waqi.info/feed"
        if api_key is None:
            raise Exception("API key cannot be set to None.")
        self.api_key = api_key
        self.pipeline_logging = pipeline_logging

    def get_air_quality(self, city: str) -> dict:
        """
        Get the latest air quality data for a specified city.

        Args:
            city: the name of the city in english.

        Returns:
            City air quality index and other related data dictionary

        Raises:
            Exception if response code is not 200.
        """
        city_parsed = city.replace(" ", "-").lower()
        response = requests.get(f"{self.base_url}/{city_parsed}/?token={self.api_key}")
        if response.status_code == 200:
            res = response.json()
            if res['status'] == "ok":
                try:
                    aqi = float(res['data']['aqi'])
                    data = {
                        'city_name': city_parsed,
                        'aqi': aqi
                    }
                    aq_data = res['data']['iaqi']
                    data.update(flatdict.FlatDict(aq_data, delimiter='.'))
                    time_data = res['data']['time']
                    data.update(time_data)
                    return data
                except ValueError:
                    self.pipeline_logging.logger.info(f"Air quality data not available for {city}")
            else:
                self.pipeline_logging.logger.info(f"Data not available for {city}")
        else:
            raise Exception(
                f"Failed to extract data from Air Quality API. Status Code: {response.status_code}. Response: {response.text}"
            )
