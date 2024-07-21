from pydantic import BaseModel


class WeatherApiSettings(BaseModel):
    API_URI: str = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
    HEADER: dict = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:35.0) Gecko/20100101 Firefox/35.0"}
