import requests
from typing import Tuple, Union


class ZenSerp:
    """
    Convenience object to handle ZenSerp requests
    Attributes:
        api_key: string, a valid api key from ZenSerp
        session: A request session object, used to connect to ZenSerp through
        status_link: The ZenSerp link to get the status from
        trends_link: The ZenSerp link to requests trends data from
    Methods:
        get_trends: Request method for getting trend data given the parameters
        get_status: Request method for getting the current status of the api key
    """
    def __init__(self, api_key: str):
        """
        Initializer method
        Arguments:
            api_key: string, a valid api key from ZenSerp
        """
        self.api_key = api_key
        self.session = requests.session()

        self.status_link = 'https://app.zenserp.com/api/v2/status'
        self.trends_link = 'https://app.zenserp.com/api/v1/trends'

    def get_trends(self, params: Tuple[Tuple[str, str], ...], json: bool = True) -> Union[dict, requests.Response]:
        """
        Method for getting trends data
        Arguments:
            params: tuple of tuple containing the request parameters. See ZenSerp to get parameters format
            json: boolean, denoting if the return value should be a json or a request.Response object
        Returns:
            dict if json is True, else the requests.Response object of the response
        """
        response = self.session.get(self.trends_link, params=params, headers={'apikey': self.api_key})
        response = self.__generic_check(response)
        if json:
            return response.json()
        return response

    def get_status(self) -> dict:
        """
        Method for getting the status of the api key
        Returns:
            Dict containing the status information of the api key
        """
        response = self.session.get(self.status_link, headers={'apikey': self.api_key})
        return self.__generic_check(response).json()

    def __generic_check(self, response: requests.Response) -> requests.Response:
        if response.ok:
            return response
        if self.get_status()['remaining_requests'] <= 0:
            raise requests.ConnectionError('Api key is out of requests')
        raise requests.ConnectionError('Could not connect to host servers')


if __name__ == '__main__':
    myKey = ''
    serp = ZenSerp(myKey)
    print(serp.get_trends(params=(("keyword[]", "Joe Biden"), ("keyword[]", "Donald Trump"))))
    print(serp.get_status())
