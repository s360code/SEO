from google.colab import auth
import gspread
from oauth2client.client import GoogleCredentials


class Auth:
    """
    Convenience object for Google Authentication
    Attributes:
        credentials: Google credentials object via oauth2client
        client: GSpread authorization client
    Methods:
        authenticate: Method for getting an authenticated client via GSpread
        re_authenticate: Method to re-authenticate the client if timeout has occurred
    """
    def __init__(self):
        """Initializer method"""
        auth.authenticate_user()
        self.credentials = GoogleCredentials.get_application_default()
        self.client = None

    def authenticate(self) -> gspread.client.Client:
        """
        Authenticator method for getting an authenticated client
        Returns:
            GSpread client object with the Google credentials
        """
        self.client = gspread.authorize(self.credentials)
        return self.client

    def re_authenticate(self):
        """
        Re-authentication method. Gets a new token if the old one has expired
        """
        if self.credentials.access_token_expired:
            self.client.login()  # refreshes the token
