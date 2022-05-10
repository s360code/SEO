from google.colab import auth, output
import gspread
from oauth2client.client import GoogleCredentials

from google.auth import default

class Authenticating:


  def __init___():
    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)

