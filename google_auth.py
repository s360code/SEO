from google.colab import auth, output
import gspread
from oauth2client.client import GoogleCredentials

from google.auth import default

class Authenticating:


  def __init___(self):
    auth.authenticate_user()
    creds, _ = default()
  
  def authenticate(self):
    gc = gspread.authorize(creds)

