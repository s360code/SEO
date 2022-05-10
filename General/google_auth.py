from google.colab import auth, output
import gspread
from oauth2client.client import GoogleCredentials
from google.auth import default

def Authenticate():
  auth.authenticate_user()
  creds, _ = default()
  gc = gspread.authorize(creds)
