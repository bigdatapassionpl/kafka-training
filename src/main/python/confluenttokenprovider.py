import base64
import datetime
import json
import time

import google.auth
import google.auth.transport.urllib3
import urllib3


# Token Provider Class
class TokenProvider(object):
 def __init__(self, **config):
   self.credentials, _project = google.auth.default(
       scopes=['https://www.googleapis.com/auth/cloud-platform']
   )
   self.http_client = urllib3.PoolManager()
   self.HEADER = json.dumps(dict(typ='JWT', alg='GOOG_OAUTH2_TOKEN'))

 def valid_credentials(self):
   if not self.credentials.valid:
     self.credentials.refresh(google.auth.transport.urllib3.Request(self.http_client))
   return self.credentials

 def get_jwt(self, creds):
   return json.dumps(
       dict(
           exp=creds.expiry.timestamp(),
           iss='Google',
           iat=datetime.datetime.now(datetime.timezone.utc).timestamp(),
           scope='kafka',
           sub=creds.service_account_email,
       )
   )

 def b64_encode(self, source):
   return (
       base64.urlsafe_b64encode(source.encode('utf-8'))
       .decode('utf-8')
       .rstrip('=')
   )

 def get_kafka_access_token(self, creds):
   return '.'.join([
     self.b64_encode(self.HEADER),
     self.b64_encode(self.get_jwt(creds)),
     self.b64_encode(creds.token)
   ])

 def token(self):
   creds = self.valid_credentials()
   # Convert to UTC
   utc_expiry = creds.expiry.replace(tzinfo=datetime.timezone.utc)
   expiry_seconds = (utc_expiry - datetime.datetime.now(datetime.timezone.utc)).total_seconds()

   return self.get_kafka_access_token(creds)


 def confluent_token(self):
   creds = self.valid_credentials()
   # Convert to UTC
   utc_expiry = creds.expiry.replace(tzinfo=datetime.timezone.utc)
   expiry_seconds = (utc_expiry - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
   return self.get_kafka_access_token(creds), time.time() + expiry_seconds

# Confluent does not use a TokenProvider, it calls a method
def ConfluentTokenProvider():
    """Method to get the Confluent Token"""
    t = TokenProvider()
    token = t.confluent_token()
    return token