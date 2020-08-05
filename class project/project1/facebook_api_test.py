from pyfacebook import Api

token= 'EAAJpYZCq7EroBAFbROdTMr8tBa6fNWvdMU4ZCnZBYNPadW1ZBqXMtCqI0qW4tF8ZBolm4E5ICvJa8NUQCXogFWTYcOYrgY44QYoxmcrCVbA53PewMq0upDETiKP5xCZBbCSloyJGI1bDY7nGVXq4VMLd1SSny8VQOCedrN6efrZBEc2tZB1xHzAtUaTTI2Wa5DislHsyV0rIwV8NTxBnyIiuxnmU92ruZCkwZD'
app_id = '678828082664122'
app_secret = '380c4b1c22f2754483b02ee3a4dcffbd'
api = Api(app_id=app_id, app_secret=app_secret, long_term_token=token)
page_info = api.get_page_info(username='Cristiano')
print(page_info)

