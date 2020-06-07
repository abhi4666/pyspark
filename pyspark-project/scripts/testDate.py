from datetime import datetime

this_month = datetime.strftime(datetime.now(), '%Y-%m')

day = datetime.strftime(datetime.now(),'%Y-%m-%d')
print(day)

print(this_month)