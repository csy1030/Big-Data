import re

pattern = "^(.*?) at (.*?)$"
r = re.compile(pattern,re.S)
title_company = "Software Design Engineer at Idea Entity"
title = r.findall(title_company)[0][0]
company = r.findall(title_company)[0][1]
print(company)
