import csv, re

result = []
with open("job_db.csv", "r", newline='', encoding='utf-8-sig', errors='ignore') as f:
    lines = csv.reader(f)

    for line in lines:

        desc = line[-1]
        pattern = '.*years.*'
        re_year = re.compile(pattern)
        year = re_year.findall(desc)
        if year:
            result.append([line[1], line[2], line[3], line[4], year])

    print(result[0:5])
