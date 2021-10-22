import os

xxx = 'script1.sql'
fullpath = os.path.join('script', xxx)
query = ""
with open(fullpath, encoding='CP1251') as f:
    for line in f:
        query += line

print(query.format(xxx))