from lstore.db import Database

db = Database()
db.open('./ECS165')
db.tables['Grades'].cache.num_slots = 100000000




print("break here")
# db.close()






