from flask import Flask
from flask import Flask, Response, render_template, request, jsonify, redirect
from lstore.db import Database
from lstore.query import Query

app = Flask(__name__)
db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

@app.route("/", methods=['GET', 'POST'])
def hello_world():
    if request.method == 'GET':
        return render_template(
            'index.html'
        )
    else:
        user_name = int(request.form['name'])
        user_class_1 = int(request.form['ecs165a'])
        user_class_2 = int(request.form['ecs120'])
        user_class_3 = int(request.form['ecs132'])
        user_class_4 = int(request.form['ecs160'])
        record = [user_name, user_class_1, user_class_2, user_class_3, user_class_4]
        print(record)
        # return Response(status=200,
        #                 response="success")
        query.insert(*record)
        print("record inserted")
        return ("", 204)

@app.route("/select", methods=['POST'])
def select_record():
    user_name = int(request.form['name'])
    record = query.select(user_name, 0, [1, 1, 1, 1, 1])[0]
    class_name = ["ECS165A", "ECS120", "ECS132", "ECS160"]
    print(record.columns)
    return render_template("index.html", user_id = record.columns[0], record = record.columns[1:], class_name = class_name)


