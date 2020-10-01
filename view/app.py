import os
from flask import Flask, render_template

# init the app for flask
app = Flask(__name__,
            template_folder=os.getcwd() + '/templates/',
            static_folder=os.getcwd() + '/static/')


@app.route('/')
def hello_world():
    return render_template('graph.html')


if __name__ == '__main__':
    app.run(debug=True)
