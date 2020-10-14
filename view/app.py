import sys
import os

# to add the module path
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from flask import Flask, render_template
from utils.CLI import CLI


cli = CLI()
# init the app for flask
app = Flask(__name__,
            template_folder=os.getcwd() + '/view/templates/',
            static_folder=os.getcwd() + '/view/static/')


@app.route('/')
@app.route('/index')
def index():
    links = []
    tables = cli.cb.get_tables()
    for table in tables:
        table["reflexive"] = 'false'
    table_names = [table['id'] for table in tables]
    relation = cli.cb.get_mysql_relations(only_table=True)

    for name in table_names:
        temp = {}
        for r in relation:
            if name == r['TABLE_NAME']:
                source_index = table_names.index(name)
                target_index = table_names.index(r['REFERENCED_TABLE_NAME'])
                temp['source'] = '{}'.format(source_index)
                temp['target'] = '{}'.format(target_index)
                temp['left'] = 'false'
                temp['right'] = 'true'
                temp['type'] = '{}_{}'.format(r['TABLE_NAME'], r['REFERENCED_TABLE_NAME'])
                links.append(temp)
    return render_template('index.html', tables=tables, links=links)


@app.route('/sql2cypher')
def SQLTOCYPHER():
    return render_template('code.html')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
