import sys
import os
import time
import requests
from view import app
from configparser import ConfigParser

# to add the module path
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from flask import Flask, render_template, redirect, url_for, request
from utils.CLI import CLI


# init the app for flask
# app = Flask(__name__,
#             template_folder=os.getcwd() + '/view/templates/',
#             static_folder=os.getcwd() + '/view/static/')

cli = None


def check_config():
    """
    check the config file to confirm whether need to redirect to config page
    :return: whether exists
    """
    return os.path.isfile(os.getcwd() + '/conf/db.ini')


def load_config():
    """
    load the config file and init CLI
    :return: new CLI config
    """
    parser = ConfigParser()
    parser.read(os.getcwd() + '/conf/db.ini')
    db = parser['current']['db']
    output_only = parser['export']['export_method']

    output = True if output_only == 'cypher' else False
    return CLI(output, db_name=db)


@app.route('/')
@app.route('/index')
def index():
    global cli
    if not check_config():
        return redirect(url_for('Config'))
    else:
        cli = load_config()
        cli.load_web_conf()
        links = []
        tables = cli.cb.get_tables()
        for table in tables:
            table["reflexive"] = 'false'
        table_names = [table['id'] for table in tables]
        relation = cli.cb.get_mysql_relations(only_table=True)
        print(relation)

        for name in table_names:
            for r in relation:
                temp = {}
                if name == r['TABLE_NAME']:
                    source_index = table_names.index(name)
                    target_index = table_names.index(r['REFERENCED_TABLE_NAME'])
                    temp['source'] = '{}'.format(source_index)
                    temp['target'] = '{}'.format(target_index)
                    temp['left'] = 'false'
                    temp['right'] = 'true'
                    temp['type'] = '{}_{}'.format(r['TABLE_NAME'], r['REFERENCED_TABLE_NAME'])
                    links.append(temp)
                    print(temp)
        return render_template('index.html', tables=tables, links=links, config=cli == None)


@app.route('/config', methods=["POST", "GET"])
def Config():
    global cli
    if request.method == "POST":
        parser = ConfigParser()
        parser.read(os.getcwd() + '/conf/db.ini')
        print(request.form.get('type'))
        parser.set(request.form.get('type'), 'host', request.form.get('host'))
        parser.set(request.form.get('type'), 'port', request.form.get('port'))
        # neo4j does not have database type
        if request.form.get('type') != "neo4j":
            parser.set(request.form.get('type'), 'database', request.form.get('database'))
        parser.set(request.form.get('type'), 'password', request.form.get('password'))

        try:
            parser.add_section('export')
        except Exception as err:
            # means the export section already exists
            pass
        parser.set('export', 'export_method', request.form.get('export_method'))
        output_only = True if 'output' in request.form else False
        parser.set('export', 'output', str(output_only))

        try:
            parser.add_section('current')
        except Exception as err:
            # mean the section already exists
            pass
        parser.set('current', 'db', request.form.get('type'))
        with open(os.getcwd() + '/conf/db.ini', "w+") as file:
            parser.write(file)

        cli = CLI(output_only, db_name=request.form.get('type'))
        # redirect into index
        return redirect(url_for('index'))
    return render_template('config.html')


@app.route('/sql2cypher')
def Code():
    return render_template('code.html')


@app.route('/convert', methods=["POST", "GET"])
def Convert():
    global cli
    if request.method == "POST":
        query = request.form.get('sql')
        if cli is None:
            cli = load_config()
        return {"cypher": "MATCH (person:person)-[r:PERSON_VISIT]->(place:place) return *, place LIMIT 20;"}


@app.route('/run-code', methods=["POST", "GET"])
def run_code():
    global cli
    if request.method == "POST":
        sql_type = request.form.get("type")
        query = request.form.get('query')
        if cli is None:
            cli = load_config()
        cli.load_web_conf()
        t1 = time.time()
        t2 = time.time()

        if sql_type == "sql":
            if cli.db_name == "mysql":
                try:
                    res = cli.cb.execute_mysql(query)
                    t2 = time.time()
                except Exception as err:
                    res = {"result": None}
            else:
                try:
                    res = cli.cb.execute_psql(query)
                    t2 = time.time()
                except Exception as err:
                    res = {"result": None}
        else:
            try:
                original = cli.cb.execute_cypher(query)
                t2 = time.time()

                headers = {
                    'Content-type': 'application/json',
                }

                data = '{"statements":[{"statement":"%s", ' \
                       '"parameters":{},"resultDataContents":["row","graph"]}]}' % query
                response = requests.post('http://{}:7474/db/data/transaction/commit'.
                                         format(cli.cb.neo4j_config['host']), headers=headers, data=data,
                                         auth=(cli.cb.neo4j_config['username'], cli.cb.neo4j_config['password']))
                # print(response)
                # table_data = []
                for d in original:
                    d[list(original[0].keys())[0]] = str(d[list(original[0].keys())[0]])
                # print(original)
                return {"data": response.json(), "table_data": original, "keys": list(original[0].keys()) if type(original) is list else None,
                        "cost": round(t2 - t1, 2), "original": original}

            except Exception as err:
                res = {"result": None}
        return {"data": res, "keys": list(res[0].keys()) if type(res) is list else None, "cost": round(t2 - t1, 2)}


@app.route('/graph-view', methods=["POST", "GET"])
def view():
    return render_template('view.html')


def main():
    app.run(debug=True)


# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0')
