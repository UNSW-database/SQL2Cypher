import sys
import os
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
def SQLTOCYPHER():
    return render_template('code.html')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
