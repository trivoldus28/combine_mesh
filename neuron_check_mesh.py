import sqlite3
import os
import pickle
import re
try: 
    from dahlia.db_server import NeuronDBServer
    from dahlia.connected_segment_server import ConnectedSegmentServer
except ModuleNotFoundError as e:
    print(e)
    print("You need to use sknize environment")
    print("try importing segway.dahalia")
    from segway.dahlia.db_server import NeuronDBServer
    from segway.dahlia.connected_segment_server import ConnectedSegmentServer


class NeuronChecker:
    def __init__(self, 
                 db_dir='/n/groups/htem/Segmentation/xg76/combine_mesh/neuron_check',
                 db_name='neuron_mesh.db', 
                 dahlia_db_name='neurondb_cb2_v4', 
                 dahlia_db_host='mongodb://10.117.28.250:27018/'):
        self.conn = sqlite3.connect(os.path.join(db_dir, db_name))
        self.cursor = self.conn.cursor()
        self.dahlia_db_name=dahlia_db_name
        self.dahlia_db_host=dahlia_db_host
        self.dahlia_db = None

    def init_dahlia(self):
        if self.dahlia_db is None:
            print(self.dahlia_db_name)
            self.dahlia_db = NeuronDBServer(db_name=self.dahlia_db_name, host=self.dahlia_db_host)
    
    def close_dahlia(self):
        if self.dahlia_db is not None:
            self.dahlia_db.close()

    def get_cursor(self):
        return self.cursor

    def init_db(self, drop=False, cell_type=None):
        self.init_dahlia()
        cell_type = ['interneuron',
                 'pc',
                 'grc',
                 'glia',

                 'stellate',
                 'basket',

                 'golgi',
                 'lugaro',
                 'ubc',
                 'globular',
                 'cc',

                 'myelin',

                 'mf',
                 'pf',
                 'cf']
        for t in cell_type:
            neurons = self.dahlia_db.find_neuron({"name_prefix": t})
            if len(neurons) > 0:
                if drop:
                    self.cursor.execute('DROP TABLE IF EXISTS {}'.format(t))
                neurons = list(filter(lambda x: not bool(re.match(re.compile('.*\.(soma|dendrite|axon).*'), x)), neurons))
                neurons = [(n, 0) for n in neurons]
                sql = '''CREATE TABLE IF NOT EXISTS {} (
                         id INTEGER PRIMARY KEY AUTOINCREMENT, 
                         name TEXT, 
                         tested INTEGER)'''.format(t)
                self.cursor.execute(sql)
                self.commit_to_db()
                self.cursor.executemany('''INSERT INTO {} (name, tested) VALUES (?, ?)'''.format(t), neurons)
                print("committing {}".format(t))
                self.commit_to_db()
                print('finish')


    def update_neuron(self, neuron_class, name, tested, commit=True):
        try:
            sql = '''UPDATE {} SET tested = ? WHERE name = ?'''.format(neuron_class)
            tested = 1 if tested else 0
            self.cursor.execute(sql, (tested, name))
            if commit:
                self.conn.commit()
            return True
        except Exception as e:
            print(e)
            return False

    def get_neuron(self, neuron_class, name):
        sql = 'SELECT tested, FROM {} WHERE name=?'.format(neuron_class)
        self.cursor.execute(sql, (name, ))
        row = self.cursor.fetchall()
        if len(row) == 0:
            return None
        else:
            return row[0]

    def get_all_neuron(self, neuron_class):
        sql = 'SELECT name, tested FROM {}'.format(neuron_class)
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        return row

    def check_tested(self, neuron_class, name):
        sql = 'SELECT tested from {} WHERE name = ?'.format(neuron_class)
        self.cursor.execute(sql, (name, ))
        row = self.cursor.fetchall()
        if len(row) == 0:
            return None
        else:
            return True if row[0][0] else False

    def commit_to_db(self):
        self.conn.commit()

    def get_untested_neurons(self, neuron_type):
        sql = 'SELECT name FROM {} WHERE tested=0'.format(neuron_type)
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        return [r[0] for r in row]
    
    def get_tested_neurons(self, neuron_type):
        sql = 'SELECT name FROM {} WHERE tested=1'.format(neuron_type)
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        return [r[0] for r in row]

    def get_exisiting_neuron_tables(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        row = self.cursor.fetchall()
        return [r[0] for r in row if r[0] != 'sqlite_sequence']

    def get_query(self, query):
        self.cursor.execute(query)
        row = self.cursor.fetchall()
        return row
    
    def update_query(self, query, data, commit=True):
        self.cursor.execute(query, data)
        if commit:
            self.commit_to_db()
    
    def update_many_query(self, query, data_list, commit=True):
        self.cursor.executemany(query, data_list)
        if commit:
            self.commit_to_db()


if __name__ == "__main__":
    nc = NeuronChecker()
    nc.init_db(drop=True)
