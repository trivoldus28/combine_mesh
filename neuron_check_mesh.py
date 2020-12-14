import sqlite3
import os
import pickle
import re
import json

try:
    from dahlia.db_server import NeuronDBServer
    from dahlia.connected_segment_server import ConnectedSegmentServer
except ModuleNotFoundError as e:
    print("try importing segway.dahalia")
    from segway.dahlia.db_server import NeuronDBServer
    from segway.dahlia.connected_segment_server import ConnectedSegmentServer


class NeuronChecker:
    def __init__(self,
                 db_dir='/n/groups/htem/Segmentation/xg76/combine_mesh/neuron_check/neuron_mesh.db',
                 dahlia_db_name='neurondb_cb2_v4',
                 dahlia_db_host='mongodb://10.117.28.250:27018/'):
        self.conn = sqlite3.connect(db_dir)
        self.cursor = self.conn.cursor()
        self.dahlia_db_name = dahlia_db_name
        self.dahlia_db_host = dahlia_db_host
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
    
    def get_subpart_mongo(self, nlist):
        self.init_dahlia()
        subpart_name = []
        for neuron_name in nlist:
            children = self.dahlia_db.get_neuron(neuron_name).children
            if children is not None and len(children) > 0:
                for child in children:
                    subpart_name.append(child)
        return subpart_name

    def all_neuron_mongo(self):
        self.init_dahlia()
        cell_type = ['interneuron', 'pc', 'grc', 'glia', 'stellate', 'basket', 'golgi',
                     'lugaro', 'ubc', 'globular', 'cc', 'myelin', 'mf', 'pf', 'cf']
        result = []
        # get parent
        for t in cell_type:
            neurons = self.dahlia_db.find_neuron({"name_prefix": t})
            result.extend(neurons)
        # get subparts
        result.extend(self.get_subpart_mongo(result))
        return list(set(result))

    def init_db(self, drop=False, cell_type=None):
        self.init_dahlia()
        cell_type = ['interneuron', 'pc', 'grc', 'glia', 'stellate', 'basket', 'golgi',
                     'lugaro', 'ubc', 'globular', 'cc', 'myelin', 'mf', 'pf', 'cf']
        if drop:
            self.cursor.execute('DROP TABLE IF EXISTS neuron')
        sql = '''CREATE TABLE IF NOT EXISTS neuron (
                    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
                    name TEXT NOT NULL UNIQUE, 
                    tested INTEGER,
                    subpart INTEGER,
                    lastupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    segments TEXT)'''
        self.cursor.execute(sql)
        self.commit_to_db()
        for t in cell_type:
            neurons = self.dahlia_db.find_neuron({"name_prefix": t})
            print(f'neuron number of {t}: {len(neurons)}')
            neurons = filter(lambda n: not bool(re.search('axon|soma|dendrite', n)), neurons)
            neurons = [(n, 0, 0, None) for n in neurons]
            self.cursor.executemany('INSERT INTO neuron (name, tested, subpart, segments) VALUES (?, ?, ?, ?)', neurons)
            print("committing {} ...".format(t))
            self.commit_to_db()

    def update_neuron(self, nid, tested, segments, commit=True):
        try:
            sql = f'UPDATE neuron SET tested = ?, segments = ?, lastupdate=CURRENT_TIMESTAMP WHERE name = ?'
            tested = 1 if tested else 0
            segments = json.dumps(list(segments))
            self.cursor.execute(sql, (tested, segments, nid))
            if commit:
                self.conn.commit()
            return True
        except Exception as e:
            print(e)
            return False

    def get_neuron(self, nid):
        sql = 'SELECT tested, segments, lastupdate FROM neuron WHERE name=?'
        self.cursor.execute(sql, (nid,))
        row = self.cursor.fetchall()
        if len(row) == 0:
            return None
        else:
            return row[0]

    def get_all_neuron(self, subpart=None):
        if subpart is None:
            sql = 'SELECT name, tested, segments, lastupdate FROM neuron'
        elif subpart:
            sql = 'SELECT name, tested, segments, lastupdate FROM neuron WHERE subpart=1'
        else:
            sql = 'SELECT name, tested, segments, lastupdate FROM neuron WHERE subpart=0'
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        return row
    
    def get_all_neuron_name(self, subpart=None):
        if subpart is None:
            sql = 'SELECT name FROM neuron'
        elif subpart:
            sql = 'SELECT name FROM neuron WHERE subpart=1'
        else:
            sql = 'SELECT name FROM neuron WHERE subpart=0'
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return [row[0] for row in result]

    def check_tested(self, nid):
        sql = 'SELECT tested from neuron WHERE name = ?'
        self.cursor.execute(sql, (nid,))
        row = self.cursor.fetchall()
        if len(row) == 0:
            return None
        else:
            return True if row[0][0] else False

    def commit_to_db(self):
        self.conn.commit()

    def get_untested_neurons(self, subpart=None):
        if subpart is None:
            sql = 'SELECT name FROM neuron WHERE tested=0'
        elif subpart:
            sql = 'SELECT name FROM neuron WHERE tested=0 AND subpart=1'
        else:
            sql = 'SELECT name FROM neuron WHERE tested=0 AND subpart=0'
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        return [r[0] for r in row]

    def get_tested_neurons(self, subpart=None):
        if subpart is None:
            sql = 'SELECT name FROM neuron WHERE tested=1'
        elif subpart:
            sql = 'SELECT name FROM neuron WHERE tested=1 AND subpart=1'
        else:
            sql = 'SELECT name FROM neuron WHERE tested=1 AND subpart=0'
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        return [r[0] for r in row]

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

def populate_subpart(nc=None):
    if nc == None:
        nc = NeuronChecker()
    nc.init_dahlia()
    all_neuron_name = nc.get_query("SELECT name FROM neuron WHERE subpart = 0")
    all_neuron_name = [row[0] for row in all_neuron_name]
    subpart_name = nc.get_subpart_mongo(all_neuron_name)
    subpart_neuron = [(n, 0, 1, None) for n in subpart_name]
    print(f'populating subparts, total #: {len(subpart_neuron)}')
    nc.cursor.executemany('INSERT INTO neuron (name, tested, subpart, segments) VALUES (?, ?, ?, ?)', subpart_neuron)
    print('committing ...')
    nc.commit_to_db()


if __name__ == "__main__":
    nc = NeuronChecker()
    nc.init_db(drop=True)
    populate_subpart(nc)
