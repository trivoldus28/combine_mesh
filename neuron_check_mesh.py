import sqlite3
import json


class NeuronChecker:
    def __init__(self,
                 db_dir='/n/groups/htem/Segmentation/xg76/combine_mesh/neuron_check/neuron_mesh.db'):
        self.conn = sqlite3.connect(db_dir)
        self.cursor = self.conn.cursor()

    def get_cursor(self):
        return self.cursor
        
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
    nc.cursor.executemany(
        'INSERT INTO neuron (name, tested, subpart, segments) VALUES (?, ?, ?, ?)', subpart_neuron)
    print('committing ...')
    nc.commit_to_db()


if __name__ == "__main__":
    nc = NeuronChecker()
    nc.init_db(drop=True)
    populate_subpart(nc)
