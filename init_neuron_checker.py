from segway.dahlia.db_server import NeuronDBServer
import sys
import re
import sqlite3


def init_neuron_checker(db_dir, db_name, host):
    conn = sqlite3.connect(db_dir)
    cursor = conn.cursor()
    nd = NeuronDBServer(
        db_name=db_name,
        host=host)
    # create table
    print('Creating table ....')
    # cursor.execute('DROP TABLE IF EXISTS neuron')
    sql = '''CREATE TABLE IF NOT EXISTS neuron (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
                name TEXT NOT NULL UNIQUE, 
                tested INTEGER,
                subpart INTEGER,
                lastupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                segments TEXT)'''
    cursor.execute(sql)
    conn.commit()

    # populate neurons
    print("retrieving ....")
    neurons = nd.find_neuron({})
    print("fomatting ....")
    neurons_to_commit = []
    for n in neurons:
        is_subpart = bool(re.search('axon|soma|dendrite|unknown_segment', n))
        n_to_commit = (n, 0, 1 if is_subpart else 0, None)
        neurons_to_commit.append(n_to_commit)
    print("committing ....")
    cursor.executemany(
        '''
        INSERT INTO neuron 
            (name, tested, subpart, segments) 
        VALUES 
            (?, ?, ?, ?)
        ''',
        neurons_to_commit
    )
    conn.commit()
    sql = 'SELECT COUNT(*) FROM neuron WHERE subpart = 0'
    cursor.execute(sql)
    row = cursor.fetchall()[0][0]
    print(f'Inserted {row} neuron(s)')
    sql = 'SELECT COUNT(*) FROM neuron WHERE subpart = 1'
    cursor.execute(sql)
    row = cursor.fetchall()[0][0]
    print(f'Inserted {row} subpart(s)')


if __name__ == "__main__":
    if len(sys.argv) == 4:
        db_dir = sys.argv[1]
        db_name = sys.argv[2]
        host = sys.argv[3]
    else:
        db_dir = '/n/groups/htem/Segmentation/xg76/combine_mesh/neuron_check/neuron_mesh_test.db'
        db_name = 'neurondb_cb2_v4'
        host = "mongodb://10.117.28.250:27018/"
    init_neuron_checker(db_dir, db_name, host)
