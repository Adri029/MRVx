# Converts the columns provided in the model file into multi record values (PostgreSQL only)
# Usage: python3 convert_model.py <model-yml> [<initial-nodes>]

import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import yaml
import sys
from collections import defaultdict
import random
import re


type_translation = {
    'character': 'varchar',
    'smallint': 'integer'
}

# column in a relation
class Column:
    def __init__(self, name, type, nullable):
        self.name = name
        self.type = type_translation.get(type, type)
        self.nullable = True if nullable == 'YES' else False

    def __repr__(self):
        return f'name: {self.name}, type: {self.type}, nullable: {self.nullable}'


def columns_str(data, with_types=False, join=', ', name_suffix='', name_prefix='', with_cast=False):
    if not with_cast:
        return join.join([name_prefix + x.name + name_suffix + (' ' + x.type if with_types else '')
                        for x in data])
    else:
        return join.join([name_prefix + x.name + name_suffix + '::' + x.type for x in data])


if len(sys.argv) < 2:
    exit('Usage: python3 convert_model.py <model-yml> [<initial-nodes>]')


model_file = sys.argv[1]
with open(model_file) as f:
    model = yaml.load(f, Loader=yaml.FullLoader)

if len(sys.argv) >= 3:
    model['initialNodes'] = min(int(sys.argv[2]), model['maxNodes'])

conn = psycopg2.connect(dbname=model['database'], host=model['host'], port=model['port'],
                        user=model['user'], password=model['password'])
cursor = conn.cursor()
cursor.execute(f"SET search_path TO {model['schema']}")


for table_data in model['tables']:
    data = {}
    table = table_data['name']
    print(f"Processing table '{table}'")
    mvn_names = set(table_data['mrv'])
    
    # all columns
    cursor.execute(f'''
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{model['schema']}'
        AND table_name = '{table}';
    ''')
    all_columns = [Column(x[0], x[1], x[2]) for x in cursor.fetchall()]


    # primary keys
    cursor.execute(f'''
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '{table}'::regclass
        AND    i.indisprimary;
    ''')
    primary_keys_names = set([x[0] for x in cursor.fetchall()])

    # store primary, regular and mrv columns for future uses
    data['pk'] = [x for x in all_columns if x.name in primary_keys_names]
    data['regular'] = [x for x in all_columns 
                              if x.name not in primary_keys_names 
                              and x.name not in mvn_names]
    data['mrv'] = [x for x in all_columns if x.name in mvn_names]
    data['not_mrv'] = [x for x in all_columns if x.name not in mvn_names]
    data['all'] = all_columns

    # rename table
    cursor.execute(f'''
        ALTER TABLE {table}
        RENAME TO {table}__aux
    ''')

    # create main table
    cursor.execute(f'''
        CREATE TABLE {table}_orig (
            {columns_str(data['not_mrv'], with_types=True)},
            PRIMARY KEY({columns_str(data['pk'])})
        )''')

    # copy data to main table
    cursor.execute(f'''
        INSERT INTO {table}_orig (
            SELECT {columns_str(data['not_mrv'])}
            FROM {table}__aux
        )''')

    # recreate indexes
    cursor.execute(f'''
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = '{table}__aux'
    ''')
    for index, in cursor.fetchall():
        index = re.sub(f"{table}", f"{table}_orig", index)
        index = re.sub(f"{table}_orig__aux", f"{table}_orig", index)
        index = re.sub(r'CREATE\s*(UNIQUE)?\s*INDEX', r'CREATE \1 INDEX IF NOT EXISTS', index)
        cursor.execute(index)


#MAX STRUCTURE
#rk, id, value pk(rk,id,value)

    # create mrv tables
    for mrv in data['mrv']:
        # create table
        cursor.execute(f'''
            CREATE TABLE {table}_{mrv.name} (
                {columns_str(data['pk'], with_types=True)},
                rk int,
                {mrv.name} {mrv.type},
                PRIMARY KEY ({columns_str(data['pk'])}, rk)
            )''')
        # move data
        cursor.execute(f"SELECT {columns_str(data['pk'])}, {mrv.name} FROM {table}__aux")
        inserts_rows = []
        initial_nodes = model['initialNodes']
        for row in cursor:
            value = row[-1]
            pk = row[:-1]
            min_inf = - 2147483648
            
                
            rks = [x for x in range(model['maxNodes'])]
            for _ in range(initial_nodes - 1):
                rk = rks[random.randrange(len(rks))]
                rks.remove(rk)
                insert_row = pk + (rk,) + (min_inf,)
                inserts_rows.append(insert_row)
            #leftover
            rk = rks[random.randrange(len(rks))]
            insert_row = pk + (rk,) + (value,)
            inserts_rows.append(insert_row)
        execute_values(cursor, f"INSERT INTO {table}_{mrv.name} VALUES %s", inserts_rows)

    # remove aux table
    cursor.execute(f'DROP TABLE {table}__aux')

    # create view
    selects = []
    for mrv in data['mrv']:
        s = f'(SELECT MAX({mrv.name}) AS {mrv.name} FROM {table}_{mrv.name} WHERE '
        wheres = [f'{table}_{mrv.name}.{pk.name} = {table}_orig.{pk.name}' for pk in data['pk']]
        s += ' AND '.join(wheres) + ')'
        selects.append(s)

    cursor.execute(f'''
        CREATE VIEW {table} AS
        SELECT {table}_orig.*, {','.join(selects)}
        FROM {table}_orig
    ''')


    
    for mrv in data['mrv']:
        #Write MAX
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION max_{table}_{mrv.name}({columns_str(data['pk'], name_suffix='_', with_types=True)}, rk_ int, {mrv.name}_ {mrv.type}) RETURNS void 
            AS $$ 
            DECLARE rk_v integer;
            BEGIN
                SELECT rk INTO rk_v FROM( 
                    (SELECT rk 
                        FROM {table}_{mrv.name} 
                        WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])} AND rk >= rk_ 
                        ORDER BY rk 
                        LIMIT 1) 
                        UNION ALL 
                        (SELECT MIN(rk) 
                        FROM {table}_{mrv.name} 
                        WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])}) 
                    ) AS T
                LIMIT 1; 

                UPDATE {table}_{mrv.name} 
                SET {mrv.name} = {mrv.name}_ 
                WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])} AND rk = rk_v
                            AND {mrv.name}_ > {mrv.name};

            END
            $$ LANGUAGE plpgsql;
            ''')

        # create insert procedure
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION insert_{table}({columns_str(data['all'], with_types=True, name_suffix='_new')}) RETURNS VOID
            AS $$
            BEGIN
                INSERT INTO {table}_orig 
                VALUES ({columns_str(data['not_mrv'], name_suffix='_new')});
            '''
            +
            '\n'.join(f'''
                INSERT INTO {table}_{mrv.name}
                VALUES ({columns_str(data['pk'], name_suffix='_new')}, 
                        FLOOR(RANDOM() * ({model['maxNodes']} + 1))::integer,
                        {mrv.name}_new);
            ''' for mrv in data['mrv']) 
            +
            '\n'.join(f'''
                INSERT INTO {table}_{mrv.name}
                VALUES ({columns_str(data['pk'], name_suffix='_new')}, 
                        FLOOR(RANDOM() * ({model['maxNodes']} + 1))::integer,
                        0);
            ''' for mrv in data['mrv'] for _ in range(initial_nodes - 1)) 
            #TODO apagar os registos extra a 0, são bons para testes           
            +
            '''
            END
            $$ LANGUAGE plpgsql;
        ''')

        # create update procedure
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION update_{table}(
                {columns_str(data['all'], with_types=True, name_suffix='_new')},
                {columns_str(data['all'], with_types=True, name_suffix='_old')}) RETURNS int
            AS $$
            DECLARE d int;
            BEGIN
            '''
            # update mrv values
            +f'''\n
                PERFORM max_{table}_{mrv.name}({columns_str(data['pk'], name_suffix='_new')}, FLOOR(RANDOM() * ({model['maxNodes']} + 1))::integer, {mrv.name}_new);
                GET diagnostics d = row_count;
            '''
            # update remaining values
            + '\n'.join([f'''
                IF {regular.name}_new <> {regular.name}_old THEN
                    UPDATE {table}_orig
                    SET {regular.name} = {regular.name}_new
                    WHERE {' AND '.join([f'{pk.name} = {pk.name}_new' for pk in data['pk']])};
                END IF;
            ''' for regular in data['regular']])
            +
            '''
            RETURN d;
            END
            $$ LANGUAGE plpgsql;
        ''')

        # create delete procedure
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION delete_{table}({columns_str(data['pk'], with_types=True, name_suffix='_old')}) RETURNS void
            AS $$
            BEGIN
                DELETE FROM {table}_orig
                WHERE {' AND '.join([f'{pk.name} = {pk.name}_old' for pk in data['pk']])};
            '''
            + '\n'.join([f'''
                DELETE FROM {table}_{mrv.name}
                WHERE {' AND '.join([f'{pk.name} = {pk.name}_old' for pk in data['pk']])};
            ''' for mrv in data['mrv']])
            +
            '''
            END
            $$ LANGUAGE plpgsql;
        ''')


        # create insert rule
        cursor.execute(f'''
            CREATE OR REPLACE RULE "insert_{table}_rule" AS
            ON INSERT TO {table}
            DO INSTEAD SELECT insert_{table}({columns_str(data['all'], name_prefix='NEW.', with_cast=True)})
        ''')


        # create update rule
        cursor.execute(f'''
            CREATE OR REPLACE RULE "update_{table}_rule" AS
            ON UPDATE TO {table}
            DO INSTEAD SELECT update_{table}(
                {columns_str(data['all'], name_prefix='NEW.', with_cast=True)},
                {columns_str(data['all'], name_prefix='OLD.', with_cast=True)})
        ''')

        # create delete rule
        cursor.execute(f'''
            CREATE OR REPLACE RULE "delete_{table}_rule" AS
            ON DELETE TO {table}
            DO INSTEAD SELECT delete_{table}({columns_str(data['pk'], name_prefix='OLD.', with_cast=True)})
        ''')

    # create mrv size function
    cursor.execute('''
        CREATE OR REPLACE FUNCTION mrv_size(tablename varchar, columnname varchar, pk varchar) RETURNS int
        LANGUAGE plpgsql
        AS $$
        DECLARE ret int;
        BEGIN
            EXECUTE 'SELECT count(*) FROM ' || tablename || '_' || columnname || ' WHERE ' || pk INTO ret;
            RETURN ret;
        END
        $$;
    ''')
    
    # create mrv total function
    cursor.execute('''
        CREATE OR REPLACE FUNCTION mrv_total(tablename varchar, columnname varchar, pk varchar) RETURNS numeric
        LANGUAGE plpgsql
        AS $$
        DECLARE ret numeric;
        BEGIN
            EXECUTE 'SELECT sum(' || columnname || ') FROM ' || tablename || '_' || columnname || ' WHERE ' || pk INTO ret;
            RETURN ret;
        END
        $$;
    ''')

conn.commit()
conn.close()

print('Done')