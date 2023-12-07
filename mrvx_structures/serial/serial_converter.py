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
        RENAME TO {table}_orig
    ''')

    # create main table
    #cursor.execute(f'''
    #    CREATE TABLE {table}_orig (
    #        {columns_str(data['not_mrv'], with_types=True)},
    #        PRIMARY KEY({columns_str(data['pk'])})
    #    )''')

    # copy data to main table
    #cursor.execute(f'''
    #    INSERT INTO {table}_orig (
    #        SELECT {columns_str(data['not_mrv'])}
    #        FROM {table}__aux
    #    )''')

    # recreate indexes
    #cursor.execute(f'''
    #    SELECT indexdef
    #    FROM pg_indexes
    #    WHERE schemaname = 'public' AND tablename = '{table}__aux'
    #''')
    #for index, in cursor.fetchall():
    #    index = re.sub(f"{table}", f"{table}_orig", index)
    #    index = re.sub(f"{table}_orig__aux", f"{table}_orig", index)
    #    index = re.sub(r'CREATE\s*(UNIQUE)?\s*INDEX', r'CREATE \1 INDEX IF NOT EXISTS', index)
    #    cursor.execute(index)


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
                valid boolean,
                PRIMARY KEY ({columns_str(data['pk'])}, rk)
            )''')
        # move data
        cursor.execute(f"SELECT {columns_str(data['pk'])}, {mrv.name} FROM {table}_orig")
        inserts_rows = []
        initial_nodes = model['initialNodes']
        for row in cursor:
            value = row[-1]
            pk = row[:-1]
            
            rks = [x for x in range(model['maxNodes'])]
            for i in range(initial_nodes - 1):
                rk = rks[random.randrange(len(rks))]
                rks.remove(rk)
                insert_row = pk + (rk,) + (value + i, True)
                inserts_rows.append(insert_row)
        execute_values(cursor, f"INSERT INTO {table}_{mrv.name} VALUES %s", inserts_rows)

    # remove aux table
    
    cursor.execute(f'''
        ALTER TABLE {table}_orig
        DROP COLUMN {columns_str(data['mrv'])};
    ''')

    counter_value = f'''(SELECT {','.join([f'{pk.name}' for pk in data['pk']])}, MIN({mrv.name}) as {mrv.name} 
        FROM {table}_{mrv.name} WHERE valid = true
        GROUP BY {','.join([f'{pk.name}' for pk in data['pk']])})
    '''

    cursor.execute(f'''
        CREATE VIEW {table} AS
        SELECT {table}_orig.*, T.{mrv.name} FROM {table}_orig JOIN 
        ''' + counter_value + f''' T
        ON 
        {' AND '.join([f'{table}_orig.{pk.name} = T.{pk.name}' for pk in data['pk']])}
    ''')
    
    


    for mrv in data['mrv']:
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION {table}_{mrv.name}({columns_str(data['pk'], name_suffix='_', with_types=True)}) 
            RETURNS TABLE (
                {','.join([f'{x.name} {x.type}' for x in data['pk']])}, {','.join([f'{x.name} {x.type}' for x in data['mrv']])}
            )
            AS $$ 
            DECLARE rk_ int = FLOOR(RANDOM() * ({model['maxNodes']} + 1))::integer;
                    node_rk int;
                    cur CURSOR FOR 
                        (SELECT rk
                        FROM {table}_{mrv.name} AS T
                        WHERE {' AND '.join([f'T.{pk.name} = {pk.name}_' for pk in data['pk']])} AND rk >= rk_ AND valid = true
                        ORDER BY rk) 
                        UNION ALL 
                        (SELECT rk
                        FROM {table}_{mrv.name} AS T
                        WHERE {' AND '.join([f'T.{pk.name} = {pk.name}_' for pk in data['pk']])} AND rk < rk_ AND valid = true 
                        ORDER BY rk); 
            BEGIN 
                OPEN cur; 
                FETCH cur INTO node_rk; 
                IF NOT FOUND THEN 
                    RETURN;
                ELSE
                    UPDATE {table}_{mrv.name} AS T
                    SET valid = FALSE
                    WHERE {' AND '.join([f'T.{pk.name} = {pk.name}_' for pk in data['pk']])} AND rk = node_rk;
                    RETURN QUERY SELECT {','.join([f'T.{x.name}' for x in data['pk']])}, {','.join([f'T.{x.name}' for x in data['mrv']])} FROM {table}_{mrv.name} AS T WHERE {' AND '.join([f'T.{pk.name} = {pk.name}_' for pk in data['pk']])} AND rk = node_rk;      
                END IF; 
                CLOSE cur;
            END 
            $$ LANGUAGE plpgsql;
            ''')

        # create insert procedure
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION insert_{table}({columns_str(data['not_mrv'], with_types=True, name_suffix='_new')}) RETURNS VOID
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
                        0, True, False);
            ''' for mrv in data['mrv']) 
            +
            '''
            END
            $$ LANGUAGE plpgsql;
        ''')

        # create update procedure
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION update_{table}(
                {columns_str(data['not_mrv'], with_types=True, name_suffix='_new')},
                {columns_str(data['not_mrv'], with_types=True, name_suffix='_old')}) RETURNS void
            AS $$
            BEGIN
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
            DO INSTEAD SELECT insert_{table}({columns_str(data['not_mrv'], name_prefix='NEW.', with_cast=True)})
        ''')

        # create update rule
        cursor.execute(f'''
            CREATE OR REPLACE RULE "update_{table}_rule" AS
            ON UPDATE TO {table}
            DO INSTEAD SELECT update_{table}(
                {columns_str(data['not_mrv'], name_prefix='NEW.', with_cast=True)},
                {columns_str(data['not_mrv'], name_prefix='OLD.', with_cast=True)})
        ''')

        # create delete rule
        cursor.execute(f'''
            CREATE OR REPLACE RULE "delete_{table}_rule" AS
            ON DELETE TO {table}
            DO INSTEAD SELECT delete_{table}({columns_str(data['pk'], name_prefix='OLD.', with_cast=True)})
        ''')

        # worker update function
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION refresh_{table}_{mrv.name}({columns_str(data['pk'], with_types=True, name_suffix='_')}) RETURNS VOID
            AS $$
            DECLARE max_counter int;
                    node_rk int;
                    done bool = FALSE;
                    cur CURSOR FOR
                        SELECT rk FROM {table}_{mrv.name}
                        WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])} AND valid = FALSE;

            BEGIN
                SELECT MAX({mrv.name}) + 1 INTO max_counter 
                FROM {table}_{mrv.name}
                WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])};

                OPEN cur;
                WHILE NOT done LOOP
                    FETCH cur INTO node_rk;
                    IF NOT FOUND THEN 
                        done = TRUE;
                    ELSE
                        UPDATE {table}_{mrv.name}
                        SET {mrv.name} = max_counter, valid = TRUE
                        WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])} AND rk = node_rk;
                        
                        max_counter := max_counter + 1;
                    END IF;
                
                END LOOP;

                CLOSE cur;
            END
            $$ LANGUAGE plpgsql;
        ''')

        # worker get pks view
        cursor.execute(f'''
            CREATE VIEW {table}_{mrv.name}_pk AS
                SELECT {columns_str(data['pk'])} FROM {table}_orig;
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

conn.commit()
conn.close()

print('Done')
