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


k = 5
for table_data in model['tables']:
    data = {}
    table = table_data['name']
    print(f"Processing table '{table}'")
    mvn_names = set(table_data['mrv'])
    payload_names = set(table_data['payload'])
    if k > 1:
        order = set(table_data['order'])
    else:
        order = []
    
    # all columns
    cursor.execute(f'''
        SELECT column_name, udt_name, is_nullable
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
    data['pk'] = [x for x in all_columns if x.name in primary_keys_names and x.name not in order]
    data['regular'] = [x for x in all_columns 
                              if x.name not in primary_keys_names 
                              and x.name not in mvn_names and x.name not in payload_names]
    data['mrv'] = [x for x in all_columns if x.name in mvn_names]
    data['payload'] = [x for x in all_columns if x.name in payload_names]
    if k > 1 :
        data['order'] = [x for x in all_columns if x.name in order]
    data['not_mrv'] = [x for x in all_columns if x.name not in mvn_names and x.name not in payload_names and x.name not in order]
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
            SELECT DISTINCT {columns_str(data['not_mrv'])}
            FROM {table}__aux
        )''')

    # recreate indexes
    cursor.execute(f'''
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = '{table}__aux'
    ''')

    if k > 1:
        orders = [f'{order.name}' for order in data['order']]
    for index, in cursor.fetchall():
        index = re.sub(f"{table}", f"{table}_orig", index)
        index = re.sub(f"{table}_orig__aux", f"{table}_orig", index)
        if k > 1:
            index = re.sub(f"{', '.join(orders)}", "", index)
        index = re.sub(r'CREATE\s*(UNIQUE)?\s*INDEX', r'CREATE \1 INDEX IF NOT EXISTS', index)
        cursor.execute(index)


#MAX STRUCTURE
#rk, id, value pk(rk,id,value)

    # create mrv tables
    num_payload = len(data['payload'])
    payload_types = [f'{payload.name} {payload.type}' for payload in data['payload']]
    payloads = [f'{payload.name}' for payload in data['payload']]
    for mrv in data['mrv']:
        # create table
        cursor.execute(f'''
            CREATE TABLE {table}_{mrv.name} (
                {columns_str(data['pk'], with_types=True)},
                rk int,
                {', '.join(payload_types)}, {mrv.name} {mrv.type},
                PRIMARY KEY ({columns_str(data['pk'])}, rk)
            )''')
        # move data
        cursor.execute(f"SELECT {columns_str(data['pk'])}, {', '.join(payloads)}, {mrv.name} FROM {table}__aux")
        inserts_rows = []
        initial_nodes = model['initialNodes']
        seen_pk = dict()
        for row in cursor:
            value = row[-1]
            payloads = row[-num_payload-1:-1]
            pk = row[:-num_payload-1]
            if pk not in seen_pk:
                seen_pk[pk] = [x for x in range(model['maxNodes'])]

            rk = seen_pk[pk][random.randrange(len(seen_pk[pk]))]
            seen_pk[pk].remove(rk)
            insert_row = pk + (rk,) + payloads + (value,)
            inserts_rows.append(insert_row)
        
        regs = max(initial_nodes,k)
        payloads = ()
        for _ in payload_names:
            payloads += (None, ) 
        for key, value in seen_pk.items():
            size = len(value)
            while model['maxNodes'] - size < regs:
                rk = value[random.randrange(size)]
                value.remove(rk)
                size -= 1
                insert_row = key + (rk,) + payloads + (0,)
                inserts_rows.append(insert_row)
        execute_values(cursor, f"INSERT INTO {table}_{mrv.name} VALUES %s", inserts_rows)


    # remove aux table
    cursor.execute(f'DROP TABLE {table}__aux')
    
    ids_str = columns_str(data['pk'], with_types=False)
    payloads_str = columns_str(data['payload'], with_types=False)
    ids_order = columns_str(data['pk'], with_types=False, join=' ASC, ')
    if k > 1:
        orders_str = columns_str(data['order'], with_types=False)
        cursor.execute(f'''
            CREATE VIEW {table} AS
            SELECT {ids_str}, {orders_str}, {mrv.name}, {payloads_str} FROM 
            (
                SELECT {', '.join([f'{table}_orig.{pk.name}' for pk in data['pk']])}, {mrv.name}, {payloads_str}, 
                       ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{table}_orig.{pk.name}' for pk in data['pk']])} ORDER BY {mrv.name} DESC) {orders_str}
                       FROM {table}_{mrv.name} INNER JOIN {table}_orig 
                       ON {' AND '.join([f'{table}_orig.{pk.name} = {table}_{mrv.name}.{pk.name}' for pk in data['pk']])}
            ) A
            WHERE {orders_str} <= {k}
            ORDER BY {ids_order} ASC, {orders_str} ASC;
        ''')
    else:
        cursor.execute(f'''
            CREATE VIEW {table} AS
            SELECT A.*, {payloads_str} 
            FROM (
                SELECT T.*, MAX({mrv.name}) {mrv.name}
                FROM {table}_{mrv.name} INNER JOIN {table}_orig AS T
                ON {' AND '.join([f'T.{pk.name} = {table}_{mrv.name}.{pk.name}' for pk in data['pk']])}
                GROUP BY {', '.join([f'T.{pk.name}' for pk in data['pk']])}
            ) A INNER JOIN {table}_{mrv.name} B 
            ON {' AND '.join([f'A.{pk.name} = B.{pk.name}' for pk in data['pk']])} AND A.{mrv.name} = B.{mrv.name};
        ''')

    
    for mrv in data['mrv']:
        #Write MAX
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION topk_insert_{table}_{mrv.name}({columns_str(data['pk'], name_suffix='_', with_types=True)}, rk_ int, {', '.join([f'{payload.name}_ {payload.type}' for payload in data['payload']])}, {mrv.name}_ {mrv.type}) RETURNS void
            AS $$ 
            DECLARE count int;
                    ins_rk int;
                    cur_rk int;
                    cur_{mrv.name} int;
                    done bool = False;
                    cursor_rk CURSOR FOR 
                        (SELECT rk, {mrv.name} 
                        FROM {table}_{mrv.name} 
                        WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])} AND rk >= rk_ 
                        ORDER BY rk) 
                        UNION ALL 
                        (SELECT rk, {mrv.name} 
                        FROM {table}_{mrv.name} 
                        WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])} AND rk < rk_ 
                        ORDER BY rk);

            BEGIN 
                count := 0;
                
                OPEN cursor_rk;
                WHILE NOT done AND count < {k}
                LOOP
                    FETCH cursor_rk INTO cur_rk, cur_{mrv.name}; 
                    IF NOT FOUND THEN 
                        done := TRUE; 
                    ELSEIF cur_{mrv.name} >= {mrv.name}_ THEN
                        count := count + 1;
                    END IF;
                END LOOP;
                CLOSE cursor_rk; 

                IF count < {k} THEN
                    SELECT rk INTO ins_rk FROM (
                        SELECT rk, {mrv.name} FROM {table}_{mrv.name} 
                        WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])} 
                        ORDER BY {mrv.name} DESC 
                        OFFSET {k-1} ROWS) AS T
                    ORDER BY RANDOM()
                    LIMIT 1;
                
                    UPDATE {table}_{mrv.name} 
                    SET {mrv.name} = {mrv.name}_, {', '.join([f'{payload.name} = {payload.name}_' for payload in data['payload']])}
                    WHERE {' AND '.join([f'{pk.name} = {pk.name}_' for pk in data['pk']])}
                                AND rk = ins_rk;
                    RAISE NOTICE 'HELLO';
                END IF;
            END       
            $$ LANGUAGE plpgsql;
            ''')

        # create insert procedure
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION insert_{table}({columns_str(data['all'], with_types=True, name_suffix='_new')}) RETURNS VOID
            AS $$
            BEGIN
                IF (NOT EXISTS (SELECT {ids_str} FROM {table}_orig WHERE {' AND '.join([f'{pk.name} = {pk.name}_new' for pk in data['pk']])})) OR {k} = 1
                THEN
                    INSERT INTO {table}_orig 
                    VALUES ({columns_str(data['not_mrv'], name_suffix='_new')});
                END IF;
            '''
            +
            '\n'.join(f'''
                INSERT INTO {table}_{mrv.name} VALUES({columns_str(data['pk'], name_suffix='_new')}, FLOOR(RANDOM() * ({model['maxNodes']} + 1))::integer, {', '.join([f'{payload.name}_new' for payload in data['payload']])}, {mrv.name}_new);
            ''' for mrv in data['mrv'])       
            +
            '\n'.join(f'''
                INSERT INTO {table}_{mrv.name} VALUES({columns_str(data['pk'], name_suffix='_new')}, FLOOR(RANDOM() * ({model['maxNodes']} + 1))::integer, {', '.join([f'{payload.name}_new' for payload in data['payload']])}, 0);
            ''' for i in range(1,max(k, initial_nodes)) for mrv in data['mrv'])       
            +
            '''
            END
            $$ LANGUAGE plpgsql;
        ''')

        # create update procedure
        cursor.execute(f'''
            CREATE OR REPLACE FUNCTION update_{table}(
                {columns_str(data['all'], with_types=True, name_suffix='_new')},
                {columns_str(data['all'], with_types=True, name_suffix='_old')}) RETURNS void
            AS $$
            BEGIN
            '''
            # update mrv values
            +f'''
                PERFORM topk_insert_{table}_{mrv.name}({columns_str(data['pk'], name_suffix='_new')}, FLOOR(RANDOM() * ({model['maxNodes']} + 1))::integer, {', '.join([f'{payload.name}_new' for payload in data['payload']])}, {mrv.name}_new);
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
