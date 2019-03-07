import itertools
import psycopg2
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
import numpy

def _addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def _addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

def _connect_database(db_name='postgres', username=None, password=None, host=None, port=None):
    try:
        connectionString = "dbname='" + db_name + "'"
        if username != None and username != '':
            connectionString += " user='" + username + "'"
        if host != None and host != '':
            connectionString += " host='" + host + "'"
        if password != None and password != '':
            connectionString += " password='" + password + "'"
        if port != None:
            connectionString += " port='" + str(port) + "'"

        connection  = psycopg2.connect(connectionString)
        register_adapter(numpy.float64, _addapt_numpy_float64)
        register_adapter(numpy.int64, _addapt_numpy_int64)
    except:
        raise

    return connection

def _create_cursor(connection):
    return connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def insert_features(id, features, model='unknown'):
    query="INSERT INTO features VALUES (%s, %s, %s)"

    tensor = []
    for a in features:
        tensor.append(a.tolist())

    cursor2.execute(query, (id, tensor, model))
    connection2.commit()

def insert_many_features(ids, features, model='unknown'):

    tensors=[]
    for feature in features :
        tensor = []
        for a in feature:
            tensor.append(a.tolist())
        tensors.append(tensor)


    tup = zip(ids, tensors, itertools.repeat(model)) 
    args_str = b','.join([cursor2.mogrify("(%s,%s,%s)", u) for u in tup])
    cursor2.execute("INSERT INTO features (product_id, tensor, model) VALUES " + args_str.decode('utf8')) 
    connection2.commit()

def test():
    tup = [('ab',[1,2,3], 'lol'),('ac',[1,2,43],'lol'),('ad',[1,2,345],'lol')]
    args_str = b','.join(cursor.mogrify("(%s,%s,%s)", (x,y,z)) for x,y,z in tup)
    cursor.execute("INSERT INTO features (product_id, tensor, model) VALUES " + args_str.decode('utf8')) 


def get_features(product_id=None, model='resnet50'):
    if type(id) == 'list' :
        cursor2.execute("select * from features where id in (%s) and model = %s", (str(id)[1:-1], model))
        tensors = cursor2.fetchall()
        return tensors
    elif product_id != none: 
        cursor2.execute("select * from features where id = %s and model = %s", (id, model))
        res=cursor2.fetchall()[0]
        return res
    else: 
        cursor.execute("select * from features where model = %s",(model,))
        tensors = cursor2.fetchall()
        return tensors

def get_features_offset(product_id=None, model='ResNet50', offset=0):
    if type(product_id) is list :
        cursor2.execute("select * from features where id in (%s) AND model = %s OFFSET %s ROWS FETCH FIRST 10 ROW ONLY", (str(product_id)[1:-1], model, offset))
        tensors = cursor2.fetchall()
        return tensors
    elif product_id != None: 
        cursor2.execute("select * from features where id = %s AND model = %s OFFSET %s ROWS FETCH FIRST 100 ROW ONLY", (id, model, offset))
        res=cursor2.fetchall()[0]
        return res
    else: 
        cursor.execute("select * from features where model = %s OFFSET %s ROWS FETCH FIRST 100 ROW ONLY",(model, offset))
        tensors = cursor2.fetchall()
        return tensors

def get_features_product_ids(product_id=None, model='ResNet50'):
    if type(id) == 'list' :
        cursor2.execute("SELECT product_id FROM features WHERE id IN (%s) AND model = %s", (str(id)[1:-1], model))
        tensors = cursor2.fetchall()
        return tensors
    elif product_id != None: 
        cursor2.execute("SELECT product_id FROM features WHERE id = %s AND model = %s", (id, model))
        res=cursor2.fetchall()[0]
        return res
    else: 
        cursor2.execute("SELECT product_id FROM features WHERE model = %s",(model,))
        tensors = cursor2.fetchall()
        return tensors


def get_products(offset=0):
   cursor.execute("SELECT * FROM products OFFSET %s ROWS FETCH FIRST 100000 ROW ONLY",(offset,))
   return cursor.fetchall()

#Used to verify if fesatures for a certain product_id has been
#calculated
def get_products_not_in_list(values, column='product_id', offset=0):
    cursor.execute("SELECT * FROM products WHERE %s NOT IN (%s) OFFSET %s ROWS FETCH FIRST 100000 ROW ONLY",(column, str(values)[1:-1], offset))
    return cursor.fetchall()

def get_products_in_list(values, column='product_id', offset=0):
    cursor.execute("SELECT * FROM products WHERE %s IN (%s) OFFSET %s ROWS FETCH FIRST 100000 ROW ONLY",(column, str(values)[1:-1], offset))
    return cursor.fetchall()

def get_pool_products():
    cursor.execute("SELECT product.* from (select unnest(products) as product_id from pool) pool JOIN products product USING (product_id)")
    return cursor.fetchall()

def get_products_from_category(product_ids):
   cursor.execute("SELECT * FROM products WHERE product_id = %s LIMIT 10000", (products_ids,))
   return cursor.fetchall()

def get_products_from_category(category):
   cursor.execute("SELECT * FROM products WHERE category = %s LIMIT 10000", (category,))
   return cursor.fetchall()

def get_cursor():
    return cursor

def get_connection():
    return connection

#connection = _connect_database(username="postgres",password="BP1sbC0EkN7c7BN6", host="35.189.114.49", port="5432")
connection2 = _connect_database(db_name='postgres', username="cleed",password="ffs", host="35.193.50.211", port="5432")
cursor2 = _create_cursor(connection2)
connection = _connect_database(db_name='postgres', username="cleed",password="ffs", host="34.76.157.225", port="5432")

cursor = _create_cursor(connection)
