# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 09:08:24 2019

@author: MinhNam
"""

from flask import Flask
from flask_restful import Api, Resource, reqparse
import urllib.request
import sqlite3
from sqlite3 import Error
import json
from collections import ChainMap
import config





app = Flask(__name__)
api = Api(app)

def bdd(db_file):
    try:
        bd=sqlite3.connect(db_file, timeout=10, check_same_thread=False)
        return bd
    except Error as e:
        print(e)

    return None

def display_all(conn):
    cur = conn.cursor()
    cur.execute ('SELECT * FROM velib')
    myresult = cur.fetchall()
    columns = [column[0] for column in cur.description]
    results = []
    for row in myresult:
        results.append(dict(zip(columns, row)))
    conn.commit()
    cur.close()
    return results

def add_dataset(conn,data):
    cur = conn.cursor()
    for data_set in data:
        stationcode = data_set["fields"]["stationcode"]
        stationname = data_set["fields"]["name"]
        is_installed = data_set["fields"]["is_installed"]
        capacity = data_set["fields"]["capacity"]
        numdocksavailable = data_set["fields"]["numdocksavailable"]
        numbikesavailable = data_set["fields"]["numbikesavailable"]
        mechanical = data_set["fields"]["mechanical"]
        ebike = data_set["fields"]["ebike"]
        is_renting = data_set["fields"]["is_renting"]
        is_returning = data_set["fields"]["is_returning"]
        duedate = data_set["fields"]["duedate"]
        coordonnees_geo = data_set["fields"]["coordonnees_geo"]
        coordonnees_geo = ', '.join(str(e) for e in coordonnees_geo)
        nom_arrondissement_communes = data_set["fields"]["nom_arrondissement_communes"]
        #cur.execute('INSERT OR IGNORE INTO velib("stationcode", "numbikesavailable") Values (?,?)',(stationcode,numbikesavailable))
        cur.execute('INSERT OR IGNORE INTO velib("stationcode", "stationname", "is_installed", "capacity", "numdocksavailable", "numbikesavailable", "mechanical", "ebike", "is_renting", "is_returning", "duedate", "coordonnees_geo", "nom_arrondissement_communes" ) Values (?,?,?,?,?,?,?,?,?,?,?,?,?)',(stationcode,stationname,is_installed,capacity,numdocksavailable,numbikesavailable,mechanical,ebike,is_renting,is_returning,duedate,coordonnees_geo,nom_arrondissement_communes,))
    conn.commit()
    cur.close()


def update_dataset(conn,data):
    cur = conn.cursor()
    for data_set in data:
        stationcode = data_set["fields"]["stationcode"]
        stationname = data_set["fields"]["name"]
        is_installed = data_set["fields"]["is_installed"]
        capacity = data_set["fields"]["capacity"]
        numdocksavailable = data_set["fields"]["numdocksavailable"]
        numbikesavailable = data_set["fields"]["numbikesavailable"]
        mechanical = data_set["fields"]["mechanical"]
        ebike = data_set["fields"]["ebike"]
        is_renting = data_set["fields"]["is_renting"]
        is_returning = data_set["fields"]["is_returning"]
        duedate = data_set["fields"]["duedate"]
        coordonnees_geo = data_set["fields"]["coordonnees_geo"]
        coordonnees_geo = ', '.join(str(e) for e in coordonnees_geo)
        nom_arrondissement_communes = data_set["fields"]["nom_arrondissement_communes"]
        data_to_add=(stationname, is_installed, capacity, numdocksavailable, numbikesavailable,mechanical, ebike, is_renting,is_returning,duedate,stationcode,nom_arrondissement_communes)
        sql_query = """UPDATE velib SET stationname = ? ,is_installed = ?, capacity = ?, numdocksavailable = ?, numbikesavailable = ?, mechanical = ?, ebike = ?, is_renting = ?, is_returning = ?, duedate = ?, nom_arrondissement_communes = ? where stationcode= ? """
        cur.execute(sql_query,data_to_add)
        update_date_query="""UPDATE velib set duedate = ? WHERE stationcode = ? """
        data_to_update_date_query=(duedate,stationcode)
        cur.execute(update_date_query,data_to_update_date_query)
    conn.commit()
    cur.close()

def get_data_from_id(conn,id_number):
    cur = conn.cursor()
    #cur.execute ('SELECT "name", "numbikesavailable" FROM velib WHERE "stationcode" =' + id_number)
    cur.execute ('SELECT * FROM velib WHERE "stationcode" =' + id_number)
    myresult = cur.fetchall()
    columns = [column[0] for column in cur.description]
    results = []
    for row in myresult:
        results.append(dict(zip(columns, row)))
    conn.commit()
    cur.close()
    return results


def get_data_from_name(conn,name):
    cur = conn.cursor()
    #cur.execute ('SELECT "name", "numbikesavailable" FROM velib WHERE "stationcode" =' + id_number)
    cur.execute ('SELECT * FROM velib WHERE "stationname" LIKE "%' + name + '%"')
    myresult = cur.fetchall()
    columns = [column[0] for column in cur.description]
    results = []
    for row in myresult:
        results.append(dict(zip(columns, row)))
    conn.commit()
    cur.close()
    return results

def get_data_from_state(conn,state):
    cur = conn.cursor()
    #cur.execute ('SELECT "name", "numbikesavailable" FROM velib WHERE "stationcode" =' + id_number)
    cur.execute ('SELECT * FROM velib WHERE LOWER("is_installed") = LOWER("' + state +'")')
    myresult = cur.fetchall()
    columns = [column[0] for column in cur.description]
    results = []
    for row in myresult:
        results.append(dict(zip(columns, row)))
    conn.commit()
    cur.close()
    return results

def get_data_from_is_renting(conn,state):
    cur = conn.cursor()
    #cur.execute ('SELECT "name", "numbikesavailable" FROM velib WHERE "stationcode" =' + id_number)
    cur.execute ('SELECT * FROM velib WHERE LOWER("is_renting") = LOWER("' + state +'")')
    myresult = cur.fetchall()
    columns = [column[0] for column in cur.description]
    results = []
    for row in myresult:
        results.append(dict(zip(columns, row)))
    conn.commit()
    cur.close()
    return results

def get_data_from_is_returning(conn,state):
    cur = conn.cursor()
    #cur.execute ('SELECT "name", "numbikesavailable" FROM velib WHERE "stationcode" =' + id_number)
    cur.execute ('SELECT * FROM velib WHERE LOWER("is_returning") = LOWER("' + state +'")')
    myresult = cur.fetchall()
    columns = [column[0] for column in cur.description]
    results = []
    for row in myresult:
        results.append(dict(zip(columns, row)))
    conn.commit()
    cur.close()
    return results

def get_data_from_mechanical(conn):
    cur = conn.cursor()
    #cur.execute ('SELECT "name", "numbikesavailable" FROM velib WHERE "stationcode" =' + id_number)
    cur.execute ('SELECT * FROM velib WHERE "mechanical" > 0')
    myresult = cur.fetchall()
    columns = [column[0] for column in cur.description]
    results = []
    for row in myresult:
        results.append(dict(zip(columns, row)))
    conn.commit()
    cur.close()
    return results

def get_data_from_electric(conn):
    cur = conn.cursor()
    #cur.execute ('SELECT "name", "numbikesavailable" FROM velib WHERE "stationcode" =' + id_number)
    cur.execute ('SELECT * FROM velib WHERE "ebike" > 0')
    myresult = cur.fetchall()
    columns = [column[0] for column in cur.description]
    results = []
    for row in myresult:
        results.append(dict(zip(columns, row)))
    conn.commit()
    cur.close()
    return results

def get_map_info(conn):
    cur = conn.cursor()
    """
    Coordonnées, nom de la station, nombre de vélos électriques, mécaniques, de vélos
    """
    cur.execute('SELECT  stationname, coordonnees_geo, mechanical, ebike, numbikesavailable from velib')
    myresult = cur.fetchall()
    columns = [column[0] for column in cur.description]
    results = []
    for row in myresult:
        results.append(dict(zip(columns, row)))
    conn.commit()
    cur.close()
    return results


db=bdd(config.db_path)

class Velib(Resource):

    def get(self):
        database_data= display_all(db)
        return database_data

    def put(self):
        status = {"status":"in progress"}
        url = urllib.request.urlopen('https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/download/?format=json&timezone=Europe/Berlin&lang=fr')
        data = url.read().decode('utf-8')
        data = str(data)
        data=eval(data)
        add_dataset(db,data)
        status = {"status":"done"}
        return status

    def post(self):
        status = {"status":"in progress"}
        url = urllib.request.urlopen('https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/download/?format=json&timezone=Europe/Berlin&lang=fr')
        data = url.read().decode('utf-8')
        data = str(data)
        data=eval(data)
        update_dataset(db,data)
        status = {"status":"done"}
        return status


api.add_resource(Velib,"/api/velib/")

class Velib_from_id(Resource):


    def get(self,id_number):
        data_row=get_data_from_id(db,id_number)
        return data_row

api.add_resource(Velib_from_id,'/api/velib/id/<id_number>')

class Velib_from_name(Resource):


    def get(self,name):
        data_row=get_data_from_name(db,name)
        return data_row

api.add_resource(Velib_from_name,'/api/velib/name/<name>')

class enfonction(Resource):

    def get(self,state):
        is_working=get_data_from_state(db,state)
        return is_working

api.add_resource(enfonction,'/api/velib/working/<state>')

class paiement(Resource):

    def get(self,state):
        can_pay=get_data_from_is_renting(db,state)
        return can_pay

api.add_resource(paiement,'/api/velib/paiement/<state>')

class returning(Resource):

    def get(self,state):
        can_return=get_data_from_is_returning(db,state)
        return can_return

api.add_resource(returning,'/api/velib/returning/<state>')

class mechanical_bike(Resource):

    def get(self):
        mechanical_bike = get_data_from_mechanical(db)
        return mechanical_bike

api.add_resource(mechanical_bike,'/api/velib/mechanical/')

class elec_bike(Resource):

    def get(self):
        elec_bike = get_data_from_electric(db)
        return elec_bike

api.add_resource(elec_bike,'/api/velib/elec/')


class coordonnees(Resource):

    def get(self):
        info = get_map_info(db)
        return info

api.add_resource(coordonnees,'/api/velib/map/')



app.run(port='5001',debug=False)
