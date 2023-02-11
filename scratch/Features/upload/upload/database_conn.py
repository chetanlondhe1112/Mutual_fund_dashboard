from configparser import ConfigParser
import mysql.connector
config = ConfigParser()
config.read('config.ini')

def connect():
    return mysql.connector.connect(host = config['mysql']['host'],
                           user = config['mysql']['user'],                          
                           db = config['mysql']['database'])

