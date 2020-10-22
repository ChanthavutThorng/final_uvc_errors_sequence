# packages
try:
    import os, sys, cx_Oracle
    from paramiko import client
    from datetime import datetime, timedelta
except Exception as e:
    print(e)


class SERVER:

    def __init__(self, server):
        self.client = client.SSHClient()
        self.client.set_missing_host_key_policy(client.AutoAddPolicy())
        self.client.connect(server['address'], username=server['username'], password=server['password'], look_for_keys=False)

    def output(self, command):
        if self.client:
            stdin, stdout, stderr = self.client.exec_command(command)
            while not stdout.channel.exit_status_ready():
                # Print data when available
                if stdout.channel.recv_ready():
                    alldata = stdout.channel.recv(1024)
                    prevdata = b"1"

                    while prevdata:
                        prevdata = stdout.channel.recv(1024)
                        alldata += prevdata

                    for data in str(alldata, 'utf-8').splitlines():
                        file_data.append(data.split('|')[1])

        else:
            print("Connection not opened.")

    def close_session(self):
        self.client.close()


class DB:
    def __init__(self, db):
        dsn_tns = cx_Oracle.makedsn(db['host'], db['port'], service_name=db['service'])
        self.connection = cx_Oracle.connect(user=db['username'], password=db['password'], dsn=dsn_tns)

    def send_query(self, script):
        cursor = self.connection.cursor()
        cursor.execute(script)
        return cursor

    def send_commit(self, script, commit):
        cursor = self.connection.cursor()
        cursor.execute(script)

        if commit == True:
            self.connection.commit()

    def close_connection(self):
        self.connection.close()


# server credentials
s_information = {
    'username': 'XXX',
    'password': 'XXX',
    'address': 'XXX'
}

# database credentials
db_information = {
    'host': 'XXX',
    'port': XXX,
    'username': 'XXX',
    'password': 'XXX',
    'service': 'XXX'
}

# global variable
file_data = []
check_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

# connect to server
try:
    send_command = "cd cdr/outbak/uvc/recharge/" + check_date + " ; cat uvc_abr*.unl"
    s_connection = SERVER(s_information)
    s_connection.output(send_command)
    s_connection.close_session()
except Exception as e:
    print(e)


# connect to database
try:
    # if file_data is empty, stop run new line code
    if not file_data:
        print('list is empty')
        sys.exit()

    # open connection
    db_connection = DB(db_information)

    # delete
    db_connection.send_commit('DELETE FROM UVC_UDT_STS.TBL_UVC_SUPPLY_TEMP', True)

    # insert
    for item in file_data:
        db_connection.send_commit('INSERT INTO UVC_UDT_STS.TBL_UVC_SUPPLY_TEMP ("SEQUENCE") VALUES (' + item + ')', True)

    # call procedure to backup records
    db_connection.send_commit('''
        BEGIN
            SCUDB.PRO_UVC_SUPPLY_BACKUP();
        END;
    ''', False)

    # select
    result =db_connection.send_query('SELECT * FROM UVC_UDT_STS.TBL_UVC_SUPPLY_TEMP a WHERE a."SEQUENCE" IN ('+ str(file_data)[1:-1] +')')
    for item in result:
        print(item[0])

    # call procedure to update status
    db_connection.send_commit('''
            BEGIN
                SCUDB.PRO_UPDATE_STATUS();
            END;
        ''', False)

    db_connection.close_connection()

except Exception as e:
    print(e)

print("Finished")
