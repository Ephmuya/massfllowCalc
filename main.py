import requests
import os
from datetime import datetime

def authentication(username, password, portal):
    url = portal + "/sharing/rest/generateToken"
    payload = {'f': 'json',
               'username': username,
               'password': password,
               'client': 'referer',
               'referer': portal,
               'expiration': '3600'}

    data = requests.post(url, payload).json()
    token = str(data['token'])
    return token


class MassFlow:
    def __init__(self, sensor_id):
        self.portal = os.getenv('PORTAL')
        self.sensor_id = sensor_id
        self.hosted = "https://geoevent.protoenergy.com/arcgis/rest/services/Hosted/"
        self.layers = "/FeatureServer/0/"
        self.delete = "/deleteFeatures"
        self.update = "/updateFeatures"
        self.query = "/query"
        self.username = os.getenv('PORTALUID')
        self.password = os.getenv('PORTALPASS')
        self.token = authentication(self.username, self.password, self.portal)
        self.featureLayer = self.hosted + self.sensor_id + self.layers
        self.queryPayload = {
            "f": "json",
            "token": self.token,
            "outFields": "*"
        }

    def get_data(self, query):
        fl = self.featureLayer + self.query

        payload = self.queryPayload
        payload['where'] = query
        data = requests.post(fl, payload).json()
        try:
            return data['features']
        except KeyError as e:
            return e

    def delete_features(self, query):
        fl = self.featureLayer + self.delete
        payload = self.queryPayload
        payload['where'] = query
        data = requests.post(fl, payload).json()


    def slice_feature(self, data):
        if data.startswith('46'):
            a = data[6:12]
            b = data[14:20]
            a_data = int(a, 16)
            b_data = int(b, 16)
            return {"a_data": a_data, "b_data": b_data}

    def update_request(self, oid, field_a, field_b, a, b):
        url = self.featureLayer + self.update
        features = [{"attributes": {"objectid": oid, field_a: int(a), field_b: int(b)}}]
        payload = {"f": "json", "token": self.token, "features": str(features)}
        response = requests.post(url=url, data=payload).json()
        return response

    def update_slice_data(self, item):
        query = "objectid= " + str(item)
        data = self.get_data(query)
        oid = data[0]['attributes']['objectid']
        data_ = data[0]['attributes']['data_']
        sliced = self.slice_feature(data_)
        update = self.update_request(oid, "value_a","value_b", sliced['a_data'], sliced['b_data'])
        return update

    def update_feature(self):
        data = self.get_data("1=1")
        try:
            oid_list = []
            for i in data:
                oid_list.append(i['attributes']['objectid'])
            oid_list.sort()
            for ii, item in enumerate(oid_list):
                self.update_slice_data(item)

        except (TypeError, KeyError):
            error = self.featureLayer + ' does not exist or is inaccessible'
            return error

    def filter_data(self):
        print('updating...')
        self.update_feature()
        data = self.get_data("1=1")
        for i, item in enumerate(data):
            oid = item['attributes']['objectid']

            if item['attributes']['time'] is not None:
                timestamp = datetime.fromtimestamp(int(item['attributes']['time']/1000))
                hours = timestamp.strftime('%H:%M')
                if "05:45" < hours < '06:20' and (hours is not None or hours is not int):
                   pass
                else:
                    self.delete_features("objectid=" + str(oid))
            else:
                self.delete_features("objectid=" + str(oid))


    def calculate_changes(self):
        self.filter_data()
        data = self.get_data("1=1")
        oids = []
        for i in data:
            oids.append(i['attributes']['objectid'])
        oids.sort()
        print('Calculating...')
        for ii, item in enumerate(oids):
            if ii == 0:
                self.update_request(item, "change_a", "change_b", 0, 0)
            elif ii > 0:
                data_1 = self.get_data("objectid=" + str(oids[ii - 1]))
                old_data_1 = data_1[0]['attributes']['value_a']
                old_data_2 = data_1[0]['attributes']['value_b']
                new_data = self.get_data("objectid=" + str(oids[ii]))
                change_a = int(new_data[0]['attributes']['value_a']) - old_data_1
                change_b = int(new_data[0]['attributes']['value_b']) - old_data_2
                self.update_request(oids[ii], "change_a", "change_b", change_a, change_b)


if __name__ == '__main__':
    Devices = ['108','103','107','0FD']
    for i,item in enumerate(Devices):
        print("updating "+ str(item))
        def excecute(i):
            try:
                MassFlow("Device_888"+str(item)).calculate_changes()
                print("updated " + str(i))
            except TypeError:
                print("trying again ...")
                MassFlow("Device_888"+str(item)).calculate_changes()
                print("updated " + str(i))
        excecute(Devices)