import json
import pandas as pd
import math
import random
import os


class compose:
    def __init__(self, path):
        self.format = []
        self.path = path

    def format_data(self, path, file):
        self.format = format(path, file)

    def compose(self):
        item = ['primary', 'secondary', 'composite']
        composed_data = {}
        data = self.structure_data(args=item)
        for i in item:
            if not data[i]:
                continue
            processing = False
            for k,v in data[i].items():
                if k.startswith('BW') or k.startswith('SF'):
                    composed_data = self.process_beeswax_data(k, v, i, composed_data)
                    processing = True
                if k.startswith('DV') or k.startswith('SF'):
                    composed_data = self.process_dv360_data(k, v, i, composed_data, processing)
                if k.startswith('celtra'):
                    composed_data = self.process_celtra_data(k, v, i, composed_data)
        try:
            composed_data = composed_data.to_dict(orient='records')
        except AttributeError:
            pass
        if composed_data:
            self.to_json_file(composed_data)

    def structure_data(self, args):
        r_data = {'primary': {}, 'secondary': {}, 'composite': {}}
        for arg in args:
            for key, value in self.format.data.items():
                if value.get('paths') and value.get('type') == arg:
                    for path in value['paths']:
                        if not r_data[arg].get(path):
                            new_value = self.create_entry(key, value)
                            r_data[arg][path] = [new_value]
                        else:
                            if value.get('type') == arg:
                                new_value = self.create_entry(key, value)
                                r_data[arg][path].append(new_value)
        return r_data

    def load_data(self, path):
        with open(path, 'r') as file:
            return pd.read_csv(file)

    def to_json_file(self, data):
        with open(f"src/modules/composition/Output/{self.format.file}", 'w+') as file:
            json.dump(data, file)

    def process_beeswax_data(self, file, value, index, composed_data):
        try:
            raw_data = self.load_data(f"{self.path}/{file}.csv")
            if index == 'primary':
                array = [i['key'] for i in value]
                filtered_data = raw_data[array]
                return filtered_data
            elif index == 'secondary':
                array = [i['key'] for i in value] + [i['fk'] for i in value]
                filtered_data = raw_data[array]
                for i in value:
                    filtered_data = filtered_data.rename(columns={i['key']: str(i['key']).replace(' ', '_').lower()})
                    filtered_data = filtered_data.rename(columns={i['fk']: str(i['fk']).replace(' ', '_').lower()})
                fk = [str(i['fk']).replace(' ', '_').lower() for i in value]
                composed_data = pd.merge(left=composed_data, right=filtered_data, left_on=fk, right_on=fk, how='left')
                return composed_data
            else:
                array = [i['key'] for i in value] + list(set(([i['fk'] for i in value])))
                filtered_data = raw_data[array]
                fk = [str(i['fk']) for i in value]
                ck = [str(i['ck']).replace(' ', '_').lower() for i in value]
                composed_data = pd.merge(left=composed_data, right=filtered_data, left_on=ck, right_on=fk, how='left')
                return composed_data
        except FileNotFoundError:
            return composed_data

    def process_dv360_data(self, file, value, index, composed_data, processing):
        try:
            raw_data = self.load_data(f"Reporting/All_Reports/{file}.csv")
            if index == 'primary':
                array = [i['key'] for i in value]
                filtered_data = raw_data[array]
                if processing:
                    keys = [i['key'] for i in value if i['agg'] == 'key']
                    composed_data = pd.concat([filtered_data, composed_data]).groupby(keys).sum().reset_index()
            return composed_data
        except FileNotFoundError:
            return composed_data

    def process_celtra_data(self, file, value, index, composed_data):
        try:
            raw_data = self.load_data(f"Reporting/All_Reports/{file}.csv")
            if index == 'primary':
                array = [i['key'] for i in value]
                filtered_data = raw_data[array]
                return filtered_data.to_dict(orient='records')
            elif index == 'secondary':
                return composed_data
        except FileNotFoundError:
            return composed_data

    def create_entry(self, key, value):
        if value.get('type') == 'primary':
            return {'id': key, "key": value['key'], "agg": value['agg']}
        elif value.get('type') == 'secondary':
            return {'id': key, "key": value['key'], "fk": value['fk']}
        else:
            return {'id': key, "key": value['key'], "fk": value['fk'], "ck": value['ck']}

    def build_report(self):
        folder_path = "src/modules/composition/Formats"
        file_list = os.listdir(folder_path)
        for file in file_list:
            self.format_data(f"{folder_path}/{file}", file)
            self.compose()

class format:
    def __init__(self, path, file):
        self.file = file
        with open(path , 'r') as file:
            self.data = json.load(file)








