from clickhouse_driver import Client
from datetime import date, datetime

def directad_linear_to_data_region(client, regions):
    query = 'SELECT \n'
    for region in regions:
        query += f'uniqIf(terminal_uid, city={region}) as "{region}", \n'
    query += "from directadl.linear where timestamp >= now() - 1h"
    data = client.execute(query)
    return data

def directad_linear_to_data_channel(client, channels):
    query = ''
    for channel in channels:
        query += f'uniqIf(terminal_uid, channel={channel}) as "{channel}", \n'
    query += "from directadl.linear where timestamp >= now() - 1h"
    data = client.execute(query)
    return data

def directad_linear_to_data_channel_region(client, regions, channels):
    query = ''
    for region in regions:
        for channel in channels:
            _as = region + "_" + channel
            query += f'uniqIf(terminal_uid, city={region}, channel={channel}) as "{_as}", \n'
    query += "from directadl.linear where timestamp >= now() - 1h"
    data = client.execute(query)
    return data

def directad_l_to_data_region(client, cities):
    query = ''
    for city in cities:
        query += f'uniqIf(terminal_uid, city={city}) as "{city}", \n'
    query += "from directadl.linear where timestamp >= now() - 1h"
    data = client.execute(query)
    return data

def directad_l_to_data_channel(client, content):
    query = ''
    for _content in content:
        query += f'uniqIf(terminal_uid, content={content}) as "{_content}", \n'
    query += "from directadl.linear where timestamp >= now() - 1h"
    data = client.execute(query)
    return data


def directad_l_to_data_channel_region(client, cities, content):
    query = ''
    for city in cities:
        for _content in content:
            _as = city + "_" + _content
            query += f'uniqIf(terminal_uid, city={city}, content={_content}) as "{_as}", \n'
    query += "from directadl.linear where timestamp >= now() - 1h"
    data = client.execute(query)
    return data

def save_data_region(client, regions, data):
    query = 'insert into dsp.data_region(timestamp, date, region, uniq) values'
    for i in range(len(regions)):
        query += f' ({datetime.now()}, {date.today()}, {regions[i]}, {data[i][0]}), '
    client.execute(query)

def save_data_channel(client, channels, data):
    query = "insert into dsp.data_channel(timestamp, date, region, uniq) values"
    for i in range(len(channels)):
        query += f' ({datetime.now()}, {date.today()}, {channels[i]}, {data[i][0]}), '
 
def save_data_channel_region(client, regions, channels, data):
    query = "insert into dsp.data_channel_region(timestamp, date, region, channel, uniq) values"
    _i = 0
    for i in range(len(regions)):
        for j in range(len(channels)):
            query += f" ({datetime.now()}, {date.today()}, {regions[i]}, {channels[j]}, {data[i][0]})"
            _i += 1
    client.execute(query)


if __name__ == "__main__":
    connection_user = ''
    connection_password = ''
    connection_host = 'localhost'
    connection_port = '9000'
    connection_db = ''
    connection_url = f"clickhouse://{connection_user}:{connection_password}@{connection_host}:{connection_port}/{connection_db}"

    client = Client(
        host=connection_host,
        user=connection_user,
        password=connection_password,
        port=connection_port
    )

    regions_list = open('cities.txt').read().split('\n')
    channels_list = open("channels.txt").read().split('\n')

    data = directad_linear_to_data_region(client, regions_list)
    save_data_region(client, data)
    data = directad_linear_to_data_channel(client, channels_list)
    save_data_channel(client, data)
    data = directad_linear_to_data_channel_region(client, regions_list, channels_list)
    save_data_channel_region(client, data)
    data = directad_l_to_data_region(client, regions_list)
    save_data_region(client, data)
    data = directad_l_to_data_channel(client, channels_list)
    save_data_channel(client, data)
    data = directad_l_to_data_channel_region(client, regions_list, channels_list)
    save_data_channel_region(client, data)