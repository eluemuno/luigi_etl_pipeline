import luigi
import os
import avro.datafile
import avro.io
import glob
import csv
import pyorc
import pandas as pd


class readLogFile(luigi.ExternalTask):

    def output(self):
        basedir = os.path.dirname(os.path.abspath('etl_train_v2.py'))
        return luigi.LocalTarget(os.path.join(basedir, 'source_in'))


# workerEnv.init()
class getSpecEntries(luigi.Task):

    def requires(self):
        return readLogFile()

    def output(self):
        path = os.path.dirname(os.path.abspath('etl_train_v2.py'))
        path_out = os.path.join(path, 'source_out/')
        return luigi.LocalTarget(os.path.join(path_out, 'etl_out.csv'))

    def run(self):
        path = os.path.dirname(os.path.abspath('etl_train_v2.py'))
        path_in = os.path.join(path, 'source_in')
        path_out = os.path.join(path, 'source_out/')

        for source in glob.glob(path_in + '/*'):
            source_name = source.split('/')[-1]
            if source_name == 'avro':
                source_path = glob.glob(source + '/*')
                for file in source_path:
                    parsed_avro_file = avro.datafile.DataFileReader(open(file, 'rb'), avro.io.DatumReader())
                    for avro_record in parsed_avro_file:
                        avro_data = []
                        avro_data_temp = {'Source': 'AVRO',
                                          'registration_dttm': avro_record['registration_dttm'],
                                          'id': avro_record['id'],
                                          'first_name': avro_record['first_name'],
                                          'last_name': avro_record['last_name'],
                                          'email': avro_record['email'],
                                          'gender': avro_record['gender'],
                                          'ip_address': avro_record['ip_address'],
                                          'cc': avro_record['cc'],
                                          'country': avro_record['country'],
                                          'birthdate': avro_record['birthdate'],
                                          'salary': avro_record['salary'],
                                          'title': avro_record['title'],
                                          'comments': avro_record['comments']}
                        avro_data.append(avro_data_temp)
                        output_file = pd.DataFrame(avro_data).to_csv(path_out +'Final_Output.csv', mode='a', header=False, index=False,
                                                                     sep=',', line_terminator='\n')

            elif source_name == 'csv':
                source_path = glob.glob(source + '/*')
                for file in source_path:
                    parsed_csv_file = csv.DictReader(open(file), delimiter=',')
                    header = next(parsed_csv_file)
                    for csv_record in parsed_csv_file:
                        csv_data = []
                        csv_data_temp = {'Source': 'CSV',
                                         'registration_dttm': csv_record['registration_dttm'],
                                         'id': csv_record['id'],
                                         'first_name': csv_record['first_name'],
                                         'last_name': csv_record['last_name'],
                                         'email': csv_record['email'],
                                         'gender': csv_record['gender'],
                                         'ip_address': csv_record['ip_address'],
                                         'cc': csv_record['cc'],
                                         'country': csv_record['country'],
                                         'birthdate': csv_record['birthdate'],
                                         'salary': csv_record['salary'],
                                         'title': csv_record['title'],
                                         'comments': csv_record['comments']}
                        csv_data.append(csv_data_temp)
                        output_file = pd.DataFrame(csv_data).to_csv(path_out + 'Final_Output.csv', mode='a', header=False, index=False,
                                                                    sep=',', line_terminator='\n')

            elif source_name == 'orc':
                source_path = glob.glob(source + '/*')
                for file in source_path:
                    with open(file, 'rb') as orc_file:
                        reader = pyorc.Reader(orc_file)
                        schema_ = reader.schema
                        df = pd.DataFrame(data=reader.read())
                        df.insert(0, 'Source', 'ORC')
                        output_file = df.to_csv(path_out + 'Final_Output.csv', mode='a', header=False,
                                                                              index=False, sep=',',line_terminator='\n')

            elif source_name == 'parquet':
                source_path = glob.glob(source + '/*')
                for file in source_path:
                    parsed_parquet_file = pd.read_parquet(file, engine='fastparquet')
                    df = pd.DataFrame(parsed_parquet_file)
                    df.insert(0, 'Source', 'PARQUET')
                    output_file = df.to_csv(path_out + 'Final_Output.csv', mode='a', header=False, index=False,
                                                                    sep=',', line_terminator='\n')

            else:
                unknown_data = 'Unidentified file format!'


if __name__ == '__main__':
    luigi.run(['--local-scheduler'], main_task_cls=getSpecEntries)
