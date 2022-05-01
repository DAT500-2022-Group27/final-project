from mrjob.job import MRJob
import datetime
import os
import pytz
# all of the tick data files should contain dates in ASCENDING order
# any deviation from this indicates an problem with the download

## to run locally:
## C:\Users\Adam\Desktop\DAT500>python check_date_order.py C:\Users\Adam\Desktop\DAT500\data

class MRCountSum(MRJob):    
    def mapper_init(self):
        self.previous_time = datetime.datetime.min
        self.previous_time = pytz.utc.localize(self.previous_time)

    def mapper(self, _, line):
        tick_line = line.split(',')
        if tick_line[0] != 'Time':
            line_date = datetime.datetime.strptime(tick_line[0], '%Y-%m-%d %H:%M:%S%z')
            if line_date < self.previous_time:
                yield os.environ['map_input_file'], 1
            else:
                self.previous_time = line_date

    def combiner(self, key, values):
        yield key, sum(values)
        
    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRCountSum.run()