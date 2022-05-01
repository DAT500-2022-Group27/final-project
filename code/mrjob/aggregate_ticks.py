from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol
from datetime import datetime
import re
import sys

class MRAggregateTicks(MRJob):    
    FILES = ['../python/tick_util.py']
    OUTPUT_PROTOCOL = TextValueProtocol

    def configure_args(self):
        # Choices are pandas DateOffset objects which correspond to certain slices of the YYYY-MM-DD HH:MM timestamp
        super(MRAggregateTicks, self).configure_args()
        self.add_passthru_arg(
            '--time-period', 
            default='1H', 
            choices=['1S', '1min', '1H', '1D'],
            help="Specify the aggregation period"
        )

    def get_tidx(self):
        if self.options.time_period=='1S':
            return 19
        elif self.options.time_period=='1min':
            return 16
        elif self.options.time_period=='1H':
            return 13
        elif self.options.time_period=='1D':
            return 10


    # key is a string with the ticker and a truncated time stamp separated by underscore...
    # i.e. EQNR_2022-01 for monthly, EQNR_2022-01-31 14 for hourly, EQNR_2022-01-31 14:30 for minute, etc.
    def mapper(self, _, line):
        # a very limited number of trades will have TickAttribLast variable set, this field JSON-like surrounded by quotes & w/ internal commas
        if '"' in line:
            tick_line = re.split(r',(?=")', line)
            return
        else:
            tick_line = line.split(',')
        ticker = tick_line[0]
        key = ticker + '_' + tick_line[2][:self.get_tidx()] # timestamp is 1st element of tick_line list, slice this string according to agg. period

        yield key, tick_line
    
    # key is a ticker+timestamp string, values is a list of ticks that fall under this key/timestamp to be aggregated into a single period
    def reducer(self, key, values):
            from tick_util import summarize_ticks # pandas must be installed on cluster
            # get the ticker and timestamp from the key and insert into the list
            ticker = key.split('_')[0]
            agg_line = summarize_ticks(values, period=self.options.time_period)
            if agg_line is not None: # discard e.g. periods w/o both trade and bidask lines
                agg_line.insert(0, ticker)
                yield ticker, ','.join(map(str, agg_line)) #join the list into a comma separated

if __name__ == '__main__':
    print('Start time: ' + datetime.now().strftime('%H:%M:%S'), file = sys.stderr)
    MRAggregateTicks.run()
    print('End time: ' + datetime.now().strftime('%H:%M:%S'), file = sys.stderr)