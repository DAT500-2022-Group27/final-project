from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol
from datetime import datetime
import os
import sys

class MRAddTickerToCSV(MRJob):    
    # preprocessing job to add the asset's ticker e.g.('EQNR', 'DNB') into the CSV file
    # as the dataset only contains this information in the file name
    OUTPUT_PROTOCOL = TextValueProtocol

    # INPUT:
    # FILE: EQNR_2021-08_BIDASK.csv, DATA: 2021-09-01 08:59:25+02:00,1630479565,,212.45,147.8,652,93 (date, timestamp, tick attr., bid, ask, bid size, ask size)
    # OR
    # FILE: EQNR_2021-08_TRADES.csv, DATA: 2021-08-16 09:00:17+02:00,,1629097217,183.68,110,, (date, timestamp, tick attr., price, size, exchange, special cond.)
    
    # get ticker (EQNR, NHY, etc.) and type (TRADE or BIDASK) from filename and add to the record
    def mapper(self, _, line):
        if not line.startswith('Time') and len(line) > 0: # discard header, blank lines
            file_name = os.environ['map_input_file']
            file_name_split = os.path.basename(file_name).split('_')
            ticker = file_name_split[0]
            file_type = file_name_split[2].split('.')[0]
            yield ticker, ticker + ',' + file_type + ',' + line

    # OUTPUT
    # EQNR,BIDASK,2021-09-01 08:59:25+02:00,1630479565,,212.45,147.8,652,93
    # OR
    # EQNR,TRADES,2021-08-16 09:00:17+02:00,,1629097217,183.68,110,,

if __name__ == '__main__':
    print('Start time: ' + datetime.now().strftime('%H:%M:%S'), file = sys.stderr)
    MRAddTickerToCSV.run()
    print('End time: ' + datetime.now().strftime('%H:%M:%S'), file = sys.stderr)