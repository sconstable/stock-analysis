import csv
import sys
import random
from collections import namedtuple
from datetime import datetime

DataRow = namedtuple('DataRow', ['date', 'open', 'high', 'low', 'close', 
                                 'adj_close', 'volume'])

# read string data into natual types
def parse_row(row):
    return DataRow(
            datetime.strptime(row[0], '%Y-%m-%d'),
            *[float(n) for n in row[1:6]],
            int(row[6])
            )

# read all data from file into DataRow objects
def read_file(input_filename):
    with open(input_filename, 'r') as csvfile:
        all_data = []
        reader = csv.reader(csvfile)
        for row in reader:
            try:
                all_data.append(parse_row(row))
            except ValueError as e:
                print(f"cound not parse row ({row}), skipping", file=sys.stderr)
                continue
    return all_data

# print first row as sanity check
def print_first_row(ticker):
    filename = f"{ticker}.csv"
    all_data = read_file(filename)
    first_row = all_data[0]
    print(first_row)

# align datasets to the same date range
def align_data(*all_datasets):
    num_datasets = len(all_datasets)
    data_lengths = [len(ds) for ds in all_datasets]
    positions = [0] * num_datasets
    while True:
        #check to see if we're past the end of any files
        if any([p >= l for (p, l) in zip(positions, data_lengths)]):
            break
        datarows = [ds[p] for (ds, p) in zip(all_datasets, positions)]
        dates = [r.date for r in datarows]
        
        # compare all dates to each other and increment where necessary
        first_date = dates[0]
        
        # flag for match found
        dates_match = True
        for (i, date) in enumerate(dates):
            if date < first_date:
                positions[i] += 1
                dates_match = False
                break
            if date > first_date:
                positions[0] += 1
                dates_match = False
                break
        if not dates_match:
            continue
        # at this point, all dates must be equal
        # and we must have data left
        assert all([d1 == d2 for d1, d2 in zip(dates, reversed(dates))])
        positions = [p + 1 for p in positions]
        yield [datarow for datarow in datarows]

def gen_windows(num_windows, window_size, data_length):
    max_start = data_length - window_size
    for w in range(num_windows):
        start = random.randint(0, max_start)
        yield(range(start, start + window_size))

def extract_data(aligned_data, dataset_num, field_to_extract, window):
    extract = []
    for row_ind in window:
        ds_row = aligned_data[row_ind][dataset_num]
        extract.append(ds_row._asdict()[field_to_extract])
    return extract

def buy_and_hold_n(dataset_num):
    def buy_and_hold(day, next_day):
        return next_day[dataset_num].high/day[dataset_num].high
    return buy_and_hold

def buy_close_sell_open_n(dataset_num):
    def buy_close_sell_open(day, next_day):
        return next_day[dataset_num].open/day[dataset_num].close
    return buy_close_sell_open

def shifting_sands(day, next_day):
    # identify biggest drop of the day
    price_drops_day1 = [stock.close/stock.open for stock in day]
    biggest_drop = min(price_drops_day1)
    worst_stock = price_drops_day1.index(biggest_drop)

    # buy worst at close, sell at open
    return next_day[worst_stock].open/day[worst_stock].close

# selection_strategy must take two adjacent days of aligned data and return
# the relative price difference such that:
#     value(day2)=value(day1)*strategy(day, day2)
#
# this fn must return total value at each point in window
def simulate_strategy(aligned_data, selection_strategy, windows, 
        initial_value = 100.00):
    total_simulated_data = []
    for window in windows:
        simulated_data = [initial_value]
        for (index1, index2) in zip(window, window[1:]):
            new_price = simulated_data[-1] * selection_strategy(
                    aligned_data[index1], aligned_data[index2])
            simulated_data.append(new_price)
        total_simulated_data.append(simulated_data)
    return total_simulated_data

# transform absolue prices to percentages
def normalize_rows(data_array):
    for row in data_array:
        first = row[0]
        yield([(p-first)/first for p in row]) 


def write_output(output_filename, data_array):
    with open(output_filename, 'w') as csvfile:
        writer = csv.writer(csvfile)
        for row in data_array:
            writer.writerow(row)

if __name__ == "__main__":
    # initialize random seed
    random.seed(420)

    # index tickers to consider (must have csv file)
    indexes = ['HXS', 'VFV', 'XUS', 'ZSP']
    datasets = [read_file(f"{ticker}.csv") for ticker in indexes]
    aligned_data = [rows for rows in align_data(*datasets)]

    # extract HXS data
    windows = [ w for w in gen_windows(100, 300, len(aligned_data))]
    data_array = [extract_data(aligned_data, 0, 'high', w) for w in windows]
    normalized_array = normalize_rows(data_array)
    write_output('extracted_hxs_high.csv', normalized_array)
    
    # simualte buy and hold HXS (should match extracted data)
    buy_hold_hxs = buy_and_hold_n(0)
    sim_data_array = simulate_strategy(aligned_data, buy_hold_hxs, windows)
    normalized_sim_data = normalize_rows(sim_data_array)
    write_output('simulated_hxs_high.csv', normalized_sim_data)

    # buy close and sell open on HXS
    buy_close_sell_open_hxs = buy_close_sell_open_n(0)
    sim_data_array = simulate_strategy(aligned_data, 
            buy_close_sell_open_hxs, windows)
    normalized_sim_data = normalize_rows(sim_data_array)
    write_output('simulated_hxs_close_open.csv', normalized_sim_data)
    
    # the big one: shifting sands
    sim_data_array = simulate_strategy(aligned_data, 
            shifting_sands, windows)
    normalized_sim_data = normalize_rows(sim_data_array)
    write_output('shifting_sands.csv', normalized_sim_data)

    # output windows for reference
    data_array = [extract_data(aligned_data, 0, 'date', w) for w in windows]
    write_output('windows.csv', data_array)
