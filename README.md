# stock-analysis
some functional-ish stock strategy analysis tools.  Built to test my 
friend's stock selection idea.

## general idea
* take all past historical data for some stocks
* align it so we are looking at contiguous dates
* pick W random windows of length L from past data
* evaluate the growth of various strategies
* output new CSVs. visualize in excel / google docs.

## notes
everything is hard coded so have fun.  source data was grabbed from
yahoo stocks.  `windows.csv` will contain the actual dates used. seed 
is hard coded for reproducability, but it's arbitrary.
