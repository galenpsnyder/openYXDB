# openYXDB
Read data from .yxdb files into R.

openYXDB is an R implementation of yxdb-py.

## Usage
Generally, you should only use `read_yxdb`. This connects to a .yxdb file and returns an appropriately formatted dataframe.

In rare cases, you may want, for example, meta-information about a .yxdb file. In such cases you can use `get_header_and_field_info` to connect to a file and produce a list of meta-information. You can then use `next_record` on this list to return observations from the .yxdb file. You should use `close` on your connection upon reading in your desired observations. Because openYXDB tracks a data stream by overwriting the connection instantiated by `get_header_and_field_info`, using any functions beyond `read_yxdb`, `get_header_and_field_info`, or `next_record` is not straightforward and is discouraged.
