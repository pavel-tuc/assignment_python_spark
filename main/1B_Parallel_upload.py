import concurrent.futures
import re
import requests


_URL = 'http://your-elastic-server:port/_cat/shards'
_REGEX_INDEX = "^index-\d{4}-\d{2}"
_REGEX_NODE = "data-node-\d{2}"


def get_year_month(index: str) -> tuple:
    """Return a tuple

    Extracts year and month parameters from index
    """
    prefix, year, month = index.split("-")
    return year, month


def get_elastic_shards(auth_header: dict, url: str) -> dict:
    """Returns a dict

    Fetch data from Elastic API and return dict of available nodes with attached indexes
    """
    shards = requests.get(url=url, headers=auth_header)  # Missing some parsing e.x. data = json.loads(shards.text) etc.

    shards_dict = {}
    for shard in shards:
        index = re.search(_REGEX_INDEX, shard).group(0)
        node = re.search(_REGEX_NODE, shard).group(0)
        if node not in shards_dict:
            shards_dict[node] = []
        shards_dict[node].append(index)
    return shards_dict


def write_to_elastic_index(df, year, month, target_index) -> None:
    """Writes data filtered from dataframe (df) by the created_year=/created_month partition filter into given Elastic index.

    Example:
    # write data from object store partition 'created_year=2018/created_month=1' into Elasticsearch index named 'index-2018-01'
    write_to_elastic(df, 2018, 1, 'index-2018-01')
    """


def upload_to_elastic(df, node, indices: list) -> None:
    """Returns None

    Uploads all available indexes available for node
    """
    for index in indices:
        year, month = get_year_month(index)
        try:
            write_to_elastic_index(df, year, month, index)
            print("Write of index: " + index + " on node: " + node + " completed")
        except Exception as exc:
            print("Node: Error: " + str(exc) + " | Write of index: " + index + " on node: " + node + " unsuccessful")



if __name__ == '__main__':
    header = {}  # Depends on the API specifications
    node_indices = get_elastic_shards(header, _URL)  # mapping of nodes and indices
    df = spark.read.parquet("....")  # read data from parquet/csv/etc.

    # Creates separate thread for each node and process all available indexes
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = [executor.submit(upload_to_elastic, df, node, indices) for node, indices in node_indices.items()]


